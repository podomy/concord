// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/logs"
)

// SyncPath is the node-to-node unary sync endpoint.
const SyncPath = "/v1/sync"

// Client is a Noise IK client for Concord peer transport.
// It presents this node's static key and parcel, dials peers whose static
// key arrives via gossip, and verifies each responder's parcel before syncing.
//
// Sessions to a peer are pooled and reused across Sync calls: one TCP+IK
// handshake serves many pages until the session ages out, errors, or the
// peer's key changes. Verification runs once per session establishment,
// not per page; the 60s age cap bounds how long a stale trust decision
// survives rotation.
//
// Client is safe for concurrent Sync. All pool state lives behind sessMu;
// timeout and the adaptive page budget (curLimit) are guarded by mu;
// timeout is otherwise set once at construction (tests may assign it
// before first use).
type Client struct {
	static  StaticKey
	parcel  []byte
	verify  Verifier
	timeout time.Duration
	logger  *logs.Logger

	mu       sync.RWMutex
	curLimit int

	sessMu   sync.Mutex
	sessions map[string]*pooledSession
}

// pooledSession is one verified Noise session to a peer, reused across
// Sync pages. refs counts in-flight requests; eviction skips sessions
// with refs above zero so a sweep never yanks a live request.
type pooledSession struct {
	key         string
	conn        *noiseConn
	transport   *http.Transport
	pub         [32]byte
	established time.Time
	refs        int
}

// maxSessionAge bounds reuse of one Noise session. Rotation propagates
// through gossip in seconds; sixty seconds caps how long a stale
// verification survives a key or generation change. Age is checked on
// every Sync, so a quiet peer's session expires before its next page.
const maxSessionAge = 60 * time.Second

// sessionIdleTimeout closes a pooled transport idle past this bound,
// below the server's 120s idle timeout so the client always hangs up
// first instead of writing into a server-closed connection.
const sessionIdleTimeout = 90 * time.Second

// minSyncLimit floors the adaptive page budget: decode failures halve the
// requested limit down to here, never below, so progress continues on
// constrained links.
const minSyncLimit = 10

// NewClient builds a Client using this node's static key and encoded parcel.
// verify checks each responder before syncing; see KeyPinner in runtime.
// The page budget starts at the default page; see limitFor.
func NewClient(static StaticKey, parcel []byte, verify Verifier) *Client {
	return &Client{
		static:   static,
		parcel:   parcel,
		verify:   verify,
		timeout:  10 * time.Second,
		curLimit: defaultSyncLimit,
		sessions: make(map[string]*pooledSession),
	}
}

// getTimeout reads the request timeout under the mutex.
func (c *Client) getTimeout() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.timeout
}

// limitFor bounds the requested page to the adaptive budget: the smaller
// of want and the current budget, or the budget itself when want asks for
// the server default. The budget starts at the default page, halves per
// decode failure to minSyncLimit, and resets on success. peersync always
// asks for the default page, so the wire sees 100 normally and less while
// failures persist.
//
// TLA drift note (no model edits): adaptive paging is unmodeled transport
// hardening; it only narrows requested pages, never the wire format.
func (c *Client) limitFor(want int) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if want <= 0 || want > c.curLimit {
		return c.curLimit
	}
	return want
}

// noteDecodeFailure halves the page budget to minSyncLimit after a page
// failed to decode, so the next request asks for a smaller page.
func (c *Client) noteDecodeFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.curLimit /= 2
	if c.curLimit < minSyncLimit {
		c.curLimit = minSyncLimit
	}
}

// noteDecodeSuccess resets the page budget after a page decoded and
// validated, so the next request asks for the full default page again.
func (c *Client) noteDecodeSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.curLimit = defaultSyncLimit
}

// Sync pulls one page of events from peer (unary HTTP POST to /v1/sync over a
// pooled Noise session, dialed on first use).
//
// peer is the peer's transport address (advertise IP + Port, usually 8443),
// not the memberlist gossip port. expect is the peer's gossiped Noise
// identity; the responder must prove that exact node ID with a CA-signed
// parcel before any sync bytes flow, otherwise the cursor cannot
// trust whose journal it reads.
//
// req.Cursor is the cursor for "events we already have from this peer"
// (often the last applied event id / mark). Empty means from the start.
// req.Limit caps how many events to return so the transfer fits a short link.
//
// This call does not push our journal to the peer; it only asks for theirs.
// The peer responds with Events (after the cursor, up to limit) and
// NextCursor to store for the next Sync.
//
// Sessions are pooled per peer and reused across pages until they age
// out, error, or the peer's key changes; a transport failure evicts the
// session and retries the page once over a fresh handshake.
func (c *Client) Sync(ctx context.Context, peer netip.AddrPort, expect Peer, req SyncRequest) (SyncResponse, error) {
	if err := ctx.Err(); err != nil {
		return SyncResponse{}, fmt.Errorf("context cancelled: %w", err)
	}
	if !peer.IsValid() {
		return SyncResponse{}, fmt.Errorf("invalid peer address") //nolint:perfsprint // plain error
	}
	if len(expect.Pub) != 32 {
		return SyncResponse{}, fmt.Errorf("peer public key must be 32 bytes, got %d", len(expect.Pub))
	}

	resp, evicted, err := c.doSync(ctx, peer, expect, req)
	if err == nil || !evicted {
		return resp, err
	}
	resp, _, err = c.doSync(ctx, peer, expect, req)
	return resp, err
}

// doSync runs one page over the pooled session. It reports whether the
// session was evicted: only transport failures evict and merit a retry.
// Validation failures leave a healthy session pooled and never retry,
// since the same page would fail identically.
func (c *Client) doSync(ctx context.Context, peer netip.AddrPort, expect Peer, req SyncRequest) (SyncResponse, bool, error) {
	sess, err := c.session(ctx, peer, expect)
	if err != nil {
		return SyncResponse{}, false, err
	}
	defer c.release(sess)

	httpReq, err := c.newSyncRequest(ctx, peer, req)
	if err != nil {
		return SyncResponse{}, false, err
	}

	status, body, err := c.roundTrip(sess, httpReq)
	if err != nil {
		c.evict(sess)
		return SyncResponse{}, true, fmt.Errorf("sync %s: %w", peer, err)
	}
	resp, err := c.decodeResponse(peer, status, body, req.Cursor)
	if err != nil {
		return SyncResponse{}, false, err
	}
	return resp, false, nil
}

// newSyncRequest encodes one sync page request. Encoding touches no
// session state, so a retry rebuilds the request from the same SyncRequest.
func (c *Client) newSyncRequest(ctx context.Context, peer netip.AddrPort, req SyncRequest) (*http.Request, error) {
	var buf bytes.Buffer
	req.Limit = c.limitFor(req.Limit)
	err := json.NewEncoder(&buf).Encode(req)
	if err != nil {
		return nil, fmt.Errorf("marshal sync request: %w", err)
	}

	hostport := net.JoinHostPort(peer.Addr().String(), strconv.FormatUint(uint64(peer.Port()), 10))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+hostport+SyncPath, &buf)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	return httpReq, nil
}

// session returns the pooled session for a peer, dialing and verifying
// a fresh one when none is cached, the cached key differs, or the cached
// session aged out. Idle expired sessions are swept on the way through.
// The handshake may hold the pool mutex; it is bounded by the handshake
// timeout and Sync callers already serialize per peer per tick.
func (c *Client) session(ctx context.Context, peer netip.AddrPort, expect Peer) (*pooledSession, error) {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	c.sweepSessionsLocked()

	key := expect.ID.String()
	var pub [32]byte
	copy(pub[:], expect.Pub)
	if sess, ok := c.sessions[key]; ok && sess.pub == pub && time.Since(sess.established) < maxSessionAge {
		sess.refs++
		return sess, nil
	}
	return c.dialLocked(ctx, peer, expect, key, pub)
}

// dialLocked handshakes a fresh session and pools it, replacing any
// older session for the peer. The caller holds sessMu.
func (c *Client) dialLocked(ctx context.Context, peer netip.AddrPort, expect Peer, key string, pub [32]byte) (*pooledSession, error) {
	conn, parcel, err := c.dial(ctx, peer, expect)
	if err != nil {
		return nil, err
	}

	err = c.verifyResponder(expect, parcel)
	if err != nil {
		_ = conn.Close() //nolint:errcheck // best-effort close on failed verify
		return nil, fmt.Errorf("verify responder %s: %w", peer, err)
	}

	if old, ok := c.sessions[key]; ok {
		delete(c.sessions, key)
		_ = old.conn.Close() //nolint:errcheck // replaced session, best-effort close
		old.transport.CloseIdleConnections()
	}

	sess := &pooledSession{
		key:         key,
		conn:        conn,
		pub:         pub,
		established: time.Now(),
		refs:        1,
	}
	sess.transport = &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return conn, nil
		},
		MaxIdleConnsPerHost: 1,
		IdleConnTimeout:     sessionIdleTimeout,
	}
	c.sessions[key] = sess
	return sess, nil
}

// release drops one in-flight reference. Sessions stay pooled until
// eviction, expiry, or key change.
func (c *Client) release(sess *pooledSession) {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	sess.refs--
}

// evict drops a session from the pool and closes it, but only when it
// is still the pooled one: a concurrent Sync may already have replaced
// it, and the replacement must survive.
func (c *Client) evict(sess *pooledSession) {
	c.sessMu.Lock()
	defer c.sessMu.Unlock()
	c.evictLocked(sess)
}

// evictLocked drops and closes sess when pooled. The caller holds sessMu.
func (c *Client) evictLocked(sess *pooledSession) {
	if current, ok := c.sessions[sess.key]; !ok || current != sess {
		return
	}
	delete(c.sessions, sess.key)
	_ = sess.conn.Close() //nolint:errcheck // evicted session, best-effort close
	sess.transport.CloseIdleConnections()
}

// sweepSessionsLocked closes idle sessions past the age cap. Sessions
// with in-flight requests are skipped; they age out on a later Sync.
func (c *Client) sweepSessionsLocked() {
	for _, sess := range c.sessions {
		if sess.refs == 0 && time.Since(sess.established) >= maxSessionAge {
			c.evictLocked(sess)
		}
	}
}

// roundTrip runs one HTTP request over a pooled session and returns the
// capped body. The transport stays open for the next page; eviction owns
// every close. Reading the body here keeps ownership local: nothing
// unclosed crosses function boundaries.
func (c *Client) roundTrip(sess *pooledSession, httpReq *http.Request) (int, []byte, error) {
	client := &http.Client{Timeout: c.getTimeout(), Transport: sess.transport}
	httpResp, err := client.Do(httpReq)
	if err != nil {
		return 0, nil, fmt.Errorf("do sync request: %w", err)
	}
	defer func() {
		_ = httpResp.Body.Close() //nolint:errcheck // body fully read below
	}()

	body, err := io.ReadAll(io.LimitReader(httpResp.Body, 1<<20)) // 1 MiB.
	if err != nil {
		return 0, nil, fmt.Errorf("read sync response: %w", err)
	}
	return httpResp.StatusCode, body, nil
}

// decodeResponse validates one capped sync page body.
// expectedCursor is the request cursor the empty page must echo; see
// validateSyncResponse. A regressive NextCursor keeps the current cursor
// with a warn instead of failing; see clampRegressiveCursor.
func (c *Client) decodeResponse(peer netip.AddrPort, status int, body []byte, expectedCursor string) (SyncResponse, error) {
	if status != http.StatusOK {
		return SyncResponse{}, fmt.Errorf("sync %s: status %d: %s", peer, status, bytes.TrimSpace(body))
	}

	var resp SyncResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		c.noteDecodeFailure()
		return SyncResponse{}, fmt.Errorf("decode sync response: %w", err)
	}
	if err := validateSyncResponse(peer, resp, expectedCursor); err != nil {
		return SyncResponse{}, fmt.Errorf("sync %s: %w", peer, err)
	}
	resp, _ = clampRegressiveCursor(expectedCursor, resp, c.logger)
	c.noteDecodeSuccess()
	return resp, nil
}

// maxSyncEventPayload caps one event payload in a sync page at 1 MiB.
// The response body is already capped at 1 MiB, and the per-event cap
// stops one giant event plus empty rest from squeezing through while a
// second event overflows the total.
const maxSyncEventPayload = 1 << 20

// syncMaxClockSkew bounds how far an event timestamp may lie in the future.
// Zero timestamps are invalid (never set by journal appends); future
// timestamps beyond one hour of skew are invalid (clock lie or forgery).
// Past timestamps carry no bound: old history must keep syncing.
const syncMaxClockSkew = time.Hour

// validateSyncResponse fails closed on an oversized or malformed page.
// The event cap is twice maxSyncLimit so a buggy or hostile peer cannot
// push an unbounded decode into the caller. NextCursor must be empty or a
// UUID, and when events are present a non-empty cursor must bind to the
// last event id so a peer cannot redirect the caller's stored cursor.
// When expectedCursor is given, an empty page must echo the request cursor
// (or carry an empty cursor); any other id is a foreign injection and is
// rejected. Without it the legacy rule applies: any UUID echo passes, kept
// for callers that bind the cursor elsewhere. Every event must carry a
// usable identity and fresh-enough metadata: nil IDs, empty types, nil
// node IDs, zero or far-future timestamps, missing or oversized payloads,
// and duplicate IDs are all rejected rather than passed to the caller.
//
// peersync advances on echo, so the echo rule keeps honest servers working:
// their empty pages carry exactly the requested cursor.
func validateSyncResponse(peer netip.AddrPort, resp SyncResponse, expectedCursor ...string) error {
	if len(resp.Events) > 2*maxSyncLimit {
		return fmt.Errorf("sync %s: too many events %d", peer, len(resp.Events))
	}
	if len(resp.Events) == 0 && len(expectedCursor) > 0 {
		return checkEmptyEcho(peer, resp, expectedCursor[0])
	}
	if resp.NextCursor != "" {
		if _, err := uuid.Parse(resp.NextCursor); err != nil {
			return fmt.Errorf("sync %s: bad next cursor: %w", peer, err)
		}
		if len(resp.Events) > 0 && resp.NextCursor != resp.Events[len(resp.Events)-1].ID.String() {
			return fmt.Errorf("sync %s: next cursor %q does not match last event %s", peer, resp.NextCursor, resp.Events[len(resp.Events)-1].ID)
		}
	}
	seen := make(map[uuid.UUID]struct{}, len(resp.Events))
	for i := range resp.Events {
		if err := validateSyncEvent(peer, &resp.Events[i], i, seen); err != nil {
			return err
		}
	}
	return nil
}

// checkEmptyEcho rejects a non-empty NextCursor on an empty page that does
// not echo the request cursor. An empty cursor always passes: it names
// nothing-new, which advance ignores either way.
func checkEmptyEcho(peer netip.AddrPort, resp SyncResponse, expected string) error {
	if resp.NextCursor == "" {
		return nil
	}
	if resp.NextCursor != normalizeCursor(expected) {
		return fmt.Errorf("sync %s: next cursor %q does not echo request", peer, resp.NextCursor)
	}
	return nil
}

// clampRegressiveCursor keeps the cursor monotonic when request and
// response cursors are comparable. UUIDs carry no order, so backward is
// only observable in two shapes: a foreign id on an empty page is rejected
// by validation, and an echo of the request cursor on a non-empty page is a
// replay of already-held events. The replay keeps the current cursor with
// a warn instead of failing: the events still apply idempotently, and the
// caller's advance becomes a no-op instead of a rewind. It reports whether
// the page was a replay.
func clampRegressiveCursor(expected string, resp SyncResponse, logger *logs.Logger) (SyncResponse, bool) {
	if len(resp.Events) == 0 {
		return resp, false
	}
	want := normalizeCursor(expected)
	if want == "" || resp.NextCursor != want {
		return resp, false
	}
	logger.Warn("sync regressive cursor, keeping current",
		logs.String("cursor", want),
		logs.Int("events", len(resp.Events)),
	)
	return resp, true
}

// validateSyncEvent checks one page event: identity, clock, payload, dup.
func validateSyncEvent(peer netip.AddrPort, ev *journal.Event, i int, seen map[uuid.UUID]struct{}) error {
	if ev.ID == uuid.Nil {
		return fmt.Errorf("sync %s: event %d has nil id", peer, i)
	}
	if _, dup := seen[ev.ID]; dup {
		return fmt.Errorf("sync %s: event %s duplicated", peer, ev.ID)
	}
	seen[ev.ID] = struct{}{}
	if ev.Type == "" {
		return fmt.Errorf("sync %s: event %s has empty type", peer, ev.ID)
	}
	if ev.NodeID == uuid.Nil {
		return fmt.Errorf("sync %s: event %s has nil node", peer, ev.ID)
	}
	if ev.Timestamp.IsZero() {
		return fmt.Errorf("sync %s: event %s has zero timestamp", peer, ev.ID)
	}
	if ev.Timestamp.After(time.Now().Add(syncMaxClockSkew)) {
		return fmt.Errorf("sync %s: event %s timestamp %s is beyond skew", peer, ev.ID, ev.Timestamp.Format(time.RFC3339))
	}
	if ev.Payload == nil {
		return fmt.Errorf("sync %s: event %s has nil payload", peer, ev.ID)
	}
	if len(ev.Payload) > maxSyncEventPayload {
		return fmt.Errorf("sync %s: event %s payload %d bytes over max %d", peer, ev.ID, len(ev.Payload), maxSyncEventPayload)
	}
	return nil
}

// verifyResponder checks the responder's in-session parcel: it must claim the
// expected node ID and carry a valid CA signature over the dialed key.
// A nil verifier fails closed: no parcel is ever trusted without one.
func (c *Client) verifyResponder(expect Peer, raw []byte) error {
	if c.verify == nil {
		return fmt.Errorf("peer %s has no verifier configured", expect.ID)
	}
	id, generation, sig, err := decodeParcel(raw)
	if err != nil {
		return fmt.Errorf("decode responder parcel: %w", err)
	}

	if id != expect.ID {
		return fmt.Errorf("peer claims %s, want %s", id, expect.ID)
	}

	err = c.verify(id, generation, expect.Pub, sig)
	if err != nil {
		return fmt.Errorf("verify responder %s: %w", expect.ID, err)
	}

	return nil
}

// dial opens the TCP connection with the caller's context and runs the IK
// initiator handshake. DialContext carries cancellation; the timeout read
// under the mutex stays as the fallback when the caller sets no deadline.
// Handshake deadlines and cancel-abort live in dialHandshake.
func (c *Client) dial(ctx context.Context, peer netip.AddrPort, expect Peer) (*noiseConn, []byte, error) {
	dialer := &net.Dialer{Timeout: c.getTimeout()}

	addr := net.JoinHostPort(peer.Addr().String(), strconv.FormatUint(uint64(peer.Port()), 10))
	conn, parcel, err := dialHandshake(ctx, dialer, addr, c.static, c.parcel, expect.Pub)
	if err != nil {
		return nil, nil, fmt.Errorf("sync %s: %w", peer, err)
	}

	return conn, parcel, nil
}
