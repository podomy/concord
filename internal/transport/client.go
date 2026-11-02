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
// Client is safe for concurrent Sync. timeout and the adaptive page budget
// (curLimit) are guarded by mu; timeout is otherwise set once at
// construction (tests may assign it before first use).
type Client struct {
	static  StaticKey
	parcel  []byte
	verify  Verifier
	timeout time.Duration
	logger  *logs.Logger

	mu       sync.RWMutex
	curLimit int
}

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
// fresh Noise session).
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

	conn, parcel, err := c.dial(ctx, peer, expect)
	if err != nil {
		return SyncResponse{}, err
	}

	err = c.verifyResponder(expect, parcel)
	if err != nil {
		_ = conn.Close() //nolint:errcheck // best-effort close on failed verify
		return SyncResponse{}, fmt.Errorf("verify responder %s: %w", peer, err)
	}

	var buf bytes.Buffer
	req.Limit = c.limitFor(req.Limit)
	if err = json.NewEncoder(&buf).Encode(req); err != nil {
		_ = conn.Close() //nolint:errcheck // best-effort close on encode error
		return SyncResponse{}, fmt.Errorf("marshal sync request: %w", err)
	}

	hostport := net.JoinHostPort(peer.Addr().String(), strconv.FormatUint(uint64(peer.Port()), 10))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+hostport+SyncPath, &buf)
	if err != nil {
		_ = conn.Close() //nolint:errcheck // best-effort close on request error
		return SyncResponse{}, fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	return c.exchange(conn, peer, httpReq, req.Cursor)
}

// exchange performs one HTTP request over an established Noise session and
// decodes the sync response. The connection closes after the body is consumed.
// expectedCursor is the request cursor the empty page must echo; see
// validateSyncResponse. A regressive NextCursor keeps the current cursor
// with a warn instead of failing; see clampRegressiveCursor.
func (c *Client) exchange(conn *noiseConn, peer netip.AddrPort, httpReq *http.Request, expectedCursor string) (SyncResponse, error) {
	// Deferred first so it runs last: body first, connection after.
	defer func() {
		_ = conn.Close() //nolint:errcheck // session done, nothing to report
	}()

	// One connection per Sync: dial-once transport with keep-alives off.
	transport := &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return conn, nil
		},
		DisableKeepAlives: true,
	}
	client := &http.Client{Timeout: c.getTimeout(), Transport: transport}

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return SyncResponse{}, fmt.Errorf("sync %s: %w", peer, err)
	}
	defer func() {
		if err := httpResp.Body.Close(); err != nil {
			// Best-effort close after read; primary error is already returned.
			_ = err
		}
	}()

	limited := io.LimitReader(httpResp.Body, 1<<20) // 1 MiB.
	if httpResp.StatusCode != http.StatusOK {
		msg, readErr := io.ReadAll(limited)
		if readErr != nil {
			return SyncResponse{}, fmt.Errorf("sync %s: status %d", peer, httpResp.StatusCode)
		}
		return SyncResponse{}, fmt.Errorf("sync %s: status %d: %s", peer, httpResp.StatusCode, bytes.TrimSpace(msg))
	}

	var resp SyncResponse
	if err := json.NewDecoder(limited).Decode(&resp); err != nil {
		c.noteDecodeFailure()
		return SyncResponse{}, fmt.Errorf("decode sync response: %w", err)
	}
	if err := validateSyncResponse(peer, resp, expectedCursor); err != nil {
		return SyncResponse{}, err
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
		return err
	}

	if id != expect.ID {
		return fmt.Errorf("peer claims %s, want %s", id, expect.ID)
	}

	err = c.verify(id, generation, expect.Pub, sig)
	if err != nil {
		return err
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
