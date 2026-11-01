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
	"time"

	"github.com/google/uuid"
)

// SyncPath is the node-to-node unary sync endpoint.
const SyncPath = "/v1/sync"

// Client is a Noise IK client for Concord peer transport.
// It presents this node's static key and parcel, dials peers whose static
// key arrives via gossip, and verifies each responder's parcel before syncing.
type Client struct {
	static  StaticKey
	parcel  []byte
	verify  Verifier
	timeout time.Duration
}

// NewClient builds a Client using this node's static key and encoded parcel.
// verify checks each responder before syncing; see KeyPinner in runtime.
func NewClient(static StaticKey, parcel []byte, verify Verifier) *Client {
	return &Client{
		static:  static,
		parcel:  parcel,
		verify:  verify,
		timeout: 10 * time.Second,
	}
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
		return SyncResponse{}, err
	}

	var buf bytes.Buffer
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

	return c.exchange(conn, peer, httpReq)
}

// exchange performs one HTTP request over an established Noise session and
// decodes the sync response. The connection closes after the body is consumed.
func (c *Client) exchange(conn *noiseConn, peer netip.AddrPort, httpReq *http.Request) (SyncResponse, error) {
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
	client := &http.Client{Timeout: c.timeout, Transport: transport}

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
		return SyncResponse{}, fmt.Errorf("decode sync response: %w", err)
	}
	if err := validateSyncResponse(peer, resp); err != nil {
		return SyncResponse{}, err
	}
	return resp, nil
}

// validateSyncResponse fails closed on an oversized or malformed page.
// The event cap is twice maxSyncLimit so a buggy or hostile peer cannot
// push an unbounded decode into the caller. NextCursor must be empty or a
// UUID, and every event must carry a usable identity: nil IDs, empty types,
// or nil node IDs are rejected rather than passed to the caller.
func validateSyncResponse(peer netip.AddrPort, resp SyncResponse) error {
	if len(resp.Events) > 2*maxSyncLimit {
		return fmt.Errorf("sync %s: too many events %d", peer, len(resp.Events))
	}
	if resp.NextCursor != "" {
		if _, err := uuid.Parse(resp.NextCursor); err != nil {
			return fmt.Errorf("sync %s: bad next cursor: %w", peer, err)
		}
	}
	for i := range resp.Events {
		ev := &resp.Events[i]
		if ev.ID == uuid.Nil {
			return fmt.Errorf("sync %s: event %d has nil id", peer, i)
		}
		if ev.Type == "" {
			return fmt.Errorf("sync %s: event %s has empty type", peer, ev.ID)
		}
		if ev.NodeID == uuid.Nil {
			return fmt.Errorf("sync %s: event %s has nil node", peer, ev.ID)
		}
	}
	return nil
}

// verifyResponder checks the responder's in-session parcel: it must claim the
// expected node ID and carry a valid CA signature over the dialed key.
func (c *Client) verifyResponder(expect Peer, raw []byte) error {
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
// initiator handshake. DialContext carries cancellation; Timeout stays as the
// fallback when the caller sets no deadline. Handshake deadlines and
// cancel-abort live in dialHandshake.
func (c *Client) dial(ctx context.Context, peer netip.AddrPort, expect Peer) (*noiseConn, []byte, error) {
	dialer := &net.Dialer{Timeout: c.timeout}

	addr := net.JoinHostPort(peer.Addr().String(), strconv.FormatUint(uint64(peer.Port()), 10))
	conn, parcel, err := dialHandshake(ctx, dialer, addr, c.static, c.parcel, expect.Pub)
	if err != nil {
		return nil, nil, fmt.Errorf("sync %s: %w", peer, err)
	}

	return conn, parcel, nil
}
