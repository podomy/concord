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
)

// SyncPath is the node-to-node unary sync endpoint.
const SyncPath = "/v1/sync"

// Client is an HTTPS/2 mTLS client for Concord peer transport.
// It presents this node's cert and trusts peers signed by the same CA.
type Client struct {
	http *http.Client
}

// NewClient builds a Client using the same TLS material as the transport server.
// caFile, certFile, and keyFile are the paths from certs.Ensure.
func NewClient(caFile, certFile, keyFile string) (*Client, error) {
	tlsCfg, err := loadClientTLSConfig(caFile, certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("client tls: %w", err)
	}

	return &Client{
		http: &http.Client{
			Timeout: 10 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig:   tlsCfg,
				ForceAttemptHTTP2: true,
			},
		},
	}, nil
}

// Sync pulls one page of events from peer (unary HTTPS POST to /v1/sync).
//
// peer is the peer's transport address (advertise IP + Port, usually 8443),
// not the memberlist gossip port.
//
// req.Watermark is the cursor for "events we already have from this peer"
// (often the last applied event id / mark). Empty means from the start.
// req.Limit caps how many events to return so the transfer fits a short link.
//
// This call does not push our journal to the peer; it only asks for theirs.
// The peer responds with Events (after the watermark, up to limit) and
// NextWatermark to store for the next Sync.
func (c *Client) Sync(ctx context.Context, peer netip.AddrPort, req SyncRequest) (SyncResponse, error) {
	if err := ctx.Err(); err != nil {
		return SyncResponse{}, fmt.Errorf("context cancelled: %w", err)
	}
	if !peer.IsValid() {
		return SyncResponse{}, fmt.Errorf("invalid peer address") //nolint:perfsprint // plain error
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(req); err != nil {
		return SyncResponse{}, fmt.Errorf("marshal sync request: %w", err)
	}

	hostport := net.JoinHostPort(peer.Addr().String(), strconv.FormatUint(uint64(peer.Port()), 10))
	url := "https://" + hostport + SyncPath
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, &buf)
	if err != nil {
		return SyncResponse{}, fmt.Errorf("new request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := c.http.Do(httpReq)
	if err != nil {
		return SyncResponse{}, fmt.Errorf("sync %s: %w", peer, err)
	}
	defer func() {
		if err := httpResp.Body.Close(); err != nil {
			// Best-effort close after read; primary error is already returned.
			_ = err
		}
	}()

	limited := io.LimitReader(httpResp.Body, 1<<20) // 1 MiB
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
	return resp, nil
}
