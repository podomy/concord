// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/journal"
)

// Nil verifier on the responder side fails closed with no handshake.
func TestServeHandshakeNilVerifierFailsClosed(t *testing.T) {
	fx := newHandshakeFixture(t)
	c1, c2 := testPipe(t)
	_ = c1

	_, err := serveHandshake(c2, fx.staticB, fx.parcelB, nil)
	if err == nil {
		t.Fatal("nil verifier accepted handshake")
	}
}

// Nil verifier on the client side fails closed with no parcel trust.
func TestClientVerifyResponderNilVerifierFailsClosed(t *testing.T) {
	fx := newHandshakeFixture(t)

	client := NewClient(fx.staticA, fx.parcelA, nil)
	err := client.verifyResponder(Peer{ID: fx.idB, Pub: fx.staticB.Public}, fx.parcelB)
	if err == nil {
		t.Fatal("nil verifier accepted responder parcel")
	}
}

// boundSyncResponse builds a valid two-event page with the cursor bound
// to the last event id.
func boundSyncResponse() SyncResponse {
	e1, e2 := testEvent("a"), testEvent("b")
	return SyncResponse{Events: []journal.Event{e1, e2}, NextCursor: e2.ID.String()}
}

// syncValidationCase is one row of the fail-closed page rules table.
type syncValidationCase struct {
	name  string
	build func() SyncResponse
	want  string
}

// syncValidationCases lists valid pages plus every rejection rule.
func syncValidationCases() []syncValidationCase {
	return []syncValidationCase{
		{
			name:  "bound cursor passes",
			build: boundSyncResponse,
			want:  "",
		},
		{
			name: "empty cursor with events passes",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				resp.NextCursor = ""
				return resp
			},
			want: "",
		},
		{
			name: "empty page with cursor echo passes",
			build: func() SyncResponse {
				return SyncResponse{Events: nil, NextCursor: uuid.New().String()}
			},
			want: "",
		},
		{
			name: "empty page empty cursor passes",
			build: func() SyncResponse {
				return SyncResponse{}
			},
			want: "",
		},
		{
			name: "zero timestamp rejected",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				resp.Events[0].Timestamp = time.Time{}
				return resp
			},
			want: "zero timestamp",
		},
		{
			name: "far future timestamp rejected",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				resp.Events[1].Timestamp = time.Now().Add(2 * time.Hour)
				return resp
			},
			want: "beyond skew",
		},
		{
			name: "nil payload rejected",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				resp.Events[0].Payload = nil
				return resp
			},
			want: "nil payload",
		},
		{
			name: "oversize payload rejected",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				resp.Events[0].Payload = bytes.Repeat([]byte("a"), maxSyncEventPayload+1)
				return resp
			},
			want: "over max",
		},
		{
			name: "duplicate ids rejected",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				resp.Events[1].ID = resp.Events[0].ID
				resp.NextCursor = resp.Events[0].ID.String()
				return resp
			},
			want: "duplicated",
		},
		{
			name: "cursor mismatch rejected",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				resp.NextCursor = uuid.New().String()
				return resp
			},
			want: "does not match last event",
		},
		{
			name: "bad cursor rejected",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				resp.NextCursor = "not-a-uuid"
				return resp
			},
			want: "bad next cursor",
		},
		{
			name: "too many events rejected",
			build: func() SyncResponse {
				resp := boundSyncResponse()
				for range 2*maxSyncLimit + 1 {
					resp.Events = append(resp.Events, testEvent("flood"))
				}
				return resp
			},
			want: "too many events",
		},
	}
}

// Sync response validation: table over the fail-closed page rules.
func TestValidateSyncResponseTable(t *testing.T) {
	t.Parallel()

	peer := netip.MustParseAddrPort("127.0.0.1:8443")

	for _, tc := range syncValidationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateSyncResponse(peer, tc.build())
			if tc.want == "" {
				if err != nil {
					t.Fatalf("valid page rejected: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("invalid page accepted, want %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want %q", err, tc.want)
			}
		})
	}
}

// A missing journal file serves an empty page, not a 500.
func TestMissingJournalServesEmptyPage(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	events, next, err := loadSyncPage(context.Background(), nil, "", 10)
	if err != nil {
		t.Fatalf("missing journal: %v", err)
	}
	if len(events) != 0 || next != "" {
		t.Fatalf("got len=%d next=%q, want empty page", len(events), next)
	}
	if events == nil {
		t.Fatal("events must encode as [], not null")
	}

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, strings.NewReader(`{"cursor":"","limit":10}`))
	rec := httptest.NewRecorder()
	postSync(rec, req, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("missing journal status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"events":[]`) {
		t.Fatalf("body = %s, want empty events array", rec.Body.String())
	}
}

// The transport server reaps idle keep-alives like the registry.
func TestNewTransportServerIdleTimeout(t *testing.T) {
	t.Parallel()

	srv := newTransportServer(http.NewServeMux())
	if srv.IdleTimeout != 120*time.Second {
		t.Fatalf("IdleTimeout = %s, want 120s", srv.IdleTimeout)
	}
}

// Exhaustion detection matches the noise terminal error text.
func TestIsNonceExhausted(t *testing.T) {
	t.Parallel()

	if !isNonceExhausted(errors.New("noise: transport nonce exhausted")) {
		t.Fatal("exhaustion not detected")
	}
	if !isNonceExhausted(errors.New("noise decrypt: noise: transport nonce exhausted")) {
		t.Fatal("wrapped exhaustion not detected")
	}
	if isNonceExhausted(nil) {
		t.Fatal("nil reports exhausted")
	}
	if isNonceExhausted(errors.New("noise: transport decrypt: tag mismatch")) {
		t.Fatal("decrypt failure reports exhausted")
	}
}

// Terminal nonce exhaustion closes the conn: no half-open session.
func TestCloseOnTerminalClosesConn(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() { _ = c2.Close() }) //nolint:errcheck // best-effort test cleanup

	nc := &noiseConn{conn: c1}
	cause := errors.New("noise: transport nonce exhausted")
	if err := c2.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	err := nc.closeOnTerminal(cause)
	if err == nil {
		t.Fatal("exhaustion swallowed")
	}
	if !errors.Is(err, cause) {
		t.Fatalf("error = %v, want wrapped cause", err)
	}
	if _, rerr := c2.Read(make([]byte, 1)); rerr == nil {
		t.Fatal("conn left half-open after exhaustion")
	}
}

// Non-terminal errors leave the conn open for the caller.
func TestCloseOnTerminalPassThrough(t *testing.T) {
	c1, c2 := net.Pipe()
	t.Cleanup(func() { _ = c1.Close() }) //nolint:errcheck // best-effort test cleanup
	t.Cleanup(func() { _ = c2.Close() }) //nolint:errcheck // best-effort test cleanup

	nc := &noiseConn{conn: c1}
	cause := errors.New("boom")
	if err := nc.closeOnTerminal(cause); !errors.Is(err, cause) {
		t.Fatalf("error = %v, want passthrough", err)
	}

	go func() {
		_, _ = c1.Write([]byte("x")) //nolint:errcheck // test roundtrip
	}()
	if err := c2.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	buf := make([]byte, 1)
	if _, err := io.ReadFull(c2, buf); err != nil || buf[0] != 'x' {
		t.Fatalf("conn broken by non-terminal error: %v", err)
	}
}

// A run of empty-plaintext frames past the cumulative per-connection cap
// fails one Read. Round4 changed the cap from per-Read to cumulative, so
// this drives the new total.
func TestNoiseConnEmptyFrameCap(t *testing.T) {
	fx := newHandshakeFixture(t)
	c1, c2 := testPipe(t)

	type result struct {
		conn *noiseConn
		err  error
	}
	initCh := make(chan result, 1)
	go func() {
		conn, _, err := initiatorHandshake(c1, fx.staticA, fx.parcelA, fx.staticB.Public)
		initCh <- result{conn: conn, err: err}
	}()
	resp, err := serveHandshake(c2, fx.staticB, fx.parcelB, testVerifier(t, fx))
	if err != nil {
		t.Fatalf("responder: %v", err)
	}
	t.Cleanup(func() { _ = resp.Close() }) //nolint:errcheck // test cleanup
	initRes := <-initCh
	if initRes.err != nil {
		t.Fatalf("initiator: %v", initRes.err)
	}
	init := initRes.conn
	t.Cleanup(func() { _ = init.Close() }) //nolint:errcheck // test cleanup

	writeCh := make(chan error, 1)
	go func() {
		for range maxEmptyFramesTotal + 1 {
			empty, encErr := init.send.Encrypt(nil, nil, []byte{})
			if encErr != nil {
				writeCh <- encErr
				return
			}
			if werr := writeFrame(init.conn, empty); werr != nil {
				writeCh <- werr
				return
			}
		}
		writeCh <- nil
	}()

	if _, rerr := resp.Read(make([]byte, 1)); rerr == nil {
		t.Fatal("empty frame run accepted past cap")
	} else if !strings.Contains(rerr.Error(), "empty frames") {
		t.Fatalf("error = %v, want empty frame cap", rerr)
	}
	if werr := <-writeCh; werr != nil {
		t.Fatalf("writer: %v", werr)
	}
}
