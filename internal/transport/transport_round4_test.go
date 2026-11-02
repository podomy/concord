// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalreader"
	"github.com/podomy/concord/internal/logs"
)

// handshakePair completes one IK handshake over a piped pair and registers
// cleanup closing both ends. Close is idempotent, so tests that close
// early stay harmless under the deferred close.
func handshakePair(t *testing.T) (init, resp *noiseConn) {
	t.Helper()
	fx := newHandshakeFixture(t)
	c1, c2 := testPipe(t)
	initCh := make(chan *noiseConn, 1)
	go func() {
		conn, _, _ := initiatorHandshake(c1, fx.staticA, fx.parcelA, fx.staticB.Public) //nolint:errcheck // test handshake
		initCh <- conn
	}()
	resp, err := serveHandshake(c2, fx.staticB, fx.parcelB, testVerifier(t, fx))
	if err != nil {
		t.Fatalf("responder: %v", err)
	}
	t.Cleanup(func() { _ = resp.Close() }) //nolint:errcheck // test cleanup
	init = <-initCh
	if init == nil {
		t.Fatal("handshake failed")
	}
	t.Cleanup(func() { _ = init.Close() }) //nolint:errcheck // test cleanup
	return init, resp
}

// A stalled dialer must not wedge honest peers: the handshake runs in the
// per-connection serve path, so Accept returns fast and the honest Sync
// completes while the stall is still open.
func TestDeferredHandshakeHonestProceedsDespiteStall(t *testing.T) {
	fx := newHandshakeFixture(t)
	caCert, err := LoadCACert(fx.caFile)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	serverVerify := CAVerifier(caCert)
	clientVerify := CAVerifier(caCert)

	raw := testListen(t)
	listener := &noiseListener{
		Listener: raw,
		static:   fx.staticB,
		parcel:   fx.parcelB,
		verify:   serverVerify,
		logger:   logs.NewNop(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+SyncPath, func(w http.ResponseWriter, r *http.Request) {
		serveSyncPage(w, r, nil, logs.NewNop())
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = srv.Serve(listener) //nolint:errcheck // test server ends with test
	}()
	t.Cleanup(func() {
		_ = srv.Close() //nolint:errcheck // test cleanup
	})

	// Stall: connect and send nothing, holding one serve goroutine in its
	// handshake until the deadline.
	dialer := &net.Dialer{}
	stall, err := dialer.DialContext(context.Background(), "tcp", raw.Addr().String())
	if err != nil {
		t.Fatalf("stall dial: %v", err)
	}
	t.Cleanup(func() { _ = stall.Close() }) //nolint:errcheck // test cleanup

	addr, err := netip.ParseAddrPort(raw.Addr().String())
	if err != nil {
		t.Fatalf("parse addr: %v", err)
	}
	client := NewClient(fx.staticA, fx.parcelA, clientVerify)
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = client.Sync(ctx, addr, Peer{ID: fx.idB, Pub: fx.staticB.Public}, SyncRequest{Limit: 10})
	if err != nil {
		t.Fatalf("honest sync: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("honest sync took %s, want under 5s despite stall", elapsed)
	}
}

// Server drops malformed events with a warn count and ships the rest with
// the cursor rebound to the surviving tail.
func TestServeSyncDropsBadEvents(t *testing.T) {
	dir := t.TempDir()
	good1, good2 := testEvent("g1"), testEvent("g2")
	bad := testEvent("bad")
	bad.Type = ""
	writeEnvJournal(t, dir, good1, bad, good2)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, strings.NewReader(`{"cursor":"","limit":10}`))
	rec := httptest.NewRecorder()
	serveSyncPage(rec, req, nil, logs.NewNop())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp SyncResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Events) != 2 || resp.Events[0].ID != good1.ID || resp.Events[1].ID != good2.ID {
		t.Fatalf("got %d events, want [good1 good2]", len(resp.Events))
	}
	if resp.NextCursor != good2.ID.String() {
		t.Fatalf("next = %q, want good2", resp.NextCursor)
	}
}

// An all-dropped page echoes the request cursor so the client reads
// nothing-new instead of a foreign id.
func TestServeSyncAllBadEchoesCursor(t *testing.T) {
	dir := t.TempDir()
	// A nil payload round-trips as the four bytes "null", so it is not
	// wire-invalid; use a zero timestamp and an empty type, which stay
	// invalid across the JSON round trip.
	bad1 := testEvent("bad1")
	bad1.Timestamp = time.Time{}
	bad2 := testEvent("bad2")
	bad2.Type = ""
	writeEnvJournal(t, dir, bad1, bad2)

	unknown := uuid.New().String()
	body := []byte(`{"cursor":"` + unknown + `","limit":10}`)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, bytes.NewReader(body))
	rec := httptest.NewRecorder()
	serveSyncPage(rec, req, nil, logs.NewNop())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp SyncResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Events) != 0 {
		t.Fatalf("got %d events, want 0", len(resp.Events))
	}
	if resp.NextCursor != unknown {
		t.Fatalf("next = %q, want echo %q", resp.NextCursor, unknown)
	}
}

// A dropped tail rebinds the cursor to the surviving tail instead of
// failing: the page makes progress past the corruption.
func TestServeSyncDroppedTailRebinds(t *testing.T) {
	dir := t.TempDir()
	good := testEvent("good")
	badTail := testEvent("bad")
	badTail.Timestamp = time.Time{}
	writeEnvJournal(t, dir, good, badTail)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, strings.NewReader(`{"cursor":"","limit":10}`))
	rec := httptest.NewRecorder()
	serveSyncPage(rec, req, nil, logs.NewNop())
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp SyncResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Events) != 1 || resp.Events[0].ID != good.ID {
		t.Fatalf("got %d events, want [good]", len(resp.Events))
	}
	if resp.NextCursor != good.ID.String() {
		t.Fatalf("next = %q, want rebound good", resp.NextCursor)
	}
}

// Outbound page faults fail the page: bad count, bad cursor shape, and an
// unbound cursor are all server bugs the client must never see.
func TestCheckOutboundPageFaults(t *testing.T) {
	e1, e2 := testEvent("a"), testEvent("b")
	good := []journal.Event{e1, e2}
	bound := e2.ID.String()

	huge := make([]journal.Event, maxSyncLimit+1)
	for i := range huge {
		huge[i] = testEvent("flood")
	}
	badMid := testEvent("bad")
	badMid.Type = ""
	mixed := []journal.Event{e1, badMid, e2}

	cases := []struct {
		name        string
		events      []journal.Event
		next        string
		wantErr     string
		wantDropped int
		wantLen     int
	}{
		{name: "bad cursor shape", events: good, next: "not-a-uuid", wantErr: "next cursor"},
		{name: "unbound cursor", events: good, next: uuid.New().String(), wantErr: "does not match"},
		{name: "over count", events: huge, next: bound, wantErr: "over max"},
		{name: "valid page", events: good, next: bound, wantLen: 2},
		{name: "dropped middle rebinds", events: mixed, next: bound, wantDropped: 1, wantLen: 2},
	}
	for _, tc := range cases {
		filtered, dropped, firstReason, err := checkOutboundPage(tc.events, tc.next)
		if tc.wantErr == "" && err != nil {
			t.Fatalf("%s rejected: %v", tc.name, err)
		}
		if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
			t.Fatalf("%s = %v, want %q", tc.name, err, tc.wantErr)
		}
		if err == nil && (dropped != tc.wantDropped || len(filtered) != tc.wantLen) {
			t.Fatalf("%s dropped=%d len=%d, want %d/%d (%v)", tc.name, dropped, len(filtered), tc.wantDropped, tc.wantLen, firstReason)
		}
	}
}

// An unknown UUID serves from-start through exactly one journal open: the
// miss scan and the from-start page share one handle via seek-back.
func TestLoadSyncPageUnknownUUIDSingleOpen(t *testing.T) {
	dir := t.TempDir()
	e1, e2 := testEvent("a"), testEvent("b")
	writeEnvJournal(t, dir, e1, e2)

	opens := 0
	prev := openJournalReader
	openJournalReader = func() (*journalreader.JSONLReader, error) {
		opens++
		return journalreader.OpenJSONLReader()
	}
	t.Cleanup(func() { openJournalReader = prev })

	got, next, err := loadSyncPage(context.Background(), nil, uuid.New().String(), 10)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if opens != 1 {
		t.Fatalf("unknown cursor opened journal %d times, want 1", opens)
	}
	if len(got) != 2 || got[0].ID != e1.ID || got[1].ID != e2.ID {
		t.Fatalf("got %d events, want from-start pair", len(got))
	}
	if next != e2.ID.String() {
		t.Fatalf("next = %q, want e2", next)
	}
}

// Repeat full scans for one cursor are throttled; a denied scan serves an
// empty echo, while the empty cursor always passes.
func TestFullScanGateThrottlesRepeats(t *testing.T) {
	now := time.Now()
	g := newScanGate(time.Minute, func() time.Time { return now })
	if !g.allow("c1") {
		t.Fatal("first scan denied")
	}
	if g.allow("c1") {
		t.Fatal("repeat scan allowed within interval")
	}
	if !g.allow("c2") {
		t.Fatal("fresh cursor denied")
	}
	now = now.Add(2 * time.Minute)
	if !g.allow("c1") {
		t.Fatal("scan denied after interval")
	}

	prev := fullScanGate
	fullScanGate = newScanGate(time.Hour, time.Now)
	t.Cleanup(func() { fullScanGate = prev })

	dir := t.TempDir()
	e1 := testEvent("a")
	writeEnvJournal(t, dir, e1)

	unknown := uuid.New().String()
	got1, _, err := loadSyncPage(context.Background(), nil, unknown, 10)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	if len(got1) != 1 {
		t.Fatalf("first load got %d events, want from-start page", len(got1))
	}
	got2, next2, err := loadSyncPage(context.Background(), nil, unknown, 10)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if len(got2) != 0 || next2 != unknown {
		t.Fatalf("throttled load = len %d next %q, want empty echo", len(got2), next2)
	}
	got3, _, err := loadSyncPage(context.Background(), nil, "", 10)
	if err != nil {
		t.Fatalf("from-start load: %v", err)
	}
	if len(got3) != 1 {
		t.Fatalf("from-start load got %d events, want 1", len(got3))
	}
}

// Pages stop past the byte budget with NextCursor on the last shipped
// event; the over-budget event is rewound, not skipped.
func TestReadPageForwardByteBudget(t *testing.T) {
	mk := func() journal.Event {
		// A JSON string literal: valid payload bytes without a marshal.
		// Three ~150 KiB events fit the ~512 KiB page budget; the
		// fourth trips it.
		raw := []byte(`"` + strings.Repeat("a", 150<<10) + `"`)
		return journal.Event{
			ID:        uuid.New(),
			Type:      "test.big",
			NodeID:    uuid.New(),
			Timestamp: time.Now().UTC(),
			Payload:   raw,
		}
	}
	events := []journal.Event{mk(), mk(), mk(), mk()}
	dir := t.TempDir()
	writeEnvJournal(t, dir, events...)

	got, next, err := loadSyncPage(context.Background(), nil, "", 10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d events, want 3 inside budget", len(got))
	}
	if next != events[2].ID.String() {
		t.Fatalf("next = %q, want third event", next)
	}
	// Resume through a fresh handle like a real client: the rewound
	// fourth event must not have been skipped.
	rest, restNext, err := loadSyncPage(context.Background(), nil, next, 10)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if len(rest) != 1 || rest[0].ID != events[3].ID {
		t.Fatalf("resume got %d events, want the rewound fourth", len(rest))
	}
	if restNext != events[3].ID.String() {
		t.Fatalf("resume next = %q, want fourth event", restNext)
	}
}

// One giant event still ships alone instead of stalling the cursor.
func TestReadPageForwardSingleGiantEvent(t *testing.T) {
	raw := []byte(`"` + strings.Repeat("a", 600<<10) + `"`)
	big := journal.Event{
		ID:        uuid.New(),
		Type:      "test.giant",
		NodeID:    uuid.New(),
		Timestamp: time.Now().UTC(),
		Payload:   raw,
	}
	r := openTempJournal(t, big)

	got, next, _, _, err := readJournalPage(context.Background(), r, "", 10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 1 || next != big.ID.String() {
		t.Fatalf("got len=%d next=%q, want lone giant", len(got), next)
	}
}

// Decode failures halve the requested limit to a floor of 10; success
// resets to the default page.
func TestClientAdaptiveLimit(t *testing.T) {
	c := NewClient(StaticKey{}, nil, nil)
	if got := c.limitFor(100); got != 100 {
		t.Fatalf("limitFor(100) = %d, want 100", got)
	}
	if got := c.limitFor(0); got != 100 {
		t.Fatalf("limitFor(0) = %d, want 100", got)
	}
	if got := c.limitFor(1000); got != 100 {
		t.Fatalf("limitFor(1000) = %d, want capped 100", got)
	}

	want := []int{50, 25, 12, 10, 10}
	for i, w := range want {
		c.noteDecodeFailure()
		if got := c.limitFor(100); got != w {
			t.Fatalf("after %d failures limitFor(100) = %d, want %d", i+1, got, w)
		}
	}
	c.noteDecodeSuccess()
	if got := c.limitFor(100); got != 100 {
		t.Fatalf("after success limitFor(100) = %d, want 100", got)
	}
}

// Concurrent limit access must not race.
func TestClientLimitConcurrent(t *testing.T) {
	c := NewClient(StaticKey{}, nil, nil)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				_ = c.limitFor(100)
				c.noteDecodeFailure()
				c.noteDecodeSuccess()
			}
		})
	}
	wg.Wait()
	c.noteDecodeSuccess()
	if got := c.limitFor(0); got != defaultSyncLimit {
		t.Fatalf("limitFor(0) = %d, want %d", got, defaultSyncLimit)
	}
}

// An empty page must echo the request cursor (or be empty); any other id
// is a foreign injection and fails closed.
func TestValidateSyncResponseExpectedCursor(t *testing.T) {
	peer := netip.MustParseAddrPort("127.0.0.1:8443")
	want := uuid.New().String()

	if err := validateSyncResponse(peer, SyncResponse{NextCursor: want}, want); err != nil {
		t.Fatalf("echo rejected: %v", err)
	}
	if err := validateSyncResponse(peer, SyncResponse{}, want); err != nil {
		t.Fatalf("empty cursor rejected: %v", err)
	}
	if err := validateSyncResponse(peer, SyncResponse{}, ""); err != nil {
		t.Fatalf("from-start empty rejected: %v", err)
	}
	if err := validateSyncResponse(peer, SyncResponse{NextCursor: uuid.New().String()}, want); err == nil {
		t.Fatal("foreign cursor accepted")
	} else if !strings.Contains(err.Error(), "echo") {
		t.Fatalf("error = %v, want echo complaint", err)
	}
	if err := validateSyncResponse(peer, SyncResponse{NextCursor: uuid.New().String()}, ""); err == nil {
		t.Fatal("foreign cursor accepted for from-start")
	}

	bound := boundSyncResponse()
	if err := validateSyncResponse(peer, bound, want); err != nil {
		t.Fatalf("bound page rejected: %v", err)
	}
	broken := boundSyncResponse()
	broken.NextCursor = uuid.New().String()
	if err := validateSyncResponse(peer, broken, want); err == nil {
		t.Fatal("mismatched cursor accepted with expected present")
	}
}

// Legacy callers without an expected cursor keep the old lenient echo rule.
func TestValidateSyncResponseLegacyEcho(t *testing.T) {
	peer := netip.MustParseAddrPort("127.0.0.1:8443")
	if err := validateSyncResponse(peer, SyncResponse{NextCursor: uuid.New().String()}); err != nil {
		t.Fatalf("legacy echo rejected: %v", err)
	}
}

// A replay (request cursor echoed back on a non-empty page) keeps the
// current cursor with a warn instead of failing or rewinding.
func TestClampRegressiveCursor(t *testing.T) {
	cur := uuid.New().String()
	replay := SyncResponse{Events: []journal.Event{testEvent("r")}, NextCursor: cur}
	out, regressed := clampRegressiveCursor(cur, replay, logs.NewTest(t))
	if !regressed {
		t.Fatal("replay not flagged")
	}
	if out.NextCursor != cur {
		t.Fatalf("next = %q, want kept %q", out.NextCursor, cur)
	}

	e := testEvent("h")
	honest := SyncResponse{Events: []journal.Event{e}, NextCursor: e.ID.String()}
	if _, regressed := clampRegressiveCursor(cur, honest, logs.NewNop()); regressed {
		t.Fatal("honest page flagged as replay")
	}
	echo := SyncResponse{NextCursor: cur}
	if _, regressed := clampRegressiveCursor(cur, echo, logs.NewNop()); regressed {
		t.Fatal("empty echo flagged as replay")
	}
}

// Failure classes are a fixed static set, never peer bytes.
func TestClassifyHandshakeError(t *testing.T) {
	if got := classifyHandshakeError(nil); got != "none" {
		t.Fatalf("nil = %q, want none", got)
	}
	if got := classifyHandshakeError(context.DeadlineExceeded); got != "timeout" {
		t.Fatalf("deadline = %q, want timeout", got)
	}
	if got := classifyHandshakeError(errors.New("read frame length: short")); got != "frame" {
		t.Fatalf("frame = %q", got)
	}
	if got := classifyHandshakeError(errors.New("parcel must hold id")); got != "parcel" {
		t.Fatalf("parcel = %q", got)
	}
	if got := classifyHandshakeError(errors.New("peer claims bad sig")); got != "verify" {
		t.Fatalf("verify = %q", got)
	}
	if got := classifyHandshakeError(errors.New("noise read request: boom")); got != "handshake" {
		t.Fatalf("other = %q", got)
	}
}

// A wrong-ID rejection carries the peer address for operator triage.
func TestSyncWrongIDErrorCarriesPeerAddr(t *testing.T) {
	fx := newHandshakeFixture(t)
	caCert, err := LoadCACert(fx.caFile)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	serverVerify := CAVerifier(caCert)
	clientVerify := CAVerifier(caCert)

	raw := testListen(t)
	listener := &noiseListener{
		Listener: raw,
		static:   fx.staticB,
		parcel:   fx.parcelB,
		verify:   serverVerify,
		logger:   logs.NewNop(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+SyncPath, func(w http.ResponseWriter, r *http.Request) {
		serveSyncPage(w, r, nil, logs.NewNop())
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = srv.Serve(listener) //nolint:errcheck // test server ends with test
	}()
	t.Cleanup(func() {
		_ = srv.Close() //nolint:errcheck // test cleanup
	})

	addr, err := netip.ParseAddrPort(raw.Addr().String())
	if err != nil {
		t.Fatalf("parse addr: %v", err)
	}
	client := NewClient(fx.staticA, fx.parcelA, clientVerify)
	_, err = client.Sync(context.Background(), addr, Peer{ID: uuid.New(), Pub: fx.staticB.Public}, SyncRequest{Limit: 10})
	if err == nil {
		t.Fatal("wrong ID accepted")
	}
	if !strings.Contains(err.Error(), addr.String()) {
		t.Fatalf("error = %v, want peer addr %s", err, addr)
	}
}

// An abandoned session after cancellation closes and reports ctx.Err;
// a live context hands the session back.
func TestAbandonIfCancelled(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	dead := func() *noiseConn {
		_, resp := handshakePair(t)
		return resp
	}()
	if _, err := abandonIfCancelled(cancelled, dead, "peer"); !errors.Is(err, context.Canceled) {
		t.Fatalf("abandon = %v, want context.Canceled", err)
	}
	if _, err := dead.Read(make([]byte, 1)); err == nil {
		t.Fatal("abandoned session still readable")
	}

	_, live := handshakePair(t)
	if _, err := abandonIfCancelled(context.Background(), live, "peer"); err != nil {
		t.Fatalf("live abandon: %v", err)
	}
}

// A frame lost after Encrypt poisons the send direction: the next Write
// fails closed instead of sending undecryptable follow-ups.
func TestNoiseConnPoisonOnWriteFailure(t *testing.T) {
	init, _ := handshakePair(t)

	init.conn = &failWriteConn{Conn: init.conn, writeErr: errors.New("boom")}
	if _, err := init.Write([]byte("hi")); err == nil {
		t.Fatal("failed frame accepted")
	}
	if _, err := init.Write([]byte("hi")); err == nil {
		t.Fatal("poisoned conn still writable")
	} else if !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("error = %v, want poisoned", err)
	}
}

// failWriteConn fails every Write while passing the rest to the embedded conn.
type failWriteConn struct {
	net.Conn
	writeErr error
}

// Write always fails with the configured error.
func (c *failWriteConn) Write([]byte) (int, error) {
	return 0, c.writeErr
}

// Close is idempotent, and Read/Write after Close fail fast.
func TestNoiseConnCloseIdempotent(t *testing.T) {
	init, _ := handshakePair(t)

	if err := init.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := init.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := init.Write([]byte("x")); err == nil {
		t.Fatal("write after close accepted")
	}
	if _, err := init.Read(make([]byte, 1)); err == nil {
		t.Fatal("read after close accepted")
	}
}

// Repeated concurrent Close with in-flight I/O must not race or panic.
func TestNoiseConnCloseConcurrent(t *testing.T) {
	init, _ := handshakePair(t)

	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 50 {
				_ = init.Close() //nolint:errcheck // racing close under test
			}
		})
		wg.Go(func() {
			_, _ = init.Write([]byte("x")) //nolint:errcheck // errors expected during close
		})
	}
	wg.Wait()
}

// writeEmpty encrypts one empty-plaintext frame and writes it. Only tests
// and non-cooperative peers produce empty frames; Write never sends one.
func writeEmpty(init *noiseConn) error {
	empty, err := init.send.Encrypt(nil, nil, []byte{})
	if err != nil {
		return fmt.Errorf("noise encrypt empty: %w", err)
	}
	err = writeFrame(init.conn, empty)
	if err != nil {
		return err
	}
	return nil
}

// dripEmpties writes rounds of perRound empty frames each followed by one
// real byte, reporting the first error. It feeds tests that spread empty
// frames across many Reads.
func dripEmpties(init *noiseConn, rounds, perRound int) chan error {
	done := make(chan error, 1)
	go func() {
		var err error
		defer func() { done <- err }()
		for range rounds {
			for range perRound {
				if err = writeEmpty(init); err != nil {
					return
				}
			}
			if _, err = init.Write([]byte("x")); err != nil {
				return
			}
		}
	}()
	return done
}

// Empty frames dripped across many Reads still trip the cumulative cap;
// honest peers send none, so the cap never fires on good traffic.
func TestNoiseConnEmptyFramesCumulativeAcrossReads(t *testing.T) {
	init, resp := handshakePair(t)
	done := dripEmpties(init, 200, 6)

	reads := 0
	for {
		_, rerr := resp.Read(make([]byte, 1))
		if rerr != nil {
			if !strings.Contains(rerr.Error(), "empty frames") {
				t.Fatalf("error = %v, want cumulative empty frame cap", rerr)
			}
			break
		}
		reads++
		if reads > 500 {
			t.Fatal("cumulative cap never tripped over 1200 drip-fed empties")
		}
	}
	// The reader stopped, so unblock the writer before collecting it.
	// Close is idempotent, so the deferred cleanup stays harmless.
	if err := resp.Close(); err != nil {
		t.Fatalf("close after cap: %v", err)
	}
	<-done
}

// A few hundred empties spread over many Reads stay under the cap.
func TestNoiseConnEmptyFramesUnderCapAcrossReads(t *testing.T) {
	init, resp := handshakePair(t)
	done := dripEmpties(init, 50, 6)

	buf := make([]byte, 1)
	for range 50 {
		if _, rerr := io.ReadFull(resp, buf); rerr != nil {
			t.Fatalf("read under cap: %v", rerr)
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("writer: %v", err)
	}
}
