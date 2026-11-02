// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/certs"
	"github.com/podomy/concord/internal/logs"
)

// testListen opens a loopback TCP listener with ListenConfig (noctx-safe).
func testListen(t *testing.T) net.Listener {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return ln
}

// Empty plaintext frames must be skipped, never surfacing as (0, nil).
func TestNoiseConnEmptyPlaintextSkipped(t *testing.T) {
	fx := newHandshakeFixture(t)
	c1, c2 := testPipe(t)

	type result struct {
		conn   *noiseConn
		parcel []byte
		err    error
	}
	initCh := make(chan result, 1)
	go func() {
		conn, parcel, err := initiatorHandshake(c1, fx.staticA, fx.parcelA, fx.staticB.Public)
		initCh <- result{conn: conn, parcel: parcel, err: err}
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

	// Writer injects one empty frame then a real message. net.Pipe writes
	// block until read, so the writer runs async.
	writeCh := make(chan error, 1)
	go func() {
		empty, encErr := init.send.Encrypt(nil, nil, []byte{})
		if encErr != nil {
			writeCh <- encErr
			return
		}
		if werr := writeFrame(init.conn, empty); werr != nil {
			writeCh <- werr
			return
		}
		_, werr := init.Write([]byte("hello"))
		writeCh <- werr
	}()

	got := make([]byte, 5)
	if _, rerr := io.ReadFull(resp, got); rerr != nil {
		t.Fatalf("read after empty frame: %v", rerr)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q, want hello (empty frame must be skipped)", got)
	}
	if werr := <-writeCh; werr != nil {
		t.Fatalf("writer: %v", werr)
	}

	// Zero-length reads keep the (0, nil) contract without touching the wire.
	if n, rerr := resp.Read([]byte{}); n != 0 || rerr != nil {
		t.Fatalf("zero-len Read = (%d, %v), want (0, nil)", n, rerr)
	}
}

// Zero-length Write sends nothing and reports (0, nil).
func TestNoiseConnZeroLenWrite(t *testing.T) {
	fx := newHandshakeFixture(t)
	c1, c2 := testPipe(t)

	initCh := make(chan error, 1)
	var init *noiseConn
	go func() {
		var parcel []byte
		var err error
		init, parcel, err = initiatorHandshake(c1, fx.staticA, fx.parcelA, fx.staticB.Public)
		_ = parcel
		initCh <- err
	}()
	resp, err := serveHandshake(c2, fx.staticB, fx.parcelB, testVerifier(t, fx))
	if err != nil {
		t.Fatalf("responder: %v", err)
	}
	t.Cleanup(func() { _ = resp.Close() }) //nolint:errcheck // test cleanup
	if err := <-initCh; err != nil {
		t.Fatalf("initiator: %v", err)
	}
	t.Cleanup(func() { _ = init.Close() }) //nolint:errcheck // test cleanup

	if n, err := init.Write(nil); n != 0 || err != nil {
		t.Fatalf("zero-len Write = (%d, %v), want (0, nil)", n, err)
	}
}

// drainLoop reads until stop closes. Read errors during close are expected.
func drainLoop(stop <-chan struct{}, setDeadline func(time.Time) error, read func([]byte) (int, error)) {
	buf := make([]byte, 64)
	for {
		select {
		case <-stop:
			return
		default:
		}
		_ = setDeadline(time.Now().Add(20 * time.Millisecond)) //nolint:errcheck // test deadline
		_, _ = read(buf)                                       //nolint:errcheck // errors expected during close
	}
}

// writeLoop pushes single bytes until stop closes. Errors during close are expected.
func writeLoop(stop <-chan struct{}, write func([]byte) (int, error)) {
	for {
		select {
		case <-stop:
			return
		default:
		}
		_, _ = write([]byte("x")) //nolint:errcheck // errors expected during close
	}
}

// Concurrent Read, Write, and Close must not race (run with -race).
func TestNoiseConnConcurrentRace(t *testing.T) {
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
	init := <-initCh
	if init == nil || resp == nil {
		t.Fatal("handshake failed")
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	// Readers drain; writers push; closer races both.
	for range 2 {
		wg.Go(func() {
			drainLoop(stop, init.SetReadDeadline, resp.Read)
		})
	}
	wg.Go(func() {
		writeLoop(stop, init.Write)
	})
	time.Sleep(100 * time.Millisecond)
	_ = init.Close() //nolint:errcheck // racing close under test
	_ = resp.Close() //nolint:errcheck // racing close under test
	close(stop)
	wg.Wait()
}

// Client.Sync over a real noiseListener rejects a wrong node ID via the
// client verify path (server handshake still succeeds).
func TestClientSyncWrongIDRejected(t *testing.T) {
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
		postSync(w, r, nil)
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
	expect := Peer{ID: uuid.New(), Pub: fx.staticB.Public}
	_, err = client.Sync(context.Background(), addr, expect, SyncRequest{Limit: 10})
	if err == nil {
		t.Fatal("wrong ID accepted")
	}
	if !strings.Contains(err.Error(), "peer claims") {
		t.Fatalf("error = %v, want peer claims mismatch", err)
	}
}

// Client.Sync over a real noiseListener rejects a tampered CA signature via
// the client verify path.
func TestClientSyncBadSigRejected(t *testing.T) {
	fx := newHandshakeFixture(t)
	caCert, err := LoadCACert(fx.caFile)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	serverVerify := CAVerifier(caCert)
	clientVerify := CAVerifier(caCert)

	id, gen, sig, err := decodeParcel(fx.parcelB)
	if err != nil {
		t.Fatalf("decode parcel: %v", err)
	}
	sig[0] ^= 0x01
	tampered := EncodeParcel(id, gen, sig)

	raw := testListen(t)
	listener := &noiseListener{
		Listener: raw,
		static:   fx.staticB,
		parcel:   tampered,
		verify:   serverVerify,
		logger:   logs.NewNop(),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+SyncPath, func(w http.ResponseWriter, r *http.Request) {
		postSync(w, r, nil)
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
	expect := Peer{ID: fx.idB, Pub: fx.staticB.Public}
	_, err = client.Sync(context.Background(), addr, expect, SyncRequest{Limit: 10})
	if err == nil {
		t.Fatal("tampered signature accepted")
	}
}

// postSync rejects limit above the max instead of serving an unbounded page.
func TestPostSyncLimitTooLarge(t *testing.T) {
	body := strings.NewReader(`{"cursor":"","limit":2000}`)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, body)
	rec := httptest.NewRecorder()
	postSync(rec, req, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("limit 2000 status = %d, want 400", rec.Code)
	}
}

// postSync rejects malformed JSON.
func TestPostSyncMalformedJSON(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, strings.NewReader(`not json`))
	rec := httptest.NewRecorder()
	postSync(rec, req, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed status = %d, want 400", rec.Code)
	}
}

// postSync rejects trailing bytes after the first JSON value.
func TestPostSyncTrailingBytes(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, strings.NewReader(`{"cursor":"","limit":10} trailing`))
	rec := httptest.NewRecorder()
	postSync(rec, req, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("trailing status = %d, want 400", rec.Code)
	}
}

// postSync rejects unknown fields via DisallowUnknownFields.
func TestPostSyncUnknownFields(t *testing.T) {
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, strings.NewReader(`{"cursor":"","limit":10,"bogus":1}`))
	rec := httptest.NewRecorder()
	postSync(rec, req, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d, want 400", rec.Code)
	}
}

// postSync enforces the 1 MiB body cap.
func TestPostSyncBodyTooLarge(t *testing.T) {
	big := strings.Repeat("a", 2<<20)
	body := strings.NewReader(`{"cursor":"` + big + `","limit":10}`)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, body)
	rec := httptest.NewRecorder()
	postSync(rec, req, nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("oversize status = %d, want 400", rec.Code)
	}
}

// postSync normalizes a non-UUID cursor to from-start with 200.
func TestPostSyncNonUUIDCursorServesFromStart(t *testing.T) {
	dir := t.TempDir()
	e1, e2 := testEvent("a"), testEvent("b")
	writeEnvJournal(t, dir, e1, e2)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, SyncPath, strings.NewReader(`{"cursor":"not-a-uuid","limit":10}`))
	rec := httptest.NewRecorder()
	postSync(rec, req, nil)
	if rec.Code != http.StatusOK {
		t.Fatalf("non-uuid cursor status = %d, want 200", rec.Code)
	}
	var resp SyncResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Events) != 2 || resp.Events[0].ID != e1.ID {
		t.Fatalf("non-uuid cursor did not serve from start: %+v", resp)
	}
}

// loadSyncPage serves a non-UUID cursor from the start in one pass.
func TestLoadSyncPageNonUUIDSinglePass(t *testing.T) {
	dir := t.TempDir()
	e1 := testEvent("x1")
	e2 := testEvent("x2")
	writeEnvJournal(t, dir, e1, e2)

	got, _, err := loadSyncPage(context.Background(), nil, "garbage-cursor", 10)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(got) != 2 || got[0].ID != e1.ID || got[1].ID != e2.ID {
		t.Fatalf("non-uuid cursor got %d events, want from-start pair", len(got))
	}
}

// Sync response validation fails closed on oversized pages.
func TestValidateSyncResponseTooMany(t *testing.T) {
	peer := netip.MustParseAddrPort("127.0.0.1:8443")
	resp := SyncResponse{NextCursor: uuid.New().String()}
	for range 2*maxSyncLimit + 1 {
		resp.Events = append(resp.Events, testEvent("e"))
	}
	if err := validateSyncResponse(peer, resp); err == nil {
		t.Fatal("oversized page accepted")
	}
}

// Sync response validation rejects a malformed NextCursor.
func TestValidateSyncResponseBadCursor(t *testing.T) {
	peer := netip.MustParseAddrPort("127.0.0.1:8443")
	r2 := SyncResponse{NextCursor: "not-a-uuid"}
	r2.Events = append(r2.Events, testEvent("e"))
	if err := validateSyncResponse(peer, r2); err == nil {
		t.Fatal("bad next cursor accepted")
	}
}

// Sync response validation rejects events with nil IDs.
func TestValidateSyncResponseNilID(t *testing.T) {
	peer := netip.MustParseAddrPort("127.0.0.1:8443")
	r2 := SyncResponse{}
	bad := testEvent("e")
	bad.ID = uuid.Nil
	r2.Events = append(r2.Events, bad)
	if err := validateSyncResponse(peer, r2); err == nil {
		t.Fatal("nil event id accepted")
	}
}

// handshakeError preserves the cause for errors.Is and errors.As.
func TestHandshakeErrorUnwrap(t *testing.T) {
	sentinel := errors.New("boom")
	wrapped := &handshakeError{err: sentinel}
	if !errors.Is(wrapped, sentinel) {
		t.Fatal("errors.Is does not reach wrapped cause")
	}
	target, ok := errors.AsType[*handshakeError](wrapped)
	if !ok || target == nil {
		t.Fatal("errors.As does not find handshakeError")
	}
	if !wrapped.Temporary() {
		t.Fatal("Temporary must stay true for Accept backoff")
	}
	if wrapped.Timeout() {
		t.Fatal("Timeout must stay false")
	}
}

// CAVerifier with a nil certificate fails closed.
func TestCAVerifierNil(t *testing.T) {
	verify := CAVerifier(nil)
	err := verify(uuid.New(), 0, bytes.Repeat([]byte{1}, 32), []byte("sig"))
	if err == nil {
		t.Fatal("nil CA accepted")
	}
}

// CAVerifier with a non-RSA CA key fails closed.
func TestCAVerifierNonRSA(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "non-rsa"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}
	verify := CAVerifier(parsed)
	err = verify(uuid.New(), 0, bytes.Repeat([]byte{1}, 32), []byte("sig"))
	if err == nil {
		t.Fatal("non-RSA CA accepted")
	}
	if !strings.Contains(err.Error(), "rsa") && !strings.Contains(err.Error(), "PublicKey") {
		t.Fatalf("error = %v, want RSA type complaint", err)
	}
}

// CAVerifier ignores certificate dates: an expired cert carrying the fleet
// RSA key still admits a valid parcel. Expiry enforcement, if ever wanted,
// lives above this layer.
func TestCAVerifierExpiredStillAdmits(t *testing.T) {
	fx := newHandshakeFixture(t)
	caCert, err := LoadCACert(fx.caFile)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	expired := &x509.Certificate{
		PublicKey: caCert.PublicKey,
		NotBefore: time.Now().Add(-2 * time.Hour),
		NotAfter:  time.Now().Add(-time.Hour),
	}
	verify := CAVerifier(expired)
	id, gen, sig, err := decodeParcel(fx.parcelA)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if err := verify(id, gen, fx.staticA.Public, sig); err != nil {
		t.Fatalf("expired CA with fleet key rejected (dates must be ignored): %v", err)
	}
}

// dial with an already-cancelled context fails fast with context cause.
func TestDialCtxCancelled(t *testing.T) {
	fx := newHandshakeFixture(t)
	caCert, err := LoadCACert(fx.caFile)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	client := NewClient(fx.staticA, fx.parcelA, CAVerifier(caCert))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	peer := netip.MustParseAddrPort("127.0.0.1:1")
	_, _, err = client.dial(ctx, peer, Peer{ID: fx.idB, Pub: fx.staticB.Public})
	if err == nil {
		t.Fatal("cancelled dial accepted")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

// dial aborts a stalled handshake when the caller cancels.
func TestDialCtxCancelDuringHandshake(t *testing.T) {
	fx := newHandshakeFixture(t)
	ln := testListen(t)
	defer func() { _ = ln.Close() }() //nolint:errcheck // test cleanup
	// Stall: accept and hold without completing the handshake reply.
	go func() {
		for {
			c, aerr := ln.Accept()
			if aerr != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()       //nolint:errcheck // stall holder
				_, _ = io.ReadFull(conn, make([]byte, 4)) //nolint:errcheck // consume length, then stall
				time.Sleep(10 * time.Second)
			}(c)
		}
	}()

	caCert, err := LoadCACert(fx.caFile)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	client := NewClient(fx.staticA, fx.parcelA, CAVerifier(caCert))
	client.timeout = 10 * time.Second
	addr, err := netip.ParseAddrPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("parse addr: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, _, derr := client.dial(ctx, addr, Peer{ID: fx.idB, Pub: fx.staticB.Public})
		done <- derr
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case derr := <-done:
		if derr == nil {
			t.Fatal("cancelled handshake accepted")
		}
		if !errors.Is(derr, context.Canceled) {
			t.Fatalf("error = %v, want context.Canceled", derr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dial did not abort on cancel (waited 5s, handshake timeout is 10s)")
	}
}

// Transport admits generation rollback: an older generation with a valid
// signature still verifies. Stale rejection lives in runtime.KeyPinner,
// which compares against the pinned generation (see
// internal/runtime/pinning.go Verify: generation < pinned is rejected).
// This test documents what the transport layer DOES, not what pinning adds.
func TestGenerationRollbackAdmitted(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	paths, err := certs.DefaultPaths()
	if err != nil {
		t.Fatalf("paths: %v", err)
	}
	caCert, err := LoadCACert(paths.CA)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}
	verify := CAVerifier(caCert)

	id := uuid.New()
	pub := bytes.Repeat([]byte{7}, 32)
	sig0, err := certs.SignNodeKey(id, 0, pub)
	if err != nil {
		t.Fatalf("sign gen0: %v", err)
	}
	sig1, err := certs.SignNodeKey(id, 1, pub)
	if err != nil {
		t.Fatalf("sign gen1: %v", err)
	}
	if err := verify(id, 1, pub, sig1); err != nil {
		t.Fatalf("gen1 rejected: %v", err)
	}
	// Rollback: gen0 still verifies at the transport layer. Pinning above
	// must reject it; transport alone admits.
	if err := verify(id, 0, pub, sig0); err != nil {
		t.Fatalf("rollback gen0 rejected at transport (pinning seam owns this): %v", err)
	}
}
