// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: BUSL-1.1

package transport

import (
	"bytes"
	"crypto/ecdh"
	"crypto/rand"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/certs"
)

// handshakeFixture holds two nodes with CA-signed generation-0 parcels.
type handshakeFixture struct {
	idA     uuid.UUID
	staticA StaticKey
	parcelA []byte
	idB     uuid.UUID
	staticB StaticKey
	parcelB []byte
	caFile  string
}

// newHandshakeFixture creates a CA plus two signed identities under a temp
// XDG dir, using only the exported certs API and this package's loader.
func newHandshakeFixture(t *testing.T) handshakeFixture {
	t.Helper()

	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	err := certs.WriteCA()
	if err != nil {
		t.Fatalf("write CA: %v", err)
	}

	mkKey := func() StaticKey {
		t.Helper()
		key, err := ecdh.X25519().GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return StaticKey{Private: key.Bytes(), Public: key.PublicKey().Bytes()}
	}
	staticA, staticB := mkKey(), mkKey()
	idA, idB := uuid.New(), uuid.New()

	sigA, err := certs.SignNodeKey(idA, 0, staticA.Public)
	if err != nil {
		t.Fatalf("sign A: %v", err)
	}
	sigB, err := certs.SignNodeKey(idB, 0, staticB.Public)
	if err != nil {
		t.Fatalf("sign B: %v", err)
	}

	return handshakeFixture{
		idA:     idA,
		staticA: staticA,
		parcelA: EncodeParcel(idA, 0, sigA),
		idB:     idB,
		staticB: staticB,
		parcelB: EncodeParcel(idB, 0, sigB),
		caFile:  filepath.Join(dir, "concord", "certs", "ca.crt"),
	}
}

// testPipe returns a connected pair with a deadline so a stuck handshake
// fails the test instead of hanging it.
func testPipe(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()

	c1, c2 := net.Pipe()
	deadline := time.Now().Add(10 * time.Second)
	if err := c1.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := c2.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c1.Close() }) //nolint:errcheck // best-effort test cleanup
	t.Cleanup(func() { _ = c2.Close() }) //nolint:errcheck // best-effort test cleanup

	return c1, c2
}

// testVerifier builds the real CA verifier for the fixture.
func testVerifier(t *testing.T, fx handshakeFixture) Verifier {
	t.Helper()

	caCert, err := LoadCACert(fx.caFile)
	if err != nil {
		t.Fatalf("load ca: %v", err)
	}

	return CAVerifier(caCert)
}

// TestIKHandshakeRoundTrip runs a full IK handshake over net.Pipe and proves
// both directions carry data, including a multi-frame message exercising the
// chunking path.
func TestIKHandshakeRoundTrip(t *testing.T) {
	fx := newHandshakeFixture(t)
	verify := testVerifier(t, fx)

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

	resp, err := serveHandshake(c2, fx.staticB, fx.parcelB, verify)
	if err != nil {
		t.Fatalf("responder: %v", err)
	}
	defer func() { _ = resp.Close() }() //nolint:errcheck // best-effort test cleanup

	init := <-initCh
	if init.err != nil {
		t.Fatalf("initiator: %v", init.err)
	}
	defer func() { _ = init.conn.Close() }() //nolint:errcheck // best-effort test cleanup

	// The responder parcel arrives as the reply payload: right node, right
	// generation, and it must verify.
	checkResponderParcel(t, verify, fx, init.parcel)

	// Small messages in both directions prove the mirrored CipherStates.
	checkEcho(t, init.conn, resp)

	// 200 KiB exceeds one Noise message: exercises chunking and reassembly.
	checkBulk(t, init.conn, resp)
}

// checkResponderParcel decodes the responder's in-session parcel and checks
// node ID, generation, and CA signature.
func checkResponderParcel(t *testing.T, verify Verifier, fx handshakeFixture, raw []byte) {
	t.Helper()

	id, gen, sig, err := decodeParcel(raw)
	if err != nil {
		t.Fatalf("decode responder parcel: %v", err)
	}
	if id != fx.idB {
		t.Fatalf("parcel id = %s, want %s", id, fx.idB)
	}
	if gen != 0 {
		t.Fatalf("parcel generation = %d, want 0", gen)
	}
	if err := verify(id, gen, fx.staticB.Public, sig); err != nil {
		t.Fatalf("responder parcel rejected: %v", err)
	}
}

// checkEcho exchanges small messages in both directions over an established
// session. Reads run in goroutines: net.Pipe writes block until read.
func checkEcho(t *testing.T, init, resp *noiseConn) {
	t.Helper()

	got := make([]byte, 5)
	readCh := make(chan error, 1)
	go func() {
		_, err := io.ReadFull(resp, got)
		readCh <- err
	}()
	if _, err := init.Write([]byte("hello")); err != nil {
		t.Fatalf("initiator write: %v", err)
	}
	if err := <-readCh; err != nil {
		t.Fatalf("responder read: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("responder got %q, want hello", got)
	}

	back := make([]byte, 5)
	backCh := make(chan error, 1)
	go func() {
		_, err := resp.Write([]byte("world"))
		backCh <- err
	}()
	if _, err := io.ReadFull(init, back); err != nil {
		t.Fatalf("initiator read: %v", err)
	}
	if err := <-backCh; err != nil {
		t.Fatalf("responder write: %v", err)
	}
	if string(back) != "world" {
		t.Fatalf("initiator got %q, want world", back)
	}
}

// checkBulk moves 200 KiB through the session and checks byte equality.
func checkBulk(t *testing.T, init, resp *noiseConn) {
	t.Helper()

	big := bytes.Repeat([]byte{0xab}, 200<<10)
	writeCh := make(chan error, 1)
	go func() {
		_, err := init.Write(big)
		writeCh <- err
	}()
	received := make([]byte, 0, len(big))
	buf := make([]byte, 32<<10)
	for len(received) < len(big) {
		n, err := resp.Read(buf)
		if err != nil {
			t.Errorf("responder big read: %v", err)
			return
		}
		received = append(received, buf[:n]...)
	}
	if err := <-writeCh; err != nil {
		t.Fatalf("initiator big write: %v", err)
	}
	if !bytes.Equal(received, big) {
		t.Fatalf("big message corrupted: got %d bytes", len(received))
	}
}

// TestIKHandshakeWrongPeerKey fails when the initiator dials with a static key
// the responder does not hold.
func TestIKHandshakeWrongPeerKey(t *testing.T) {
	fx := newHandshakeFixture(t)
	verify := testVerifier(t, fx)

	stranger, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	c1, c2 := testPipe(t)

	initCh := make(chan error, 1)
	go func() {
		_, _, err := initiatorHandshake(c1, fx.staticA, fx.parcelA, stranger.PublicKey().Bytes())
		initCh <- err
	}()

	_, serveErr := serveHandshake(c2, fx.staticB, fx.parcelB, verify)
	initErr := <-initCh
	if initErr == nil && serveErr == nil {
		t.Fatal("handshake with wrong peer key succeeded")
	}
}

// TestServeHandshakeRejectsMismatchedKey fails when the initiator presents a
// static key different from the one its CA signature was issued for: a valid
// signature over the wrong key.
func TestServeHandshakeRejectsMismatchedKey(t *testing.T) {
	fx := newHandshakeFixture(t)
	verify := testVerifier(t, fx)

	// Parcel claimed for A but signed for nobody: signature over B's key.
	bad, err := certs.SignNodeKey(fx.idA, 0, fx.staticB.Public)
	if err != nil {
		t.Fatal(err)
	}
	rogueParcel := EncodeParcel(fx.idA, 0, bad)

	c1, c2 := testPipe(t)

	go func() {
		// Result under test is the responder's; the initiator only needs to
		// speak first. Its outcome is asserted through serveHandshake below.
		_, _, _ = initiatorHandshake(c1, fx.staticA, rogueParcel, fx.staticB.Public) //nolint:errcheck // see above
	}()

	_, err = serveHandshake(c2, fx.staticB, fx.parcelB, verify)
	if err == nil {
		t.Fatal("mismatched parcel accepted")
	}
}

// TestServeHandshakeRejectsBadSignature fails when the initiator parcel
// carries a tampered CA signature.
func TestServeHandshakeRejectsBadSignature(t *testing.T) {
	fx := newHandshakeFixture(t)
	verify := testVerifier(t, fx)

	id, gen, sig, err := decodeParcel(fx.parcelA)
	if err != nil {
		t.Fatal(err)
	}
	sig[0] ^= 0x01
	tampered := EncodeParcel(id, gen, sig)

	c1, c2 := testPipe(t)

	go func() {
		// Result under test is the responder's; see above.
		_, _, _ = initiatorHandshake(c1, fx.staticA, tampered, fx.staticB.Public) //nolint:errcheck // see above
	}()

	_, err = serveHandshake(c2, fx.staticB, fx.parcelB, verify)
	if err == nil {
		t.Fatal("tampered signature accepted")
	}
}

// TestDecodeParcelRejectsShortInput guards the parser against truncated
// parcels.
func TestDecodeParcelRejectsShortInput(t *testing.T) {
	t.Parallel()

	_, _, _, err := decodeParcel([]byte("short"))
	if err == nil {
		t.Fatal("short parcel accepted")
	}
}
