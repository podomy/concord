// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
)

// roundtripReader is a deterministic byte source for reproducible keys
// and payloads. Each Read increments from n.
type roundtripReader struct {
	n byte
}

// Read fills p with incrementing bytes starting after n.
func (r *roundtripReader) Read(p []byte) (int, error) {
	for i := range p {
		r.n++
		p[i] = r.n
	}
	return len(p), nil
}

// roundtripStatics returns deterministic initiator and responder keys.
func roundtripStatics(t *testing.T) (StaticKey, StaticKey) {
	t.Helper()
	initKey, err := GenerateStaticKey(&roundtripReader{n: 0})
	if err != nil {
		t.Fatalf("GenerateStaticKey initiator: %v", err)
	}
	respKey, err := GenerateStaticKey(&roundtripReader{n: 0xa0})
	if err != nil {
		t.Fatalf("GenerateStaticKey responder: %v", err)
	}
	return initKey, respKey
}

// roundtripPattern returns n deterministic payload bytes tagged by seed.
func roundtripPattern(n int, seed byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = seed + byte(i)
	}
	return out
}

// roundtripHandshake runs a full IK handshake and returns the paired
// transports: initiator send/recv and responder send/recv.
func roundtripHandshake(t *testing.T, initKey, respKey StaticKey, prologue, initPayload, respPayload []byte) (initSend, initRecv, respSend, respRecv *Transport) {
	t.Helper()
	initiator, err := NewIKInitiator(prologue, initKey, respKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator: %v", err)
	}
	req, err := initiator.WriteRequest(initPayload)
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	if len(req) != 96+len(initPayload) {
		t.Fatalf("request len %d, want %d", len(req), 96+len(initPayload))
	}
	responder, err := NewIKResponder(prologue, respKey)
	if err != nil {
		t.Fatalf("NewIKResponder: %v", err)
	}
	gotInit, peerStatic, err := responder.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	if !bytes.Equal(gotInit, initPayload) {
		t.Fatalf("initiator payload len %d mismatch", len(gotInit))
	}
	if !bytes.Equal(peerStatic[:], initKey.Public[:]) {
		t.Fatalf("peer static mismatch")
	}
	respMsg, send, recv, err := responder.WriteResponse(respPayload)
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	if len(respMsg) != 48+len(respPayload) {
		t.Fatalf("response len %d, want %d", len(respMsg), 48+len(respPayload))
	}
	gotResp, isend, irecv, err := initiator.ReadResponse(respMsg)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if !bytes.Equal(gotResp, respPayload) {
		t.Fatalf("responder payload len %d mismatch", len(gotResp))
	}
	return isend, irecv, send, recv
}

// roundtripCase runs one handshake plus transport pair for a table entry.
func roundtripCase(t *testing.T, prologue []byte, size int, name string) {
	t.Helper()
	initKey, respKey := roundtripStatics(t)
	initPayload := roundtripPattern(size, 0x11)
	respPayload := roundtripPattern(size, 0x77)
	initSend, initRecv, respSend, respRecv := roundtripHandshake(t, initKey, respKey, prologue, initPayload, respPayload)
	ad := []byte("ad-" + name)
	ct := mustEncrypt(t, initSend, ad, initPayload, "Encrypt initiator")
	if len(ct) != len(initPayload)+16 {
		t.Fatalf("ciphertext len %d, want %d", len(ct), len(initPayload)+16)
	}
	pt := mustDecrypt(t, respRecv, ad, ct, "Decrypt responder")
	if !bytes.Equal(pt, initPayload) {
		t.Fatalf("forward payload mismatch at size %d", size)
	}
	ct = mustEncrypt(t, respSend, ad, respPayload, "Encrypt responder")
	pt = mustDecrypt(t, initRecv, ad, ct, "Decrypt initiator")
	if !bytes.Equal(pt, respPayload) {
		t.Fatalf("reverse payload mismatch at size %d", size)
	}
}

// roundtripExchange checks one forward and reverse message pair.
func roundtripExchange(t *testing.T, initSend, initRecv, respSend, respRecv *Transport, i int) {
	t.Helper()
	ad := roundtripPattern(8, byte(i))    //nolint:gosec // test loop index bounded to 0-9
	want := roundtripPattern(64, byte(i)) //nolint:gosec // test loop index bounded to 0-9
	ct := mustEncrypt(t, initSend, ad, want, fmt.Sprintf("Encrypt forward %d", i))
	got := mustDecrypt(t, respRecv, ad, ct, fmt.Sprintf("Decrypt forward %d", i))
	if !bytes.Equal(got, want) {
		t.Fatalf("forward message %d mismatch", i)
	}
	wantBack := roundtripPattern(64, byte(0x80+i)) //nolint:gosec // test loop index bounded to 0-9
	ct = mustEncrypt(t, respSend, ad, wantBack, fmt.Sprintf("Encrypt reverse %d", i))
	gotBack := mustDecrypt(t, initRecv, ad, ct, fmt.Sprintf("Decrypt reverse %d", i))
	if !bytes.Equal(gotBack, wantBack) {
		t.Fatalf("reverse message %d mismatch", i)
	}
}

// roundtripRekeyAll rekeys all four transports.
func roundtripRekeyAll(t *testing.T, initSend, initRecv, respSend, respRecv *Transport) {
	t.Helper()
	mustRekey(t, initSend, "Rekey initSend")
	mustRekey(t, respRecv, "Rekey respRecv")
	mustRekey(t, respSend, "Rekey respSend")
	mustRekey(t, initRecv, "Rekey initRecv")
}

// roundtripPostRekey checks one post-rekey pair in both directions.
func roundtripPostRekey(t *testing.T, initSend, initRecv, respSend, respRecv *Transport, i int) {
	t.Helper()
	ad := roundtripPattern(8, byte(0xf0+i))    //nolint:gosec // test loop index bounded to 0-1
	want := roundtripPattern(32, byte(0x20+i)) //nolint:gosec // test loop index bounded to 0-1
	ct := mustEncrypt(t, initSend, ad, want, fmt.Sprintf("Encrypt post-rekey forward %d", i))
	got := mustDecrypt(t, respRecv, ad, ct, fmt.Sprintf("Decrypt post-rekey forward %d", i))
	if !bytes.Equal(got, want) {
		t.Fatalf("post-rekey forward %d mismatch", i)
	}
	ct = mustEncrypt(t, respSend, ad, want, fmt.Sprintf("Encrypt post-rekey reverse %d", i))
	got = mustDecrypt(t, initRecv, ad, ct, fmt.Sprintf("Decrypt post-rekey reverse %d", i))
	if !bytes.Equal(got, want) {
		t.Fatalf("post-rekey reverse %d mismatch", i)
	}
}

// TestIKRoundTripHandshakePayloads checks handshake and transport delivery
// for a table of payload sizes including MaxMsgLen.
func TestIKRoundTripHandshakePayloads(t *testing.T) {
	prologue := []byte("concord-noise-transport-v1")
	cases := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"one", 1},
		{"hundred", 100},
		{"thousand", 1000},
		{"max", MaxMsgLen},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			roundtripCase(t, prologue, c.size, c.name)
		})
	}
}

// TestIKRoundTripTransportMulti sends 10 messages each way with distinct
// ad, rekeys all transports, and checks traffic still flows in sync.
func TestIKRoundTripTransportMulti(t *testing.T) {
	prologue := []byte("concord-noise-transport-v1")
	initKey, respKey := roundtripStatics(t)
	initSend, initRecv, respSend, respRecv := roundtripHandshake(t, initKey, respKey, prologue, []byte("hi"), []byte("yo"))
	for i := range 10 {
		roundtripExchange(t, initSend, initRecv, respSend, respRecv, i)
	}
	roundtripRekeyAll(t, initSend, initRecv, respSend, respRecv)
	for i := range 2 {
		roundtripPostRekey(t, initSend, initRecv, respSend, respRecv, i)
	}
}

// TestIKRoundTripLiveRand checks one handshake and one message each way
// with fresh crypto/rand keys to prove liveness outside fixtures.
func TestIKRoundTripLiveRand(t *testing.T) {
	prologue := []byte("concord-noise-transport-v1")
	initKey, err := GenerateStaticKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateStaticKey initiator: %v", err)
	}
	respKey, err := GenerateStaticKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateStaticKey responder: %v", err)
	}
	initSend, initRecv, respSend, respRecv := roundtripHandshake(t, initKey, respKey, prologue, []byte("live-hi"), []byte("live-yo"))
	ct, err := initSend.Encrypt(nil, []byte("live-ad"), []byte("live-ping"))
	if err != nil {
		t.Fatalf("Encrypt live forward: %v", err)
	}
	pt, err := respRecv.Decrypt(nil, []byte("live-ad"), ct)
	if err != nil {
		t.Fatalf("Decrypt live forward: %v", err)
	}
	if !bytes.Equal(pt, []byte("live-ping")) {
		t.Fatalf("live forward %q mismatch", pt)
	}
	ct, err = respSend.Encrypt(nil, []byte("live-ad"), []byte("live-pong"))
	if err != nil {
		t.Fatalf("Encrypt live reverse: %v", err)
	}
	pt, err = initRecv.Decrypt(nil, []byte("live-ad"), ct)
	if err != nil {
		t.Fatalf("Decrypt live reverse: %v", err)
	}
	if !bytes.Equal(pt, []byte("live-pong")) {
		t.Fatalf("live reverse %q mismatch", pt)
	}
}
