// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// Vector fixtures for Noise_IK_25519_ChaChaPoly_SHA256.
//
// Full CacNoise vectors.txt is not vendored here, so this file pins the
// minimal deterministic fixtures directly: fixed static and sample
// ephemeral key material plus the framing sizes observed from the
// implementation. Static pairs were generated once from an incrementing
// reader and their public halves are pinned to catch X25519 regressions.
// Handshake ephemerals are fresh random per WriteRequest, so the tests
// pin structure (96 byte request overhead, 48 byte response overhead)
// and self-consistency instead of exact message bytes.

// vectorPrologue is the mesh caller prologue mixed after InitializeSymmetric.
const vectorPrologue = "concord-noise-transport-v1"

const (
	vectorInitPrivHex = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	vectorInitPubHex  = "07a37cbc142093c8b755dc1b10e86cb426374ad16aa853ed0bdfc0b2b86d1c7c"
	vectorRespPrivHex = "a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0"
	vectorRespPubHex  = "ad438bfae31f6c093d61d4339255ea798092c9fadd07b97827f4b0ae9dee7c1c"
	vectorEphPrivHex  = "4142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f60"
	vectorEphPubHex   = "64b101b1d0be5a8704bd078f9895001fc03e8e9f9522f188dd128d9846d48466"
)

// vectorStaticKey builds a StaticKey from a pinned private hex string and
// checks the derived public half against the pinned public hex string.
func vectorStaticKey(t *testing.T, privHex, pubHex string) StaticKey {
	t.Helper()
	priv, err := hex.DecodeString(privHex)
	if err != nil {
		t.Fatalf("decode private hex: %v", err)
	}
	wantPub, err := hex.DecodeString(pubHex)
	if err != nil {
		t.Fatalf("decode public hex: %v", err)
	}
	k, err := GenerateStaticKey(bytes.NewReader(priv))
	if err != nil {
		t.Fatalf("GenerateStaticKey: %v", err)
	}
	if !bytes.Equal(k.Public[:], wantPub) {
		t.Fatalf("public mismatch: got %x want %x", k.Public, wantPub)
	}
	return k
}

// vectorStatics returns the pinned initiator and responder static keys.
func vectorStatics(t *testing.T) (StaticKey, StaticKey) {
	t.Helper()
	initKey := vectorStaticKey(t, vectorInitPrivHex, vectorInitPubHex)
	respKey := vectorStaticKey(t, vectorRespPrivHex, vectorRespPubHex)
	return initKey, respKey
}

// TestIKVectorsStaticDerivation pins X25519 derivation for both statics.
func TestIKVectorsStaticDerivation(t *testing.T) {
	_, _ = vectorStatics(t)
}

// TestIKVectorsEphemeralDerivation pins X25519 derivation for one sample
// ephemeral key in the CacNoise style.
func TestIKVectorsEphemeralDerivation(t *testing.T) {
	_ = vectorStaticKey(t, vectorEphPrivHex, vectorEphPubHex)
}

// vectorsCheckRequestPrefix asserts the request ephemeral prefix is fresh.
func vectorsCheckRequestPrefix(t *testing.T, req []byte, initKey, respKey StaticKey) {
	t.Helper()
	zero := make([]byte, 32)
	ephPrefix := req[:32]
	if bytes.Equal(ephPrefix, zero) {
		t.Fatalf("request ephemeral prefix is all zero")
	}
	if bytes.Equal(ephPrefix, initKey.Public[:]) {
		t.Fatalf("request prefix equals initiator static pub")
	}
	if bytes.Equal(ephPrefix, respKey.Public[:]) {
		t.Fatalf("request prefix equals responder static pub")
	}
}

// vectorsCheckResponsePrefix asserts the response ephemeral prefix is fresh.
func vectorsCheckResponsePrefix(t *testing.T, respMsg []byte) {
	t.Helper()
	zero := make([]byte, 32)
	if bytes.Equal(respMsg[:32], zero) {
		t.Fatalf("response ephemeral prefix is all zero")
	}
}

// vectorsCheckTransports cross checks both transport directions.
func vectorsCheckTransports(t *testing.T, initSend, initRecv, respSend, respRecv *Transport) {
	t.Helper()
	if initSend == nil || initRecv == nil || respSend == nil || respRecv == nil {
		t.Fatalf("nil transport after split")
	}
	ad := []byte("vector-ad")
	ct := mustEncrypt(t, initSend, ad, []byte("ping"), "initSend Encrypt")
	pt := mustDecrypt(t, respRecv, ad, ct, "respRecv Decrypt")
	if !bytes.Equal(pt, []byte("ping")) {
		t.Fatalf("forward transport %q, want %q", pt, "ping")
	}
	ct = mustEncrypt(t, respSend, ad, []byte("pong"), "respSend Encrypt")
	pt = mustDecrypt(t, initRecv, ad, ct, "initRecv Decrypt")
	if !bytes.Equal(pt, []byte("pong")) {
		t.Fatalf("reverse transport %q, want %q", pt, "pong")
	}
}

// TestIKVectorsHandshake runs a full IK handshake with deterministic
// statics and checks framing, the ephemeral prefix, payload delivery,
// and response verification.
func TestIKVectorsHandshake(t *testing.T) {
	initKey, respKey := vectorStatics(t)
	initPayload := []byte("hello-initiator")
	respPayload := []byte("hello-responder")
	initiator, err := NewIKInitiator([]byte(vectorPrologue), initKey, respKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator: %v", err)
	}
	req := mustWriteRequest(t, initiator, initPayload)
	if len(req) != 96+len(initPayload) {
		t.Fatalf("request len %d, want %d", len(req), 96+len(initPayload))
	}
	vectorsCheckRequestPrefix(t, req, initKey, respKey)
	responder, err := NewIKResponder([]byte(vectorPrologue), respKey)
	if err != nil {
		t.Fatalf("NewIKResponder: %v", err)
	}
	gotInitPayload, peerStatic := mustReadRequest(t, responder, req)
	if !bytes.Equal(gotInitPayload, initPayload) {
		t.Fatalf("initiator payload %q, want %q", gotInitPayload, initPayload)
	}
	if !bytes.Equal(peerStatic[:], initKey.Public[:]) {
		t.Fatalf("peer static %x, want %x", peerStatic, initKey.Public)
	}
	respMsg, respSend, respRecv := mustWriteResponse(t, responder, respPayload)
	if len(respMsg) != 48+len(respPayload) {
		t.Fatalf("response len %d, want %d", len(respMsg), 48+len(respPayload))
	}
	vectorsCheckResponsePrefix(t, respMsg)
	gotRespPayload, initSend, initRecv := mustReadResponse(t, initiator, respMsg)
	if !bytes.Equal(gotRespPayload, respPayload) {
		t.Fatalf("responder payload %q, want %q", gotRespPayload, respPayload)
	}
	vectorsCheckTransports(t, initSend, initRecv, respSend, respRecv)
}

// TestIKVectorsTamperRejects checks that corrupt handshake messages fail.
func TestIKVectorsTamperRejects(t *testing.T) {
	initKey, respKey := vectorStatics(t)

	initiator, err := NewIKInitiator([]byte(vectorPrologue), initKey, respKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator: %v", err)
	}
	req, err := initiator.WriteRequest([]byte("payload"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	badReq := bytes.Clone(req)
	badReq[len(badReq)-1] ^= 0x01
	responder, err := NewIKResponder([]byte(vectorPrologue), respKey)
	if err != nil {
		t.Fatalf("NewIKResponder: %v", err)
	}
	_, _, err = responder.ReadRequest(badReq)
	if err == nil {
		t.Fatalf("ReadRequest with tampered payload succeeded")
	}

	initiator2, err := NewIKInitiator([]byte(vectorPrologue), initKey, respKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator retry: %v", err)
	}
	req2, err := initiator2.WriteRequest([]byte("payload"))
	if err != nil {
		t.Fatalf("WriteRequest retry: %v", err)
	}
	responder2, err := NewIKResponder([]byte(vectorPrologue), respKey)
	if err != nil {
		t.Fatalf("NewIKResponder retry: %v", err)
	}
	_, _, err = responder2.ReadRequest(req2)
	if err != nil {
		t.Fatalf("ReadRequest retry: %v", err)
	}
	respMsg, _, _, err := responder2.WriteResponse([]byte("reply"))
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	badResp := bytes.Clone(respMsg)
	badResp[len(badResp)-1] ^= 0x01
	_, _, _, err = initiator2.ReadResponse(badResp)
	if err == nil {
		t.Fatalf("ReadResponse with tampered payload succeeded")
	}
}
