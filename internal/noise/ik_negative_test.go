// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// negPrologue is the handshake prologue used by negative tests.
var negPrologue = []byte("concord-noise-transport-v1")

// negStatic generates a fresh static keypair or fails the test.
func negStatic(t *testing.T) StaticKey {
	t.Helper()
	k, err := GenerateStaticKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateStaticKey: %v", err)
	}
	return k
}

// negPair builds a matched initiator and responder or fails the test.
func negPair(t *testing.T, prologue []byte) (*IKInitiator, *IKResponder) {
	t.Helper()
	initiatorKey := negStatic(t)
	responderKey := negStatic(t)
	init, err := NewIKInitiator(prologue, initiatorKey, responderKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator: %v", err)
	}
	resp, err := NewIKResponder(prologue, responderKey)
	if err != nil {
		t.Fatalf("NewIKResponder: %v", err)
	}
	return init, resp
}

// negComplete runs a full handshake or fails the test. It returns the
// initiator send and recv transports and the responder send and recv.
func negComplete(t *testing.T, reqPayload, respPayload []byte) (iSend, iRecv, rSend, rRecv *Transport) {
	t.Helper()
	init, resp := negPair(t, negPrologue)
	req := mustWriteRequest(t, init, reqPayload)
	mustReadRequest(t, resp, req)
	respMsg, rSend, rRecv := mustWriteResponse(t, resp, respPayload)
	_, iSend, iRecv = mustReadResponse(t, init, respMsg)
	return iSend, iRecv, rSend, rRecv
}

// negFlip returns a copy of msg with the byte at idx flipped.
func negFlip(msg []byte, idx int) []byte {
	out := bytes.Clone(msg)
	out[idx] ^= 0x01
	return out
}

// negTamperRequestCase runs one tamper-request subcase.
func negTamperRequestCase(t *testing.T, payload []byte, name string, idx func(n int) int) {
	t.Helper()
	init, resp := negPair(t, negPrologue)
	req := mustWriteRequest(t, init, payload)
	bad := negFlip(req, idx(len(req)))
	_, _, err := resp.ReadRequest(bad)
	if err == nil {
		t.Fatalf("ReadRequest with tampered %s byte succeeded, want error", name)
	}
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("ReadRequest retry after tampered %s byte succeeded, want poisoned handshake", name)
	}
}

// negTamperCases is the shared first/middle/last table for tamper tests.
func negTamperCases() []struct {
	name string
	idx  func(n int) int
} {
	return []struct {
		name string
		idx  func(n int) int
	}{
		{name: "first", idx: func(n int) int { return 0 }},
		{name: "middle", idx: func(n int) int { return n / 2 }},
		{name: "last", idx: func(n int) int { return n - 1 }},
	}
}

// negRunCases runs each tamper case as a subtest.
func negRunCases(t *testing.T, payload []byte, fn func(t *testing.T, payload []byte, name string, idx func(n int) int)) {
	t.Helper()
	for _, tc := range negTamperCases() {
		t.Run(tc.name, func(t *testing.T) {
			fn(t, payload, tc.name, tc.idx)
		})
	}
}

// TestIKNegativeTamperRequest flips the first, middle, and last byte of
// the request. Each case must fail ReadRequest and poison the responder.
func TestIKNegativeTamperRequest(t *testing.T) {
	negRunCases(t, []byte("request payload"), negTamperRequestCase)
}

// negTamperResponseCase runs one tamper-response subcase.
func negTamperResponseCase(t *testing.T, payload []byte, name string, idx func(n int) int) {
	t.Helper()
	init, resp := negPair(t, negPrologue)
	req := mustWriteRequest(t, init, []byte("hello"))
	mustReadRequest(t, resp, req)
	respMsg, _, _ := mustWriteResponse(t, resp, payload)
	bad := negFlip(respMsg, idx(len(respMsg)))
	_, _, _, err := init.ReadResponse(bad)
	if err == nil {
		t.Fatalf("ReadResponse with tampered %s byte succeeded, want error", name)
	}
	_, _, _, err = init.ReadResponse(respMsg)
	if err == nil {
		t.Fatalf("ReadResponse retry after tampered %s byte succeeded, want poisoned handshake", name)
	}
}

// TestIKNegativeTamperResponse flips the first, middle, and last byte of
// the response. Each case must fail ReadResponse and poison the initiator.
func TestIKNegativeTamperResponse(t *testing.T) {
	negRunCases(t, []byte("response payload"), negTamperResponseCase)
}

// TestIKNegativeWrongPrologue checks that a prologue mismatch fails the
// handshake and poisons the responder so retry cannot succeed.
func TestIKNegativeWrongPrologue(t *testing.T) {
	initiatorKey := negStatic(t)
	responderKey := negStatic(t)
	init, err := NewIKInitiator([]byte("prologue-a"), initiatorKey, responderKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator: %v", err)
	}
	resp, err := NewIKResponder([]byte("prologue-b"), responderKey)
	if err != nil {
		t.Fatalf("NewIKResponder: %v", err)
	}
	req, err := init.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("ReadRequest with wrong prologue succeeded, want error")
	}
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("ReadRequest retry after wrong prologue succeeded, want poisoned handshake")
	}
}

// TestIKNegativeWrongPeerStatic checks that binding the wrong responder
// static key fails the handshake and poisons the responder.
func TestIKNegativeWrongPeerStatic(t *testing.T) {
	initiatorKey := negStatic(t)
	responderKey := negStatic(t)
	strangerKey := negStatic(t)
	init, err := NewIKInitiator(negPrologue, initiatorKey, strangerKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator: %v", err)
	}
	resp, err := NewIKResponder(negPrologue, responderKey)
	if err != nil {
		t.Fatalf("NewIKResponder: %v", err)
	}
	req, err := init.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("ReadRequest with wrong peer static succeeded, want error")
	}
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("ReadRequest retry after wrong peer static succeeded, want poisoned handshake")
	}
}

// TestIKNegativeTruncatedRequest checks that a request short by one byte
// fails and poisons the responder.
func TestIKNegativeTruncatedRequest(t *testing.T) {
	init, resp := negPair(t, negPrologue)
	req, err := init.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	short := req[:len(req)-1]
	_, _, err = resp.ReadRequest(short)
	if err == nil {
		t.Fatalf("ReadRequest with truncated message succeeded, want error")
	}
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("ReadRequest retry after truncation succeeded, want poisoned handshake")
	}
}

// TestIKNegativeTruncatedResponse checks that a response short by one byte
// fails and poisons the initiator.
func TestIKNegativeTruncatedResponse(t *testing.T) {
	init, resp := negPair(t, negPrologue)
	req, err := init.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, _, err = resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	respMsg, _, _, err := resp.WriteResponse([]byte("world"))
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	short := respMsg[:len(respMsg)-1]
	_, _, _, err = init.ReadResponse(short)
	if err == nil {
		t.Fatalf("ReadResponse with truncated message succeeded, want error")
	}
	_, _, _, err = init.ReadResponse(respMsg)
	if err == nil {
		t.Fatalf("ReadResponse retry after truncation succeeded, want poisoned handshake")
	}
}

// TestIKNegativeOversizePayload checks that MaxMsgLen+1 payloads are
// rejected on both handshake writes and on transport encrypt.
func TestIKNegativeOversizePayload(t *testing.T) {
	big := make([]byte, MaxMsgLen+1)
	init, resp := negPair(t, negPrologue)
	_, err := init.WriteRequest(big)
	if err == nil {
		t.Fatalf("WriteRequest with %d bytes succeeded, want error", len(big))
	}
	req, err := init.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, _, err = resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	_, _, _, err = resp.WriteResponse(big)
	if err == nil {
		t.Fatalf("WriteResponse with %d bytes succeeded, want error", len(big))
	}
	iSend, _, _, _ := negComplete(t, []byte("a"), []byte("b"))
	_, err = iSend.Encrypt(nil, nil, big)
	if err == nil {
		t.Fatalf("Encrypt with %d bytes succeeded, want error", len(big))
	}
}

// TestIKNegativeTransportOutOfOrder checks that decrypting the second
// transport message first fails, then in-order delivery still works.
func TestIKNegativeTransportOutOfOrder(t *testing.T) {
	iSend, _, _, rRecv := negComplete(t, []byte("a"), []byte("b"))
	ct0, err := iSend.Encrypt(nil, nil, []byte("msg0"))
	if err != nil {
		t.Fatalf("Encrypt msg0: %v", err)
	}
	ct1, err := iSend.Encrypt(nil, nil, []byte("msg1"))
	if err != nil {
		t.Fatalf("Encrypt msg1: %v", err)
	}
	_, err = rRecv.Decrypt(nil, nil, ct1)
	if err == nil {
		t.Fatalf("out-of-order Decrypt succeeded, want error")
	}
	pt, err := rRecv.Decrypt(nil, nil, ct0)
	if err != nil {
		t.Fatalf("Decrypt ct0 after failed ct1: %v", err)
	}
	if string(pt) != "msg0" {
		t.Fatalf("Decrypt ct0 = %q, want %q", pt, "msg0")
	}
	pt, err = rRecv.Decrypt(nil, nil, ct1)
	if err != nil {
		t.Fatalf("Decrypt ct1 in order: %v", err)
	}
	if string(pt) != "msg1" {
		t.Fatalf("Decrypt ct1 = %q, want %q", pt, "msg1")
	}
}

// TestIKNegativeTransportNonceReuse checks that decrypting the same
// ciphertext twice fails on the second attempt.
func TestIKNegativeTransportNonceReuse(t *testing.T) {
	iSend, _, _, rRecv := negComplete(t, []byte("a"), []byte("b"))
	ct, err := iSend.Encrypt(nil, nil, []byte("once"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	_, err = rRecv.Decrypt(nil, nil, ct)
	if err != nil {
		t.Fatalf("first Decrypt: %v", err)
	}
	_, err = rRecv.Decrypt(nil, nil, ct)
	if err == nil {
		t.Fatalf("second Decrypt of same ciphertext succeeded, want error")
	}
}

// TestIKNegativeTransportForged checks that a forged ciphertext fails to
// decrypt and leaves the stream position unchanged.
func TestIKNegativeTransportForged(t *testing.T) {
	iSend, _, _, rRecv := negComplete(t, []byte("a"), []byte("b"))
	ct, err := iSend.Encrypt(nil, nil, []byte("secret"))
	if err != nil {
		t.Fatalf("Encrypt: %v", err)
	}
	bad := bytes.Clone(ct)
	bad[len(bad)-1] ^= 0x01
	_, err = rRecv.Decrypt(nil, nil, bad)
	if err == nil {
		t.Fatalf("Decrypt of forged ciphertext succeeded, want error")
	}
	pt, err := rRecv.Decrypt(nil, nil, ct)
	if err != nil {
		t.Fatalf("Decrypt of original after forgery: %v", err)
	}
	if string(pt) != "secret" {
		t.Fatalf("Decrypt = %q, want %q", pt, "secret")
	}
}

// TestIKNegativeZeroStaticKeys checks that zero keys fail closed at
// construction time.
func TestIKNegativeZeroStaticKeys(t *testing.T) {
	good := negStatic(t)
	var zeroPriv StaticKey
	zeroPriv.Public = good.Public
	_, err := NewIKInitiator(negPrologue, zeroPriv, good.Public)
	if err == nil {
		t.Fatalf("NewIKInitiator with zero private succeeded, want error")
	}
	var zeroPub StaticKey
	zeroPub.Private = good.Private
	_, err = NewIKInitiator(negPrologue, zeroPub, good.Public)
	if err == nil {
		t.Fatalf("NewIKInitiator with zero public succeeded, want error")
	}
	_, err = NewIKInitiator(negPrologue, good, [32]byte{})
	if err == nil {
		t.Fatalf("NewIKInitiator with zero peer static succeeded, want error")
	}
	var zeroRespPriv StaticKey
	zeroRespPriv.Public = good.Public
	_, err = NewIKResponder(negPrologue, zeroRespPriv)
	if err == nil {
		t.Fatalf("NewIKResponder with zero private succeeded, want error")
	}
	var zeroRespPub StaticKey
	zeroRespPub.Private = good.Private
	_, err = NewIKResponder(negPrologue, zeroRespPub)
	if err == nil {
		t.Fatalf("NewIKResponder with zero public succeeded, want error")
	}
}

// TestIKNegativeReadBeforeWrite checks that reading a response before
// writing a request, and writing a response before reading a request,
// both fail.
func TestIKNegativeReadBeforeWrite(t *testing.T) {
	init, resp := negPair(t, negPrologue)
	_, _, _, err := init.ReadResponse([]byte("anything"))
	if err == nil {
		t.Fatalf("ReadResponse before WriteRequest succeeded, want error")
	}
	_, _, _, err = resp.WriteResponse([]byte("anything"))
	if err == nil {
		t.Fatalf("WriteResponse before ReadRequest succeeded, want error")
	}
}
