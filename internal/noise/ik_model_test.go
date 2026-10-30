// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
)

// modelPrologue is the handshake prologue used by model tests.
var modelPrologue = []byte("concord-noise-transport-v1")

// modelStatic returns a fresh static keypair or fails the test.
func modelStatic(t *testing.T) StaticKey {
	t.Helper()
	k, err := GenerateStaticKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateStaticKey: %v", err)
	}
	return k
}

// modelPair builds a matched initiator and responder or fails the test.
func modelPair(t *testing.T) (*IKInitiator, *IKResponder) {
	t.Helper()
	initKey := modelStatic(t)
	respKey := modelStatic(t)
	init, err := NewIKInitiator(modelPrologue, initKey, respKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator: %v", err)
	}
	resp, err := NewIKResponder(modelPrologue, respKey)
	if err != nil {
		t.Fatalf("NewIKResponder: %v", err)
	}
	return init, resp
}

// modelHandshake runs a full IK handshake and returns paired transports.
func modelHandshake(t *testing.T, initPayload, respPayload []byte) (iSend, iRecv, rSend, rRecv *Transport) {
	t.Helper()
	init, resp := modelPair(t)
	req, err := init.WriteRequest(initPayload)
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, _, err = resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	respMsg, send, recv, err := resp.WriteResponse(respPayload)
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	rSend = send
	rRecv = recv
	_, send, recv, err = init.ReadResponse(respMsg)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	iSend = send
	iRecv = recv
	return iSend, iRecv, rSend, rRecv
}

// modelFlip returns a copy of msg with one byte flipped.
func modelFlip(msg []byte, idx int) []byte {
	out := bytes.Clone(msg)
	out[idx] ^= 0x01
	return out
}

// TestModelInitiatorOrder checks IWrite before IRead ordering, double
// write and double read rejection, and poison terminal on failed.
func TestModelInitiatorOrder(t *testing.T) {
	init, resp := modelPair(t)

	// IRead before IWrite must fail.
	_, _, _, err := init.ReadResponse([]byte("anything"))
	if err == nil {
		t.Fatalf("ReadResponse before WriteRequest succeeded, want error")
	}

	// IWrite runs once and succeeds.
	req, err := init.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	// Second IWrite must fail.
	_, err = init.WriteRequest([]byte("hello"))
	if err == nil {
		t.Fatalf("second WriteRequest succeeded, want error")
	}

	// Drive the responder to obtain a valid response.
	_, _, err = resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	respMsg, _, _, err := resp.WriteResponse([]byte("world"))
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}

	// IRead after IWrite succeeds.
	_, _, _, err = init.ReadResponse(respMsg)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}

	// Second IRead after done must fail.
	_, _, _, err = init.ReadResponse(respMsg)
	if err == nil {
		t.Fatalf("second ReadResponse after done succeeded, want error")
	}

	// Poison terminal: a failed IRead poisons later IWrite and IRead.
	badInit, _ := modelPair(t)
	_, err = badInit.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, _, _, err = badInit.ReadResponse([]byte("short"))
	if err == nil {
		t.Fatalf("ReadResponse with short message succeeded, want error")
	}
	_, err = badInit.WriteRequest([]byte("hello"))
	if err == nil {
		t.Fatalf("WriteRequest after failed ReadResponse succeeded, want poisoned handshake")
	}
	_, _, _, err = badInit.ReadResponse(respMsg)
	if err == nil {
		t.Fatalf("ReadResponse after failure succeeded, want poisoned handshake")
	}
}

// TestModelResponderOrder checks RRead before RWrite ordering, double
// read and double write rejection, and poison terminal on failed.
func TestModelResponderOrder(t *testing.T) {
	init, resp := modelPair(t)

	// RWrite before RRead must fail.
	_, _, _, err := resp.WriteResponse([]byte("anything"))
	if err == nil {
		t.Fatalf("WriteResponse before ReadRequest succeeded, want error")
	}

	req, err := init.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}

	// RRead runs once and succeeds.
	_, _, err = resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}

	// Second RRead must fail.
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("second ReadRequest succeeded, want error")
	}

	// RWrite after RRead succeeds.
	_, _, _, err = resp.WriteResponse([]byte("world"))
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}

	// Second RWrite after done must fail.
	_, _, _, err = resp.WriteResponse([]byte("world"))
	if err == nil {
		t.Fatalf("second WriteResponse after done succeeded, want error")
	}

	// Poison terminal: a failed RRead poisons later RRead and RWrite.
	_, badResp := modelPair(t)
	_, _, err = badResp.ReadRequest([]byte("short"))
	if err == nil {
		t.Fatalf("ReadRequest with short message succeeded, want error")
	}
	_, _, err = badResp.ReadRequest(req)
	if err == nil {
		t.Fatalf("ReadRequest after failure succeeded, want poisoned handshake")
	}
	_, _, _, err = badResp.WriteResponse([]byte("world"))
	if err == nil {
		t.Fatalf("WriteResponse after failed ReadRequest succeeded, want poisoned handshake")
	}
}

// TestModelKeyMirror checks send and recv mirror across Split by
// cross-encrypting each direction with opaque Transport keys.
func TestModelKeyMirror(t *testing.T) {
	iSend, iRecv, rSend, rRecv := modelHandshake(t, []byte("hi"), []byte("yo"))

	// Initiator send must open on responder recv.
	ct, err := iSend.Encrypt(nil, []byte("ad-fwd"), []byte("ping"))
	if err != nil {
		t.Fatalf("Encrypt initiator send: %v", err)
	}
	pt, err := rRecv.Decrypt(nil, []byte("ad-fwd"), ct)
	if err != nil {
		t.Fatalf("Decrypt responder recv: %v", err)
	}
	if !bytes.Equal(pt, []byte("ping")) {
		t.Fatalf("forward cross-decrypt = %q, want %q", pt, "ping")
	}

	// Responder send must open on initiator recv.
	ct, err = rSend.Encrypt(nil, []byte("ad-rev"), []byte("pong"))
	if err != nil {
		t.Fatalf("Encrypt responder send: %v", err)
	}
	pt, err = iRecv.Decrypt(nil, []byte("ad-rev"), ct)
	if err != nil {
		t.Fatalf("Decrypt initiator recv: %v", err)
	}
	if !bytes.Equal(pt, []byte("pong")) {
		t.Fatalf("reverse cross-decrypt = %q, want %q", pt, "pong")
	}
}

// modelOrdered checks five sequential messages each direction decrypt
// in order on a single handshake.
func modelOrdered(t *testing.T) {
	t.Helper()
	iSend, iRecv, rSend, rRecv := modelHandshake(t, []byte("hi"), []byte("yo"))
	fwd := make([][]byte, 5)
	for i := range 5 {
		want := []byte{byte(i), byte(i + 1), byte(i + 2)}
		fwd[i] = mustEncrypt(t, iSend, []byte("ad-fwd"), want, fmt.Sprintf("Encrypt forward %d", i))
	}
	for i := range 5 {
		want := []byte{byte(i), byte(i + 1), byte(i + 2)}
		got := mustDecrypt(t, rRecv, []byte("ad-fwd"), fwd[i], fmt.Sprintf("Decrypt forward %d", i))
		if !bytes.Equal(got, want) {
			t.Fatalf("forward message %d mismatch", i)
		}
	}
	rev := make([][]byte, 5)
	for i := range 5 {
		want := []byte{byte(0x80 + i), byte(i)}
		rev[i] = mustEncrypt(t, rSend, []byte("ad-rev"), want, fmt.Sprintf("Encrypt reverse %d", i))
	}
	for i := range 5 {
		want := []byte{byte(0x80 + i), byte(i)}
		got := mustDecrypt(t, iRecv, []byte("ad-rev"), rev[i], fmt.Sprintf("Decrypt reverse %d", i))
		if !bytes.Equal(got, want) {
			t.Fatalf("reverse message %d mismatch", i)
		}
	}
}

// modelOutOfOrder checks that decrypting message 1 before message 0 fails
// closed but in-order delivery still works.
func modelOutOfOrder(t *testing.T) {
	t.Helper()
	ooSend, _, _, ooRecv := modelHandshake(t, []byte("hi"), []byte("yo"))
	ct0 := mustEncrypt(t, ooSend, nil, []byte("msg0"), "Encrypt msg0")
	ct1 := mustEncrypt(t, ooSend, nil, []byte("msg1"), "Encrypt msg1")
	_, err := ooRecv.Decrypt(nil, nil, ct1)
	if err == nil {
		t.Fatalf("out-of-order Decrypt of msg1 before msg0 succeeded, want error")
	}
	pt := mustDecrypt(t, ooRecv, nil, ct0, "Decrypt msg0 after failed out-of-order attempt")
	if string(pt) != "msg0" {
		t.Fatalf("Decrypt msg0 = %q, want %q", pt, "msg0")
	}
}

// modelDuplicate checks that decrypting the same ciphertext twice fails.
func modelDuplicate(t *testing.T) {
	t.Helper()
	dupSend, _, _, dupRecv := modelHandshake(t, []byte("hi"), []byte("yo"))
	ct := mustEncrypt(t, dupSend, nil, []byte("once"), "Encrypt")
	_ = mustDecrypt(t, dupRecv, nil, ct, "first Decrypt")
	_, err := dupRecv.Decrypt(nil, nil, ct)
	if err == nil {
		t.Fatalf("duplicate Decrypt succeeded, want error")
	}
}

// modelRekeySync checks rekey on both sides preserves interop.
func modelRekeySync(t *testing.T) {
	t.Helper()
	rkISend, rkIRecv, rkRSend, rkRRecv := modelHandshake(t, []byte("hi"), []byte("yo"))
	mustRekey(t, rkISend, "Rekey initiator send")
	mustRekey(t, rkRRecv, "Rekey responder recv")
	mustRekey(t, rkRSend, "Rekey responder send")
	mustRekey(t, rkIRecv, "Rekey initiator recv")
	ct := mustEncrypt(t, rkISend, nil, []byte("post-fwd"), "Encrypt post-rekey forward")
	pt := mustDecrypt(t, rkRRecv, nil, ct, "Decrypt post-rekey forward")
	if string(pt) != "post-fwd" {
		t.Fatalf("post-rekey forward = %q, want %q", pt, "post-fwd")
	}
	ct = mustEncrypt(t, rkRSend, nil, []byte("post-rev"), "Encrypt post-rekey reverse")
	pt = mustDecrypt(t, rkIRecv, nil, ct, "Decrypt post-rekey reverse")
	if string(pt) != "post-rev" {
		t.Fatalf("post-rekey reverse = %q, want %q", pt, "post-rev")
	}
}

// modelOneSided checks that one-sided rekey breaks the peer.
func modelOneSided(t *testing.T) {
	t.Helper()
	oneSend, _, _, oneRecv := modelHandshake(t, []byte("hi"), []byte("yo"))
	mustRekey(t, oneSend, "Rekey sender only")
	ct := mustEncrypt(t, oneSend, nil, []byte("broken"), "Encrypt after one-sided Rekey")
	_, err := oneRecv.Decrypt(nil, nil, ct)
	if err == nil {
		t.Fatalf("Decrypt after one-sided Rekey succeeded, want error")
	}
}

// TestModelTransportNonces checks ordered delivery, out-of-order and
// duplicate rejection, and Rekey interop for both transport directions.
func TestModelTransportNonces(t *testing.T) {
	modelOrdered(t)
	modelOutOfOrder(t)
	modelDuplicate(t)
	modelRekeySync(t)
	modelOneSided(t)
}

// TestModelTamperMapsToFailed checks a tampered request maps RRead to
// failed terminal so later RWrite also fails.
func TestModelTamperMapsToFailed(t *testing.T) {
	init, resp := modelPair(t)
	req, err := init.WriteRequest([]byte("hello"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	bad := modelFlip(req, len(req)-1)
	_, _, err = resp.ReadRequest(bad)
	if err == nil {
		t.Fatalf("ReadRequest with tampered byte succeeded, want error")
	}
	_, _, _, err = resp.WriteResponse([]byte("world"))
	if err == nil {
		t.Fatalf("WriteResponse after tampered ReadRequest succeeded, want failed terminal")
	}
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("ReadRequest retry after tamper succeeded, want failed terminal")
	}
}
