// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"crypto/rand"
	"testing"
)

// edgeStatic generates a fresh static keypair or fails the test.
func edgeStatic(t *testing.T) StaticKey {
	t.Helper()
	k, err := GenerateStaticKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateStaticKey: %v", err)
	}
	return k
}

// edgePair builds a matched initiator and responder or fails the test.
// It returns the initiator key for peer-static checks.
func edgePair(t *testing.T) (*IKInitiator, *IKResponder, StaticKey) {
	t.Helper()
	initiatorKey := edgeStatic(t)
	responderKey := edgeStatic(t)
	init, err := NewIKInitiator(edgePrologue, initiatorKey, responderKey.Public)
	if err != nil {
		t.Fatalf("NewIKInitiator: %v", err)
	}
	resp, err := NewIKResponder(edgePrologue, responderKey)
	if err != nil {
		t.Fatalf("NewIKResponder: %v", err)
	}
	return init, resp, initiatorKey
}

// mustWriteRequest writes a handshake request or fails the test.
func mustWriteRequest(t *testing.T, init *IKInitiator, payload []byte) []byte {
	t.Helper()
	req, err := init.WriteRequest(payload)
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	return req
}

// mustReadRequest reads a handshake request or fails the test.
func mustReadRequest(t *testing.T, resp *IKResponder, req []byte) ([]byte, [32]byte) {
	t.Helper()
	got, peer, err := resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	return got, peer
}

// mustWriteResponse writes a handshake response or fails the test.
func mustWriteResponse(t *testing.T, resp *IKResponder, payload []byte) ([]byte, *Transport, *Transport) {
	t.Helper()
	msg, send, recv, err := resp.WriteResponse(payload)
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	return msg, send, recv
}

// mustReadResponse reads a handshake response or fails the test.
func mustReadResponse(t *testing.T, init *IKInitiator, msg []byte) ([]byte, *Transport, *Transport) {
	t.Helper()
	got, send, recv, err := init.ReadResponse(msg)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	return got, send, recv
}

// mustRekey rekeys a transport or fails the test.
func mustRekey(t *testing.T, tr *Transport, what string) {
	t.Helper()
	err := tr.Rekey()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

// mustEncrypt encrypts plaintext or fails the test.
func mustEncrypt(t *testing.T, send *Transport, ad, plain []byte, what string) []byte {
	t.Helper()
	ct, err := send.Encrypt(nil, ad, plain)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	return ct
}

// mustDecrypt decrypts ciphertext or fails the test.
func mustDecrypt(t *testing.T, recv *Transport, ad, ct []byte, what string) []byte {
	t.Helper()
	pt, err := recv.Decrypt(nil, ad, ct)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	return pt
}

// doHandshake runs a full IK handshake and returns paired transports.
func doHandshake(t *testing.T, init *IKInitiator, resp *IKResponder, reqPayload, respPayload []byte) (iSend, iRecv, rSend, rRecv *Transport) {
	t.Helper()
	req := mustWriteRequest(t, init, reqPayload)
	mustReadRequest(t, resp, req)
	respMsg, rSend, rRecv := mustWriteResponse(t, resp, respPayload)
	_, iSend, iRecv = mustReadResponse(t, init, respMsg)
	return iSend, iRecv, rSend, rRecv
}

// rekeyAll rekeys both directions on both ends.
func rekeyAll(t *testing.T, iSend, iRecv, rSend, rRecv *Transport) {
	t.Helper()
	mustRekey(t, iSend, "Rekey initiator send")
	mustRekey(t, rRecv, "Rekey responder recv")
	mustRekey(t, rSend, "Rekey responder send")
	mustRekey(t, iRecv, "Rekey initiator recv")
}

// checkOneSidedBreak runs a handshake then rekeys one side and asserts
// that decryption breaks.
func checkOneSidedBreak(t *testing.T, rekeySender bool) {
	t.Helper()
	init, resp, _ := edgePair(t)
	iSend, _, _, rRecv := doHandshake(t, init, resp, []byte("a"), []byte("b"))
	if rekeySender {
		mustRekey(t, iSend, "sender Rekey")
		ct := mustEncrypt(t, iSend, nil, []byte("data"), "Encrypt after one-sided rekey")
		_, err := rRecv.Decrypt(nil, nil, ct)
		if err == nil {
			t.Fatalf("Decrypt after sender-only rekey succeeded, want error")
		}
		return
	}
	mustRekey(t, rRecv, "receiver Rekey")
	ct := mustEncrypt(t, iSend, nil, []byte("data"), "Encrypt")
	_, err := rRecv.Decrypt(nil, nil, ct)
	if err == nil {
		t.Fatalf("Decrypt after receiver-only rekey succeeded, want error")
	}
}
