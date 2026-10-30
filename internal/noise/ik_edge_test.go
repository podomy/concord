// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"bytes"
	"testing"
)

// edgePrologue is the handshake prologue used by edge tests.
var edgePrologue = []byte("concord-noise-transport-v1")

// TestIKEdgeEmptyPayload checks that empty payloads round trip in both
// handshake messages and that the resulting transports work.
func TestIKEdgeEmptyPayload(t *testing.T) {
	init, resp, _ := edgePair(t)
	req := mustWriteRequest(t, init, nil)
	gotReq, _ := mustReadRequest(t, resp, req)
	if len(gotReq) != 0 {
		t.Fatalf("request payload = %d bytes, want empty", len(gotReq))
	}
	respMsg, rSend, rRecv := mustWriteResponse(t, resp, nil)
	gotResp, iSend, iRecv := mustReadResponse(t, init, respMsg)
	if len(gotResp) != 0 {
		t.Fatalf("response payload = %d bytes, want empty", len(gotResp))
	}
	ct := mustEncrypt(t, iSend, nil, []byte("ping"), "Encrypt")
	pt := mustDecrypt(t, rRecv, nil, ct, "Decrypt")
	if string(pt) != "ping" {
		t.Fatalf("Decrypt = %q, want %q", pt, "ping")
	}
	ct = mustEncrypt(t, rSend, nil, []byte("pong"), "Encrypt")
	pt = mustDecrypt(t, iRecv, nil, ct, "Decrypt")
	if string(pt) != "pong" {
		t.Fatalf("Decrypt = %q, want %q", pt, "pong")
	}
}

// TestIKEdgeExactMaxMsgLen checks that payloads of exactly MaxMsgLen
// bytes are accepted in both handshake messages and on the transport.
func TestIKEdgeExactMaxMsgLen(t *testing.T) {
	reqPayload := bytes.Repeat([]byte{0xA5}, MaxMsgLen)
	respPayload := bytes.Repeat([]byte{0x5A}, MaxMsgLen)
	init, resp, _ := edgePair(t)
	req, err := init.WriteRequest(reqPayload)
	if err != nil {
		t.Fatalf("WriteRequest MaxMsgLen: %v", err)
	}
	gotReq, _, err := resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest MaxMsgLen: %v", err)
	}
	if !bytes.Equal(gotReq, reqPayload) {
		t.Fatalf("request payload mismatch at MaxMsgLen")
	}
	respMsg, _, _, err := resp.WriteResponse(respPayload)
	if err != nil {
		t.Fatalf("WriteResponse MaxMsgLen: %v", err)
	}
	gotResp, iSend, _, err := init.ReadResponse(respMsg)
	if err != nil {
		t.Fatalf("ReadResponse MaxMsgLen: %v", err)
	}
	if !bytes.Equal(gotResp, respPayload) {
		t.Fatalf("response payload mismatch at MaxMsgLen")
	}
	plain := bytes.Repeat([]byte{0x3C}, MaxMsgLen)
	ct, err := iSend.Encrypt(nil, nil, plain)
	if err != nil {
		t.Fatalf("Encrypt MaxMsgLen: %v", err)
	}
	if len(ct) != MaxMsgLen+16 {
		t.Fatalf("ciphertext = %d bytes, want %d", len(ct), MaxMsgLen+16)
	}
}

// TestIKEdgeOversizeRejected checks that MaxMsgLen+1 is rejected on every
// write and decrypt path.
func TestIKEdgeOversizeRejected(t *testing.T) {
	big := make([]byte, MaxMsgLen+1)
	init, resp, _ := edgePair(t)
	_, err := init.WriteRequest(big)
	if err == nil {
		t.Fatalf("WriteRequest with MaxMsgLen+1 succeeded, want error")
	}
	req, err := init.WriteRequest([]byte("ok"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, _, err = resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	_, _, _, err = resp.WriteResponse(big)
	if err == nil {
		t.Fatalf("WriteResponse with MaxMsgLen+1 succeeded, want error")
	}
	respMsg, _, rRecv, err := resp.WriteResponse([]byte("ok"))
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	_, iSend, iRecv, err := init.ReadResponse(respMsg)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	_, err = iSend.Encrypt(nil, nil, big)
	if err == nil {
		t.Fatalf("Encrypt with MaxMsgLen+1 succeeded, want error")
	}
	bigCt := make([]byte, MaxMsgLen+1+16)
	_, err = rRecv.Decrypt(nil, nil, bigCt)
	if err == nil {
		t.Fatalf("Decrypt with oversize ciphertext succeeded, want error")
	}
	_, err = iRecv.Decrypt(nil, nil, bigCt)
	if err == nil {
		t.Fatalf("Decrypt with oversize ciphertext succeeded, want error")
	}
}

// TestIKEdgeRekeyBothSidesSync checks that rekeying both ends of a
// direction keeps traffic flowing in both directions.
func TestIKEdgeRekeyBothSidesSync(t *testing.T) {
	init, resp, _ := edgePair(t)
	iSend, iRecv, rSend, rRecv := doHandshake(t, init, resp, []byte("a"), []byte("b"))
	rekeyAll(t, iSend, iRecv, rSend, rRecv)
	ct := mustEncrypt(t, iSend, nil, []byte("after rekey"), "Encrypt after rekey")
	pt := mustDecrypt(t, rRecv, nil, ct, "Decrypt after rekey")
	if string(pt) != "after rekey" {
		t.Fatalf("Decrypt = %q, want %q", pt, "after rekey")
	}
	ct = mustEncrypt(t, rSend, nil, []byte("back"), "Encrypt after rekey")
	pt = mustDecrypt(t, iRecv, nil, ct, "Decrypt after rekey")
	if string(pt) != "back" {
		t.Fatalf("Decrypt = %q, want %q", pt, "back")
	}
}

// TestIKEdgeRekeyOneSidedBreaks checks that rekeying only one end of a
// direction breaks decryption for the peer.
func TestIKEdgeRekeyOneSidedBreaks(t *testing.T) {
	checkOneSidedBreak(t, true)
}

// TestIKEdgeRekeyReceiverOnlyBreaks checks that rekeying only the receiver
// breaks decryption.
func TestIKEdgeRekeyReceiverOnlyBreaks(t *testing.T) {
	checkOneSidedBreak(t, false)
}

// TestIKEdgePeerStaticAndPayload checks that ReadRequest reports the
// initiator static key and that both payloads pass through intact.
func TestIKEdgePeerStaticAndPayload(t *testing.T) {
	reqPayload := []byte("request-data-123")
	respPayload := []byte("response-data-456")
	init, resp, initiatorKey := edgePair(t)
	req, err := init.WriteRequest(reqPayload)
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	gotReq, peerStatic, err := resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	if peerStatic != initiatorKey.Public {
		t.Fatalf("peerStatic mismatch: got %x, want %x", peerStatic, initiatorKey.Public)
	}
	if !bytes.Equal(gotReq, reqPayload) {
		t.Fatalf("request payload = %q, want %q", gotReq, reqPayload)
	}
	respMsg, _, _, err := resp.WriteResponse(respPayload)
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	gotResp, _, _, err := init.ReadResponse(respMsg)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	if !bytes.Equal(gotResp, respPayload) {
		t.Fatalf("response payload = %q, want %q", gotResp, respPayload)
	}
}

// TestIKEdgeStateMisuseDoubleCall checks that repeating a one-shot
// handshake step fails on both roles.
func TestIKEdgeStateMisuseDoubleCall(t *testing.T) {
	init, resp, _ := edgePair(t)
	req, err := init.WriteRequest([]byte("a"))
	if err != nil {
		t.Fatalf("WriteRequest: %v", err)
	}
	_, err = init.WriteRequest([]byte("b"))
	if err == nil {
		t.Fatalf("second WriteRequest succeeded, want error")
	}
	_, _, err = resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("ReadRequest: %v", err)
	}
	_, _, err = resp.ReadRequest(req)
	if err == nil {
		t.Fatalf("second ReadRequest succeeded, want error")
	}
	respMsg, _, _, err := resp.WriteResponse([]byte("b"))
	if err != nil {
		t.Fatalf("WriteResponse: %v", err)
	}
	_, _, _, err = resp.WriteResponse([]byte("c"))
	if err == nil {
		t.Fatalf("second WriteResponse succeeded, want error")
	}
	_, _, _, err = init.ReadResponse(respMsg)
	if err != nil {
		t.Fatalf("ReadResponse: %v", err)
	}
	_, _, _, err = init.ReadResponse(respMsg)
	if err == nil {
		t.Fatalf("second ReadResponse succeeded, want error")
	}
}
