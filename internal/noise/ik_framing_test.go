// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise_test

import (
	"bytes"
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/podomy/concord/internal/noise"
)

// testFrameLenBytes is the width of the length prefix on every frame.
const testFrameLenBytes = 4

// testHandshakeFrameMax is the largest handshake message accepted. It
// mirrors the transport limit without importing that package.
const testHandshakeFrameMax = 2048

// frameEncodeTest prefixes payload with a 4-byte big-endian length.
func frameEncodeTest(payload []byte) []byte {
	out := make([]byte, testFrameLenBytes+len(payload))
	binary.BigEndian.PutUint32(out[:testFrameLenBytes], uint32(len(payload))) //nolint:gosec // test payload length bounded by MaxMsgLen
	copy(out[testFrameLenBytes:], payload)

	return out
}

// frameDecodeTest reads one length-prefixed frame and rejects lengths
// above maxLen before allocating the payload.
func frameDecodeTest(frame []byte, maxLen uint32) ([]byte, error) {
	if len(frame) < testFrameLenBytes {
		err := fmt.Errorf("frame too short: %d bytes", len(frame))
		return nil, err
	}
	length := binary.BigEndian.Uint32(frame[:testFrameLenBytes])
	if length > maxLen {
		err := fmt.Errorf("frame length %d exceeds max %d", length, maxLen)
		return nil, err
	}
	if uint32(len(frame)-testFrameLenBytes) < length { //nolint:gosec // frame length checked against maxLen above
		err := fmt.Errorf("frame truncated: have %d want %d", len(frame)-testFrameLenBytes, length)
		return nil, err
	}

	return append([]byte(nil), frame[testFrameLenBytes:testFrameLenBytes+int(length)]...), nil
}

// framingHandshake completes one IK handshake and returns both transport
// pairs plus the raw request bytes.
func framingHandshake(t *testing.T, prologue, initPayload, respPayload []byte) (req []byte, initSend, initRecv, respSend, respRecv *noise.Transport) {
	t.Helper()
	initStatic, err := noise.GenerateStaticKey(crand.Reader)
	if err != nil {
		t.Fatalf("generate initiator static: %v", err)
	}
	respStatic, err := noise.GenerateStaticKey(crand.Reader)
	if err != nil {
		t.Fatalf("generate responder static: %v", err)
	}
	init, err := noise.NewIKInitiator(prologue, initStatic, respStatic.Public)
	if err != nil {
		t.Fatalf("new initiator: %v", err)
	}
	req, err = init.WriteRequest(initPayload)
	if err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := noise.NewIKResponder(prologue, respStatic)
	if err != nil {
		t.Fatalf("new responder: %v", err)
	}
	_, _, err = resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	reply, respSend, respRecv, err := resp.WriteResponse(respPayload)
	if err != nil {
		t.Fatalf("write response: %v", err)
	}
	_, initSend, initRecv, err = init.ReadResponse(reply)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	return req, initSend, initRecv, respSend, respRecv
}

// TestIKFramingHandshakeFits checks a 280-byte parcel request fits the
// 2048-byte handshake frame limit.
func TestIKFramingHandshakeFits(t *testing.T) {
	parcel := make([]byte, 280)
	_, err := crand.Read(parcel)
	if err != nil {
		t.Fatalf("parcel rand: %v", err)
	}
	req, _, _, _, _ := framingHandshake(t, []byte("concord-noise-transport-v1"), parcel, []byte("ok"))
	if len(req) >= testHandshakeFrameMax {
		t.Fatalf("request %d bytes exceeds frame max %d", len(req), testHandshakeFrameMax)
	}
	frame := frameEncodeTest(req)
	got, err := frameDecodeTest(frame, testHandshakeFrameMax)
	if err != nil {
		t.Fatalf("decode fitted frame: %v", err)
	}
	if !bytes.Equal(got, req) {
		t.Fatalf("framed request mismatch")
	}
}

// TestIKFramingOversizeRejected checks a handshake message over 2048 bytes
// is rejected by the frame length check.
func TestIKFramingOversizeRejected(t *testing.T) {
	big := make([]byte, 2000)
	_, err := crand.Read(big)
	if err != nil {
		t.Fatalf("payload rand: %v", err)
	}
	req, _, _, _, _ := framingHandshake(t, []byte("concord-noise-transport-v1"), big, []byte("ok"))
	if len(req) <= testHandshakeFrameMax {
		t.Fatalf("expected oversize request, got %d bytes", len(req))
	}
	frame := frameEncodeTest(req)
	_, err = frameDecodeTest(frame, testHandshakeFrameMax)
	if err == nil {
		t.Fatalf("oversize frame accepted")
	}
}

// TestIKFramingTransportChunking checks MaxMsgLen chunking: a large input
// splits into 65535-byte pieces that each round trip through transport.
func TestIKFramingTransportChunking(t *testing.T) {
	prologue := []byte("concord-noise-transport-v1")
	_, initSend, _, _, respRecv := framingHandshake(t, prologue, []byte("hi"), []byte("hi"))
	bigLen := 2*noise.MaxMsgLen + 100
	big := make([]byte, bigLen)
	_, err := crand.Read(big)
	if err != nil {
		t.Fatalf("payload rand: %v", err)
	}
	var frames [][]byte
	rest := big
	for len(rest) > 0 {
		n := len(rest)
		n = min(n, noise.MaxMsgLen)
		ct, err := initSend.Encrypt(nil, nil, rest[:n])
		if err != nil {
			t.Fatalf("chunk encrypt: %v", err)
		}
		frames = append(frames, frameEncodeTest(ct))
		rest = rest[n:]
	}
	if len(frames) != 3 {
		t.Fatalf("chunk count %d, want 3", len(frames))
	}
	var reassembled []byte
	for _, f := range frames {
		ct, err := frameDecodeTest(f, uint32(noise.MaxMsgLen+16+testFrameLenBytes))
		if err != nil {
			t.Fatalf("chunk decode: %v", err)
		}
		pt, err := respRecv.Decrypt(nil, nil, ct)
		if err != nil {
			t.Fatalf("chunk decrypt: %v", err)
		}
		reassembled = append(reassembled, pt...)
	}
	if !bytes.Equal(reassembled, big) {
		t.Fatalf("chunked round trip mismatch: got %d want %d", len(reassembled), len(big))
	}
}

// fuzzRoundTrip runs one handshake round trip for fuzz input and fails on
// mismatch. Inputs longer than MaxMsgLen must fail closed, never panic.
func fuzzRoundTrip(t *testing.T, payload []byte) {
	t.Helper()
	initStatic, err := noise.GenerateStaticKey(crand.Reader)
	if err != nil {
		t.Fatalf("generate initiator static: %v", err)
	}
	respStatic, err := noise.GenerateStaticKey(crand.Reader)
	if err != nil {
		t.Fatalf("generate responder static: %v", err)
	}
	prologue := []byte("concord-noise-transport-v1")
	init, err := noise.NewIKInitiator(prologue, initStatic, respStatic.Public)
	if err != nil {
		t.Fatalf("new initiator: %v", err)
	}
	req, err := init.WriteRequest(payload)
	if len(payload) > noise.MaxMsgLen {
		if err == nil {
			t.Fatalf("oversize payload accepted")
		}

		return
	}
	if err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := noise.NewIKResponder(prologue, respStatic)
	if err != nil {
		t.Fatalf("new responder: %v", err)
	}
	got, _, err := resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("fuzz payload mismatch: got %d want %d", len(got), len(payload))
	}
}

// FuzzIKHandshake runs one handshake round trip per input with seed corpus
// covering empty, 1 byte, 256 bytes, and MaxMsgLen.
func FuzzIKHandshake(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x01})
	f.Add(bytes.Repeat([]byte{0x02}, 256))
	f.Add(bytes.Repeat([]byte{0x03}, noise.MaxMsgLen))
	f.Fuzz(func(t *testing.T, payload []byte) {
		fuzzRoundTrip(t, payload)
	})
}
