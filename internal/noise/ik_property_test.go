// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise_test

import (
	"bytes"
	crand "crypto/rand"
	mrand "math/rand"
	"testing"

	"github.com/podomy/concord/internal/noise"
)

// propertyRoundTrip runs one IK handshake and checks both payloads plus
// transport traffic in both directions.
func propertyRoundTrip(t *testing.T, prologue, initPayload, respPayload []byte) {
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
	req, err := init.WriteRequest(initPayload)
	if err != nil {
		t.Fatalf("write request: %v", err)
	}
	resp, err := noise.NewIKResponder(prologue, respStatic)
	if err != nil {
		t.Fatalf("new responder: %v", err)
	}
	gotInit, peerStatic, err := resp.ReadRequest(req)
	if err != nil {
		t.Fatalf("read request: %v", err)
	}
	if peerStatic != initStatic.Public {
		t.Fatalf("peer static mismatch")
	}
	if !bytes.Equal(gotInit, initPayload) {
		t.Fatalf("request payload mismatch: got %d want %d", len(gotInit), len(initPayload))
	}
	reply, respSend, respRecv, err := resp.WriteResponse(respPayload)
	if err != nil {
		t.Fatalf("write response: %v", err)
	}
	gotResp, initSend, initRecv, err := init.ReadResponse(reply)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if !bytes.Equal(gotResp, respPayload) {
		t.Fatalf("response payload mismatch: got %d want %d", len(gotResp), len(respPayload))
	}
	checkTransportPair(t, initSend, respRecv, []byte("initiator to responder"))
	checkTransportPair(t, respSend, initRecv, []byte("responder to initiator"))
}

// checkTransportPair encrypts msg on send and decrypts it on recv.
func checkTransportPair(t *testing.T, send, recv *noise.Transport, msg []byte) {
	t.Helper()
	ct, err := send.Encrypt(nil, nil, msg)
	if err != nil {
		t.Fatalf("transport encrypt: %v", err)
	}
	pt, err := recv.Decrypt(nil, nil, ct)
	if err != nil {
		t.Fatalf("transport decrypt: %v", err)
	}
	if !bytes.Equal(pt, msg) {
		t.Fatalf("transport payload mismatch")
	}
}

// propertySweep runs iters round trips with lengths from rng and bytes
// from fill. It shares the payload sampling for deterministic and crypto
// spot checks. Label preserves the original failure context.
func propertySweep(t *testing.T, rng *mrand.Rand, fill func([]byte) (int, error), iters, maxPro int, label string) {
	t.Helper()
	for range iters {
		initLen := rng.Intn(2001)
		respLen := rng.Intn(2001)
		proLen := rng.Intn(maxPro)
		initPayload := make([]byte, initLen)
		_, err := fill(initPayload)
		if err != nil {
			t.Fatalf("%s init payload: %v", label, err)
		}
		respPayload := make([]byte, respLen)
		_, err = fill(respPayload)
		if err != nil {
			t.Fatalf("%s resp payload: %v", label, err)
		}
		prologue := make([]byte, proLen)
		_, err = fill(prologue)
		if err != nil {
			t.Fatalf("%s prologue: %v", label, err)
		}
		propertyRoundTrip(t, prologue, initPayload, respPayload)
	}
}

// TestIKPropertyRoundTrip runs 100 deterministic round trips with payload
// lengths 0..2000 and random prologues.
func TestIKPropertyRoundTrip(t *testing.T) {
	rng := mrand.New(mrand.NewSource(20261030)) //nolint:gosec // deterministic test vectors need reproducible lengths
	propertySweep(t, rng, rng.Read, 100, 64, "deterministic")
}

// TestIKPropertyCryptoRandSpotCheck repeats a small round trip sample with
// crypto/rand payloads and prologues.
func TestIKPropertyCryptoRandSpotCheck(t *testing.T) {
	rng := mrand.New(mrand.NewSource(7)) //nolint:gosec // deterministic test lengths with crypto payload sampling
	propertySweep(t, rng, crand.Read, 4, 32, "crypto")
}
