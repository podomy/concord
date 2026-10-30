// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise_test

import (
	"bytes"
	crand "crypto/rand"
	"fmt"
	"sync"
	"testing"

	"github.com/podomy/concord/internal/noise"
)

// concurrentPayloads builds deterministic payloads for a worker tag.
func concurrentPayloads(tag int) (initPayload, respPayload []byte) {
	initPayload = fmt.Appendf(nil, "init payload %d", tag)
	respPayload = fmt.Appendf(nil, "resp payload %d", tag)
	return initPayload, respPayload
}

// concurrentStatics generates fresh initiator and responder statics.
func concurrentStatics() (noise.StaticKey, noise.StaticKey, error) {
	initStatic, err := noise.GenerateStaticKey(crand.Reader)
	if err != nil {
		err = fmt.Errorf("generate initiator static: %w", err)
		return initStatic, noise.StaticKey{}, err
	}
	respStatic, err := noise.GenerateStaticKey(crand.Reader)
	if err != nil {
		err = fmt.Errorf("generate responder static: %w", err)
		return initStatic, respStatic, err
	}
	return initStatic, respStatic, nil
}

// concurrentHandshake runs the IK handshake and checks both payloads.
func concurrentHandshake(tag int, initStatic, respStatic noise.StaticKey, initPayload, respPayload []byte) (*noise.Transport, *noise.Transport, *noise.Transport, *noise.Transport, error) {
	prologue := []byte("concord-noise-transport-v1")
	init, err := noise.NewIKInitiator(prologue, initStatic, respStatic.Public)
	if err != nil {
		err = fmt.Errorf("new initiator: %w", err)
		return nil, nil, nil, nil, err
	}
	req, err := init.WriteRequest(initPayload)
	if err != nil {
		err = fmt.Errorf("write request: %w", err)
		return nil, nil, nil, nil, err
	}
	resp, err := noise.NewIKResponder(prologue, respStatic)
	if err != nil {
		err = fmt.Errorf("new responder: %w", err)
		return nil, nil, nil, nil, err
	}
	gotInit, _, err := resp.ReadRequest(req)
	if err != nil {
		err = fmt.Errorf("read request: %w", err)
		return nil, nil, nil, nil, err
	}
	if !bytes.Equal(gotInit, initPayload) {
		err = fmt.Errorf("request payload mismatch for tag %d", tag)
		return nil, nil, nil, nil, err
	}
	reply, respSend, respRecv, err := resp.WriteResponse(respPayload)
	if err != nil {
		err = fmt.Errorf("write response: %w", err)
		return nil, nil, nil, nil, err
	}
	gotResp, initSend, initRecv, err := init.ReadResponse(reply)
	if err != nil {
		err = fmt.Errorf("read response: %w", err)
		return nil, nil, nil, nil, err
	}
	if !bytes.Equal(gotResp, respPayload) {
		err = fmt.Errorf("response payload mismatch for tag %d", tag)
		return nil, nil, nil, nil, err
	}
	return initSend, initRecv, respSend, respRecv, nil
}

// concurrentTransports checks forward and reverse transport delivery.
func concurrentTransports(tag int, initSend, initRecv, respSend, respRecv *noise.Transport, initPayload, respPayload []byte) error {
	fwd, err := initSend.Encrypt(nil, nil, initPayload)
	if err != nil {
		err = fmt.Errorf("forward encrypt: %w", err)
		return err
	}
	plain, err := respRecv.Decrypt(nil, nil, fwd)
	if err != nil {
		err = fmt.Errorf("forward decrypt: %w", err)
		return err
	}
	if !bytes.Equal(plain, initPayload) {
		err = fmt.Errorf("forward transport mismatch for tag %d", tag)
		return err
	}
	rev, err := respSend.Encrypt(nil, nil, respPayload)
	if err != nil {
		err = fmt.Errorf("reverse encrypt: %w", err)
		return err
	}
	back, err := initRecv.Decrypt(nil, nil, rev)
	if err != nil {
		err = fmt.Errorf("reverse decrypt: %w", err)
		return err
	}
	if !bytes.Equal(back, respPayload) {
		err = fmt.Errorf("reverse transport mismatch for tag %d", tag)
		return err
	}
	return nil
}

// concurrentRoundTrip runs one full handshake plus transport messages in
// both directions with no shared handshake state.
func concurrentRoundTrip(tag int) error {
	initStatic, respStatic, err := concurrentStatics()
	if err != nil {
		return err
	}
	initPayload, respPayload := concurrentPayloads(tag)
	initSend, initRecv, respSend, respRecv, err := concurrentHandshake(tag, initStatic, respStatic, initPayload, respPayload)
	if err != nil {
		return err
	}
	err = concurrentTransports(tag, initSend, initRecv, respSend, respRecv, initPayload, respPayload)
	if err != nil {
		return err
	}
	return nil
}

// TestIKConcurrentHandshakes runs 20 independent handshakes in parallel.
// It must pass under the race detector.
func TestIKConcurrentHandshakes(t *testing.T) {
	const workers = 20
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Add(1)
		go func(tag int) {
			defer wg.Done()
			err := concurrentRoundTrip(tag)
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent handshake: %v", err)
	}
}
