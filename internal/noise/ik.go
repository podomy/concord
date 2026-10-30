// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math"
)

// MaxMsgLen is the largest handshake payload or transport plaintext in
// bytes. Longer inputs fail closed before any crypto runs.
const MaxMsgLen = 65535

// ikProtocolName seeds InitializeSymmetric. The caller prologue (for the
// mesh, concord-noise-transport-v1) is mixed next with MixHash.
const ikProtocolName = "Noise_IK_25519_ChaChaPoly_SHA256"

// Fixed handshake message overheads: 32-byte ephemeral public key, 48-byte
// encrypted static key, 16-byte AEAD tag per encrypted section.
const (
	ikPubLen  = 32
	ikEncSLen = 48
	ikTagLen  = 16

	ikReqMin  = ikPubLen + ikEncSLen + ikTagLen
	ikReqMax  = ikReqMin + MaxMsgLen
	ikRespMin = ikPubLen + ikTagLen
	ikRespMax = ikRespMin + MaxMsgLen
)

// Transport carries one direction of a Noise session after Split. The zero
// value is not usable; build both ends from a completed handshake. A
// Transport is not safe for concurrent use; the caller serializes each
// direction.
type Transport struct {
	key   [32]byte
	nonce uint64
}

// Encrypt seals plaintext with ad and appends the ciphertext to out. The
// nonce starts at 0, increments per message, and fails closed at 2^64-1:
// the overflowing message is never encrypted.
func (t *Transport) Encrypt(out, ad, plaintext []byte) ([]byte, error) {
	if len(plaintext) > MaxMsgLen {
		err := fmt.Errorf("noise: plaintext %d bytes exceeds max %d", len(plaintext), MaxMsgLen)
		return nil, err
	}
	if t.nonce == math.MaxUint64 {
		err := errors.New("noise: transport nonce exhausted")
		return nil, err //nolint:wrapcheck // sentinel error, no wrap target
	}
	ct, err := aeadEncrypt(t.key, t.nonce, ad, plaintext)
	if err != nil {
		err = fmt.Errorf("noise: transport encrypt: %w", err)
		return nil, err
	}
	t.nonce++

	return append(out, ct...), nil
}

// Decrypt opens ciphertext with ad and appends the plaintext to out. The
// nonce tracks the peer message count and increments only on success, so a
// forged message leaves the stream position unchanged. Lengths outside the
// exact ciphertext range fail closed before decryption.
func (t *Transport) Decrypt(out, ad, ciphertext []byte) ([]byte, error) {
	if len(ciphertext) < ikTagLen || len(ciphertext) > MaxMsgLen+ikTagLen {
		err := fmt.Errorf("noise: ciphertext %d bytes outside valid range", len(ciphertext))
		return nil, err
	}
	if t.nonce == math.MaxUint64 {
		err := errors.New("noise: transport nonce exhausted")
		return nil, err //nolint:wrapcheck // sentinel error, no wrap target
	}
	pt, err := aeadDecrypt(t.key, t.nonce, ad, ciphertext)
	if err != nil {
		err = fmt.Errorf("noise: transport decrypt: %w", err)
		return nil, err
	}
	t.nonce++

	return append(out, pt...), nil
}

// Rekey replaces the transport key with the first output of Noise
// HKDF-SHA256 keyed by the current key over empty input. The nonce is
// unchanged, so rekey points must be agreed by the application; both mesh
// peers run this code and derive the same next key.
func (t *Transport) Rekey() error {
	next, _ := hkdfSHA256(t.key[:], nil)
	t.key = next

	return nil
}

// IKInitiator runs the dialing side of IK. The responder static key comes
// from gossip and is bound into the handshake hash before any bytes move.
type IKInitiator struct {
	sym    symmetricState
	stat   StaticKey
	peer   [32]byte
	eph    StaticKey
	wrote  bool
	failed bool
	done   bool
}

// NewIKInitiator starts an IK handshake for static against peerStatic with
// prologue mixed after InitializeSymmetric. Zero keys fail closed.
func NewIKInitiator(prologue []byte, static StaticKey, peerStatic [32]byte) (*IKInitiator, error) {
	if ikIsZero(&static.Private) {
		err := errors.New("noise: initiator static private key is zero")
		return nil, err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if ikIsZero(&static.Public) {
		err := errors.New("noise: initiator static public key is zero")
		return nil, err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if ikIsZero(&peerStatic) {
		err := errors.New("noise: responder static key is zero")
		return nil, err //nolint:wrapcheck // sentinel error, no wrap target
	}
	i := &IKInitiator{stat: static, peer: peerStatic}
	i.sym = newSymmetric([]byte(ikProtocolName))
	i.sym.MixHash(prologue)
	i.sym.MixHash(peerStatic[:])

	return i, nil
}

// checkWriteRequest validates initiator state and payload length for
// WriteRequest. It preserves the exact order and messages of the inline
// checks it replaces.
func (i *IKInitiator) checkWriteRequest(payload []byte) error {
	if i.failed {
		err := errors.New("noise: initiator handshake already failed")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if i.done {
		err := errors.New("noise: initiator handshake already complete")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if i.wrote {
		err := errors.New("noise: initiator request already written")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if len(payload) > MaxMsgLen {
		err := fmt.Errorf("noise: payload %d bytes exceeds max %d", len(payload), MaxMsgLen)
		return err
	}

	return nil
}

// WriteRequest emits -> e, es, s, ss carrying payload: fresh ephemeral,
// DH with the responder static, encrypted initiator static, DH of both
// statics, then the encrypted payload. It runs once per handshake.
func (i *IKInitiator) WriteRequest(payload []byte) ([]byte, error) {
	err := i.checkWriteRequest(payload)
	if err != nil {
		return nil, err
	}
	eph, err := GenerateStaticKey(rand.Reader)
	if err != nil {
		i.fail()
		err = fmt.Errorf("noise: generate ephemeral: %w", err)
		return nil, err
	}
	i.eph = eph
	i.sym.MixHash(i.eph.Public[:])
	i.sym.MixKey(i.eph.Public[:])
	es, err := dhX25519(i.eph.Private, i.peer)
	if err != nil {
		i.fail()
		err = fmt.Errorf("noise: dh es: %w", err)
		return nil, err
	}
	i.sym.MixKey(es[:])
	ikWipe(&es)
	encS, err := i.sym.EncryptAndHash(i.stat.Public[:])
	if err != nil {
		i.fail()
		return nil, err
	}
	ss, err := dhX25519(i.stat.Private, i.peer)
	if err != nil {
		i.fail()
		err = fmt.Errorf("noise: dh ss: %w", err)
		return nil, err
	}
	i.sym.MixKey(ss[:])
	ikWipe(&ss)
	encPayload, err := i.sym.EncryptAndHash(payload)
	if err != nil {
		i.fail()
		return nil, err
	}
	msg := make([]byte, 0, ikPubLen+len(encS)+len(encPayload))
	msg = append(msg, i.eph.Public[:]...)
	msg = append(msg, encS...)
	msg = append(msg, encPayload...)
	i.wrote = true

	return msg, nil
}

// checkReadResponse validates initiator state and response length for
// ReadResponse. Length failures poison the handshake, matching the inline
// checks it replaces.
func (i *IKInitiator) checkReadResponse(msg []byte) error {
	if i.failed {
		err := errors.New("noise: initiator handshake already failed")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if i.done {
		err := errors.New("noise: initiator handshake already complete")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if !i.wrote {
		err := errors.New("noise: initiator request not written yet")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if len(msg) < ikRespMin || len(msg) > ikRespMax {
		i.fail()
		err := fmt.Errorf("noise: response %d bytes outside valid range", len(msg))
		return err
	}

	return nil
}

// ReadResponse consumes <- e, ee, se and returns the responder payload with
// the session transports. send encrypts initiator to responder traffic and
// recv decrypts the reverse. Any error poisons the handshake; the caller
// must start over, never retry on the same object.
func (i *IKInitiator) ReadResponse(msg []byte) (peerPayload []byte, send, recv *Transport, err error) {
	err = i.checkReadResponse(msg)
	if err != nil {
		return nil, nil, nil, err
	}
	var re [32]byte
	copy(re[:], msg[:ikPubLen])
	i.sym.MixHash(re[:])
	i.sym.MixKey(re[:])
	ee, err := dhX25519(i.eph.Private, re)
	if err != nil {
		i.fail()
		err = fmt.Errorf("noise: dh ee: %w", err)
		return nil, nil, nil, err
	}
	i.sym.MixKey(ee[:])
	ikWipe(&ee)
	se, err := dhX25519(i.stat.Private, re)
	if err != nil {
		i.fail()
		err = fmt.Errorf("noise: dh se: %w", err)
		return nil, nil, nil, err
	}
	i.sym.MixKey(se[:])
	ikWipe(&se)
	peerPayload, err = i.sym.DecryptAndHash(msg[ikPubLen:])
	if err != nil {
		i.fail()
		return nil, nil, nil, err
	}
	if len(peerPayload) > MaxMsgLen {
		i.fail()
		err := fmt.Errorf("noise: response payload %d bytes exceeds max %d", len(peerPayload), MaxMsgLen)
		return nil, nil, nil, err
	}
	k1, k2 := i.sym.Split()
	i.eph.Zero()
	i.sym = symmetricState{}
	i.done = true
	send = &Transport{key: k1}
	recv = &Transport{key: k2}
	ikWipe(&k1)
	ikWipe(&k2)

	return peerPayload, send, recv, nil
}

// fail poisons the initiator and wipes handshake secrets.
func (i *IKInitiator) fail() {
	i.eph.Zero()
	i.sym = symmetricState{}
	i.failed = true
}

// IKResponder runs the accepting side of IK. It learns the initiator static
// key from the first message; the transport layer must verify that key
// against the CA-signed parcel before serving traffic.
type IKResponder struct {
	sym    symmetricState
	stat   StaticKey
	re     [32]byte
	peer   [32]byte
	read   bool
	failed bool
	done   bool
}

// NewIKResponder starts an IK handshake for static with prologue mixed
// after InitializeSymmetric. Zero keys fail closed.
func NewIKResponder(prologue []byte, static StaticKey) (*IKResponder, error) {
	if ikIsZero(&static.Private) {
		err := errors.New("noise: responder static private key is zero")
		return nil, err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if ikIsZero(&static.Public) {
		err := errors.New("noise: responder static public key is zero")
		return nil, err //nolint:wrapcheck // sentinel error, no wrap target
	}
	r := &IKResponder{stat: static}
	r.sym = newSymmetric([]byte(ikProtocolName))
	r.sym.MixHash(prologue)
	r.sym.MixHash(static.Public[:])

	return r, nil
}

// checkReadRequest validates responder state and request length for
// ReadRequest. Length failures poison the handshake, matching the inline
// checks it replaces.
func (r *IKResponder) checkReadRequest(msg []byte) error {
	if r.failed {
		err := errors.New("noise: responder handshake already failed")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if r.done {
		err := errors.New("noise: responder handshake already complete")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if r.read {
		err := errors.New("noise: responder request already read")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if len(msg) < ikReqMin || len(msg) > ikReqMax {
		r.fail()
		err := fmt.Errorf("noise: request %d bytes outside valid range", len(msg))
		return err
	}

	return nil
}

// checkPeerStatic validates the decrypted initiator static key length and
// rejects the all-zero key. It preserves the exact messages of the inline
// checks it replaces.
func (r *IKResponder) checkPeerStatic(rawS []byte, peerStatic *[32]byte) error {
	if len(rawS) != ikPubLen {
		r.fail()
		err := fmt.Errorf("noise: peer static %d bytes, want %d", len(rawS), ikPubLen)
		return err
	}
	copy(peerStatic[:], rawS)
	if ikIsZero(peerStatic) {
		r.fail()
		err := errors.New("noise: peer static key is zero")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}

	return nil
}

// ReadRequest consumes -> e, es, s, ss and returns the initiator payload
// with its static key. An all-zero initiator static key fails closed. Any
// error poisons the handshake; the caller must start over.
func (r *IKResponder) ReadRequest(msg []byte) (payload []byte, peerStatic [32]byte, err error) {
	err = r.checkReadRequest(msg)
	if err != nil {
		return nil, peerStatic, err
	}
	var re [32]byte
	copy(re[:], msg[:ikPubLen])
	r.sym.MixHash(re[:])
	r.sym.MixKey(re[:])
	es, err := dhX25519(r.stat.Private, re)
	if err != nil {
		r.fail()
		err = fmt.Errorf("noise: dh es: %w", err)
		return nil, peerStatic, err
	}
	r.sym.MixKey(es[:])
	ikWipe(&es)
	rawS, err := r.sym.DecryptAndHash(msg[ikPubLen : ikPubLen+ikEncSLen])
	if err != nil {
		r.fail()
		return nil, peerStatic, err
	}
	err = r.checkPeerStatic(rawS, &peerStatic)
	if err != nil {
		return nil, peerStatic, err
	}
	ss, err := dhX25519(r.stat.Private, peerStatic)
	if err != nil {
		r.fail()
		err = fmt.Errorf("noise: dh ss: %w", err)
		return nil, peerStatic, err
	}
	r.sym.MixKey(ss[:])
	ikWipe(&ss)
	payload, err = r.sym.DecryptAndHash(msg[ikPubLen+ikEncSLen:])
	if err != nil {
		r.fail()
		return nil, peerStatic, err
	}
	if len(payload) > MaxMsgLen {
		r.fail()
		err := fmt.Errorf("noise: request payload %d bytes exceeds max %d", len(payload), MaxMsgLen)
		return nil, peerStatic, err
	}
	r.re = re
	r.peer = peerStatic
	r.read = true

	return payload, peerStatic, nil
}

// checkWriteResponse validates responder state and payload length for
// WriteResponse. It preserves the exact order and messages of the inline
// checks it replaces.
func (r *IKResponder) checkWriteResponse(payload []byte) error {
	if r.failed {
		err := errors.New("noise: responder handshake already failed")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if r.done {
		err := errors.New("noise: responder handshake already complete")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if !r.read {
		err := errors.New("noise: responder request not read yet")
		return err //nolint:wrapcheck // sentinel error, no wrap target
	}
	if len(payload) > MaxMsgLen {
		err := fmt.Errorf("noise: payload %d bytes exceeds max %d", len(payload), MaxMsgLen)
		return err
	}

	return nil
}

// WriteResponse emits <- e, ee, se carrying payload and returns the session
// transports. Split order is mirrored against the initiator: the responder
// sends with the second Split key and receives with the first, so responder
// send matches initiator recv and vice versa. It runs once per handshake.
func (r *IKResponder) WriteResponse(payload []byte) (msg []byte, send, recv *Transport, err error) {
	err = r.checkWriteResponse(payload)
	if err != nil {
		return nil, nil, nil, err
	}
	eph, err := GenerateStaticKey(rand.Reader)
	if err != nil {
		r.fail()
		err = fmt.Errorf("noise: generate ephemeral: %w", err)
		return nil, nil, nil, err
	}
	defer eph.Zero()
	r.sym.MixHash(eph.Public[:])
	r.sym.MixKey(eph.Public[:])
	ee, err := dhX25519(eph.Private, r.re)
	if err != nil {
		r.fail()
		err = fmt.Errorf("noise: dh ee: %w", err)
		return nil, nil, nil, err
	}
	r.sym.MixKey(ee[:])
	ikWipe(&ee)
	se, err := dhX25519(eph.Private, r.peer)
	if err != nil {
		r.fail()
		err = fmt.Errorf("noise: dh se: %w", err)
		return nil, nil, nil, err
	}
	r.sym.MixKey(se[:])
	ikWipe(&se)
	encPayload, err := r.sym.EncryptAndHash(payload)
	if err != nil {
		r.fail()
		return nil, nil, nil, err
	}
	msg = make([]byte, 0, ikPubLen+len(encPayload))
	msg = append(msg, eph.Public[:]...)
	msg = append(msg, encPayload...)
	k1, k2 := r.sym.Split()
	r.sym = symmetricState{}
	r.done = true
	send = &Transport{key: k2}
	recv = &Transport{key: k1}
	ikWipe(&k1)
	ikWipe(&k2)

	return msg, send, recv, nil
}

// fail poisons the responder and wipes handshake secrets.
func (r *IKResponder) fail() {
	r.sym = symmetricState{}
	r.failed = true
}

// ikIsZero reports whether k holds all zero bytes.
func ikIsZero(k *[32]byte) bool {
	for i := range k {
		if k[i] != 0 {
			return false
		}
	}

	return true
}

// ikWipe zeroes k.
func ikWipe(k *[32]byte) {
	for i := range k {
		k[i] = 0
	}
}
