// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
)

// symmetricState holds the Noise symmetric state for the IK handshake.
//
// It tracks the chaining key ck and handshake hash h, plus the handshake
// cipher key k, its availability flag, and the handshake nonce n. Transport
// nonces live in the transport layer; n here is only for handshake payloads
// and starts at 0 with an increment per successful EncryptAndHash or
// DecryptAndHash once a key is set.
//
// Suite dependency (defined in suite.go, same package):
//   - func hkdfSHA256(chainingKey, input []byte) (ck, temp [32]byte)
//   - func aeadEncrypt(key [32]byte, nonce uint64, ad, plaintext []byte) ([]byte, error)
//   - func aeadDecrypt(key [32]byte, nonce uint64, ad, ciphertext []byte) ([]byte, error)
//
// hkdfSHA256 yields two outputs, which covers MixKey and Split. MixKeyAndHash
// needs three outputs per Noise spec section 5, so this file derives them
// with hkdfSHA256Three below using the same HMAC-SHA256 construction.
type symmetricState struct {
	ck    [32]byte
	h     [32]byte
	k     [32]byte
	hasK  bool
	nonce uint64
}

// newSymmetric implements InitializeSymmetric from Noise spec section 5.
// If protocolName is longer than 32 bytes it is hashed into h, otherwise it
// is zero padded. ck copies h, the cipher key is empty, and nonce is 0. The
// handshake layer mixes the prologue with MixHash after this returns.
func newSymmetric(protocolName []byte) symmetricState {
	var st symmetricState
	if len(protocolName) <= 32 {
		copy(st.h[:], protocolName)
	} else {
		st.h = sha256.Sum256(protocolName)
	}
	st.ck = st.h
	st.nonce = 0
	st.hasK = false
	return st
}

// MixHash sets h to SHA256(h || data).
func (s *symmetricState) MixHash(data []byte) {
	h := sha256.New()
	_, _ = h.Write(s.h[:]) //nolint:errcheck // hash Write never fails
	_, _ = h.Write(data)   //nolint:errcheck // hash Write never fails
	h.Sum(s.h[:0])
}

// MixKey mixes input into ck with two HKDF outputs and sets the cipher key.
func (s *symmetricState) MixKey(input []byte) {
	ck, tempK := hkdfSHA256(s.ck[:], input)
	s.ck = ck
	s.k = tempK
	s.hasK = true
	s.nonce = 0
}

// MixKeyAndHash mixes input into ck with three HKDF outputs, hashes the
// second output into h, and sets the cipher key to the third output.
func (s *symmetricState) MixKeyAndHash(input []byte) {
	ck, tempH, tempK := hkdfSHA256Three(s.ck[:], input)
	s.ck = ck
	s.MixHash(tempH[:])
	s.k = tempK
	s.hasK = true
	s.nonce = 0
}

// EncryptAndHash encrypts plaintext with ad h when a key is set, mixes the
// ciphertext into h, and increments the nonce. Without a key it mixes the
// plaintext into h and returns it unchanged.
func (s *symmetricState) EncryptAndHash(plaintext []byte) ([]byte, error) {
	if !s.hasK {
		s.MixHash(plaintext)
		return plaintext, nil
	}
	ct, err := aeadEncrypt(s.k, s.nonce, s.h[:], plaintext)
	if err != nil {
		err = fmt.Errorf("noise: encrypt: %w", err)
		return nil, err
	}
	s.MixHash(ct)
	s.nonce++
	return ct, nil
}

// DecryptAndHash decrypts ciphertext with ad h when a key is set, mixes the
// ciphertext into h, and increments the nonce. Without a key it mixes the
// ciphertext into h and returns it unchanged. A decrypt failure leaves h
// and nonce unchanged.
func (s *symmetricState) DecryptAndHash(ciphertext []byte) ([]byte, error) {
	if !s.hasK {
		s.MixHash(ciphertext)
		return ciphertext, nil
	}
	pt, err := aeadDecrypt(s.k, s.nonce, s.h[:], ciphertext)
	if err != nil {
		err = fmt.Errorf("noise: decrypt: %w", err)
		return nil, err
	}
	s.MixHash(ciphertext)
	s.nonce++
	return pt, nil
}

// Split derives two transport keys with HKDF on ck and zero length input.
// The first return value carries initiator to responder traffic and the
// second carries responder to initiator traffic. The handshake layer assigns
// send and recv keys by role: the initiator sends with the first key and
// receives with the second, and the responder does the reverse.
func (s *symmetricState) Split() (k1, k2 [32]byte) {
	k1, k2 = hkdfSHA256(s.ck[:], []byte{})
	return k1, k2
}

// hkdfSHA256Three is the Noise HKDF with SHA-256 returning three outputs per
// Noise spec section 4. suite.go provides the two-output form; IK needs the
// three-output form for MixKeyAndHash.
func hkdfSHA256Three(chainingKey, input []byte) (ck, tempH, tempK [32]byte) {
	mac := hmac.New(sha256.New, chainingKey)
	_, _ = mac.Write(input) //nolint:errcheck // hash Write never fails
	tempKey := mac.Sum(nil)
	mac = hmac.New(sha256.New, tempKey)
	_, _ = mac.Write([]byte{0x01}) //nolint:errcheck // hash Write never fails
	copy(ck[:], mac.Sum(nil))
	mac = hmac.New(sha256.New, tempKey)
	_, _ = mac.Write(ck[:])        //nolint:errcheck // hash Write never fails
	_, _ = mac.Write([]byte{0x02}) //nolint:errcheck // hash Write never fails
	copy(tempH[:], mac.Sum(nil))
	mac = hmac.New(sha256.New, tempKey)
	_, _ = mac.Write(tempH[:])     //nolint:errcheck // hash Write never fails
	_, _ = mac.Write([]byte{0x03}) //nolint:errcheck // hash Write never fails
	copy(tempK[:], mac.Sum(nil))
	return ck, tempH, tempK
}
