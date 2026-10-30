// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"golang.org/x/crypto/chacha20poly1305"
)

// dhX25519 performs X25519 DH between priv and pub per Noise section 4.
func dhX25519(priv, pub [32]byte) ([32]byte, error) {
	var out [32]byte
	curve := ecdh.X25519()
	privKey, err := curve.NewPrivateKey(priv[:])
	if err != nil {
		err = fmt.Errorf("noise: invalid dh private key: %w", err)
		return out, err
	}
	pubKey, err := curve.NewPublicKey(pub[:])
	if err != nil {
		err = fmt.Errorf("noise: invalid dh public key: %w", err)
		return out, err
	}
	shared, err := privKey.ECDH(pubKey)
	if err != nil {
		err = fmt.Errorf("noise: ecdh failed: %w", err)
		return out, err
	}
	copy(out[:], shared)
	return out, nil
}

// hkdfSHA256 is the Noise HKDF with SHA-256 returning two outputs per Noise section 4.
func hkdfSHA256(chainingKey, input []byte) (ck, temp [32]byte) {
	tmp := hmacSHA256OneShot(chainingKey, input)
	one := hmacSHA256OneShot(tmp, []byte{0x01})
	copy(ck[:], one)
	buf := make([]byte, 0, 33)
	buf = append(buf, one...)
	buf = append(buf, 0x02)
	two := hmacSHA256OneShot(tmp, buf)
	copy(temp[:], two)
	return ck, temp
}

// hmacSHA256OneShot returns HMAC-SHA256(key, data).
func hmacSHA256OneShot(key, data []byte) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(data) //nolint:errcheck // hash Write never fails
	return mac.Sum(nil)
}

// aeadEncrypt encrypts plaintext with ChaCha20-Poly1305 per Noise section 4.
func aeadEncrypt(key [32]byte, nonce uint64, ad, plaintext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		err = fmt.Errorf("noise: new aead: %w", err)
		return nil, err
	}
	var n [12]byte
	binary.LittleEndian.PutUint64(n[4:], nonce)
	out := aead.Seal(nil, n[:], plaintext, ad)
	return out, nil
}

// aeadDecrypt decrypts ciphertext with ChaCha20-Poly1305 per Noise section 4.
func aeadDecrypt(key [32]byte, nonce uint64, ad, ciphertext []byte) ([]byte, error) {
	aead, err := chacha20poly1305.New(key[:])
	if err != nil {
		err = fmt.Errorf("noise: new aead: %w", err)
		return nil, err
	}
	var n [12]byte
	binary.LittleEndian.PutUint64(n[4:], nonce)
	plain, err := aead.Open(nil, n[:], ciphertext, ad)
	if err != nil {
		err = fmt.Errorf("noise: aead open: %w", err)
		return nil, err
	}
	return plain, nil
}
