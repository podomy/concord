// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package noise

import (
	"crypto/ecdh"
	"errors"
	"fmt"
	"io"
)

// StaticKey holds an X25519 static keypair for the Noise IK handshake.
type StaticKey struct {
	Private [32]byte
	Public  [32]byte
}

// GenerateStaticKey creates a fresh X25519 static keypair using rand as entropy.
func GenerateStaticKey(rand io.Reader) (StaticKey, error) {
	var k StaticKey
	if rand == nil {
		err := errors.New("noise: nil rand reader")
		return StaticKey{}, err //nolint:wrapcheck // sentinel error, no wrap target
	}
	_, err := io.ReadFull(rand, k.Private[:])
	if err != nil {
		err = fmt.Errorf("noise: read private key: %w", err)
		return StaticKey{}, err
	}
	curve := ecdh.X25519()
	priv, err := curve.NewPrivateKey(k.Private[:])
	if err != nil {
		k.Zero()
		err = fmt.Errorf("noise: invalid private key: %w", err)
		return StaticKey{}, err
	}
	pub := priv.PublicKey()
	copy(k.Public[:], pub.Bytes())
	return k, nil
}

// Zero wipes the keypair.
func (k *StaticKey) Zero() {
	for i := range k.Private {
		k.Private[i] = 0
	}
	for i := range k.Public {
		k.Public[i] = 0
	}
}
