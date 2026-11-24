// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: BUSL-1.1

package certs

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"fmt"

	"github.com/google/uuid"
)

const (
	// nodeKeyDomain namespaces Noise key bindings signed by the fleet CA. The
	// CA here is not TLS: it is the offline RSA membership key from ca.key, and
	// the signature is plain RSA over bytes, not an X.509 certificate. The
	// prefix keeps such a signature from ever passing as some other signed
	// message.
	nodeKeyDomain = "concord-noise-static-key:"

	// generationWidth is the byte width of the encoded rotation counter: one
	// big-endian uint64, matching binary.BigEndian.AppendUint64 below.
	generationWidth = 8
)

// buildNodeKeyBinding assembles the binding the fleet CA stamps: domain
// prefix, node ID, big-endian generation, static public key. A binding is not
// a Noise protocol message and not a key: it is the document stating which key
// belongs to which node at which generation. All three inputs are local
// identity material, nothing arrives from the network: nodeID is the node's
// stable identity from node config, generation is its key rotation counter (0
// for the first key), and pub is its static public key from EnsureStaticKey.
// Callers hash the binding and sign or verify the hash. SignNodeKey and
// VerifyNodeKey share this builder so the two sides cannot drift apart.
func buildNodeKeyBinding(nodeID uuid.UUID, generation uint64, pub []byte) []byte {
	// One allocation of exactly the final size: prefix plus node ID plus
	// generation plus public key. generationWidth matches what AppendUint64
	// writes below, so the appends never reallocate.
	msg := make([]byte, 0, len(nodeKeyDomain)+len(nodeID)+generationWidth+len(pub))
	msg = append(msg, nodeKeyDomain...)
	msg = append(msg, nodeID[:]...)
	// Big-endian by convention. Sign and verify must use the same order, which
	// holds because both go through this function.
	msg = binary.BigEndian.AppendUint64(msg, generation)
	msg = append(msg, pub...)

	return msg
}

// SignNodeKey binds a node ID and key generation to a Noise static public key
// by signing with the fleet CA private key from ca.key. A generation is the
// key rotation counter: 0 for a node's first static key, plus one on each
// rotation. The highest generation seen for a node wins, so a signature over
// one generation can never authorize another. The output is raw RSA signature
// bytes, not a certificate, and carries no timestamp or validity window. It is
// the node's membership credential for the Noise transport.
//
// VerifyNodeKey checks the same binding against the CA public key. Both sides
// go through buildNodeKeyBinding so the signed bytes can never diverge.
func SignNodeKey(nodeID uuid.UUID, generation uint64, pub []byte) ([]byte, error) {
	if len(pub) != 32 {
		return nil, fmt.Errorf("public key must be 32 bytes") //nolint:perfsprint // plain sentinel, no wrap target
	}

	paths, err := DefaultPaths()
	if err != nil {
		return nil, fmt.Errorf("default paths: %w", err)
	}

	_, caKey, err := loadCA(paths)
	if err != nil {
		return nil, fmt.Errorf("load ca: %w", err)
	}

	msg := buildNodeKeyBinding(nodeID, generation, pub)

	sum := sha256.Sum256(msg)
	sig, err := rsa.SignPKCS1v15(nil, caKey, crypto.SHA256, sum[:])
	if err != nil {
		return nil, fmt.Errorf("sign node key: %w", err)
	}

	return sig, nil
}

// VerifyNodeKey checks that pub belongs to nodeID at generation under a fleet
// CA signature. The caCert argument is the parsed ca.crt, used only as a
// carrier for the CA RSA public key: never X.509 chain rules and never the
// certificate dates. A nil error means the key is a fleet member. Comparing
// generations across competing bindings is the caller's job, not this
// function's.
func VerifyNodeKey(
	caCert *x509.Certificate,
	nodeID uuid.UUID,
	generation uint64,
	pub []byte,
	sig []byte,
) error {
	if len(pub) != 32 {
		return fmt.Errorf("verify node key: public key must be 32 bytes, got %d", len(pub))
	}

	if caCert == nil {
		return fmt.Errorf("verify node key: ca certificate is nil") //nolint:perfsprint // plain sentinel, no wrap target
	}

	rsaPub, ok := caCert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return fmt.Errorf("verify node key: ca key is %T, want *rsa.PublicKey", caCert.PublicKey)
	}

	msg := buildNodeKeyBinding(nodeID, generation, pub)

	sum := sha256.Sum256(msg)
	err := rsa.VerifyPKCS1v15(rsaPub, crypto.SHA256, sum[:], sig)
	if err != nil {
		return fmt.Errorf("verify node key: %w", err)
	}

	return nil
}
