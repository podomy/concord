// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

// Mint creates a new local CA and a node certificate signed by that CA, then
// writes them to the default cert paths (ca.crt, node.crt, node.key).
//
// The CA is self-signed. The node cert embeds nodeID in its subject CommonName
// and is usable for both TLS server and client auth (mTLS). The CA private key
// is used only to sign the node cert and is not written to disk; transport
// only needs ca.crt plus this node's cert and key.
//
// Mint overwrites existing files at those paths.
func Mint(nodeID uuid.UUID) error {
	caDER, caCert, caKey, err := createCA()
	if err != nil {
		return err
	}

	nodeDER, nodeKey, err := createNode(nodeID, caCert, caKey)
	if err != nil {
		return err
	}

	paths, err := DefaultPaths()
	if err != nil {
		return fmt.Errorf("default paths: %w", err)
	}

	err = writePEM(paths.CA, "CERTIFICATE", caDER)
	if err != nil {
		return fmt.Errorf("write pem: %w", err)
	}
	err = writePEM(paths.Cert, "CERTIFICATE", nodeDER)
	if err != nil {
		return fmt.Errorf("write pem: %w", err)
	}
	err = writePEM(paths.Key, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(nodeKey))
	if err != nil {
		return fmt.Errorf("write pem: %w", err)
	}

	return nil
}

// createCA generates an RSA key and a self-signed CA certificate.
// Returns the cert DER (for writing ca.crt), the parsed cert (parent for
// signing node certs), and the CA private key (signer only; not persisted).
func createCA() (caDER []byte, caCert *x509.Certificate, caKey *rsa.PrivateKey, err error) {
	caKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("rsa generate ca key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Concord CA"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	caDER, err = x509.CreateCertificate(rand.Reader, template, template, &caKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("create ca certificate: %w", err)
	}
	caCert, err = x509.ParseCertificate(caDER)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse ca certificate: %w", err)
	}
	return caDER, caCert, caKey, nil
}

// createNode generates this node's RSA key and a certificate signed by the CA.
//
// The certificate subject is the node ID. ExtKeyUsage includes both server and
// client auth so the same material works when we listen and when we dial peers.
// CreateCertificate binds the node's public key into the cert and signs with
// the CA private key (issuer signs, subject is the node).
func createNode(nodeID uuid.UUID, caCert *x509.Certificate, caKey *rsa.PrivateKey) ([]byte, *rsa.PrivateKey, error) {
	nodeKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, fmt.Errorf("rsa generate node key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "Concord Node: " + nodeID.String()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}

	nodeDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &nodeKey.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create node certificate: %w", err)
	}
	return nodeDER, nodeKey, nil
}

// writePEM writes der as a single PEM block of the given type to path.
// Files are created or truncated with mode 0600 (owner read/write only).
func writePEM(path, blockType string, der []byte) (err error) {
	f, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("close %s: %w; original error: %w", path, closeErr, err)
				return
			}
			err = fmt.Errorf("close %s: %w", path, closeErr)
		}
	}()
	if err = pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		return fmt.Errorf("encode %s: %w", path, err)
	}
	return nil
}
