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
	"time"
)

// WriteCA creates a new self-signed CA and writes ca.crt and ca.key to the
// default certs directory.
//
// Not used on normal node bootstrap. Concord runtime never creates a CA:
// the operator (factory image, flash drive, offline tool) must place ca.crt
// and ca.key before Ensure runs. WriteCA is only for those provisioning
// tools and tests.
//
// Overwrites existing CA files at those paths.
func WriteCA() error {
	caDER, _, caKey, err := createCA()
	if err != nil {
		return err
	}

	paths, err := DefaultPaths()
	if err != nil {
		return fmt.Errorf("default paths: %w", err)
	}

	if err = writePEM(paths.CA, "CERTIFICATE", caDER); err != nil {
		return fmt.Errorf("write pem: %w", err)
	}
	if err = writePEM(paths.CAKey, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(caKey)); err != nil {
		return fmt.Errorf("write pem: %w", err)
	}
	return nil
}

// loadCA reads operator-provided ca.crt and ca.key from paths.
func loadCA(paths Paths) (*x509.Certificate, *rsa.PrivateKey, error) {
	// #nosec G304: paths come from DefaultPaths (local config dir).
	caPEM, err := os.ReadFile(paths.CA)
	if err != nil {
		return nil, nil, fmt.Errorf("read ca cert: %w", err)
	}
	block, _ := pem.Decode(caPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, nil, fmt.Errorf("decode ca cert: no CERTIFICATE PEM in %s", paths.CA)
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse ca cert: %w", err)
	}

	// #nosec G304: paths come from DefaultPaths (local config dir).
	keyPEM, err := os.ReadFile(paths.CAKey)
	if err != nil {
		return nil, nil, fmt.Errorf("read ca key: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, fmt.Errorf("decode ca key: no PEM in %s", paths.CAKey)
	}
	caKey, err := x509.ParsePKCS1PrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, fmt.Errorf("parse ca key: %w", err)
	}
	return caCert, caKey, nil
}

// createCA generates an RSA key and a self-signed CA certificate.
// Used only by WriteCA (provisioning), never by Ensure or node startup.
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
