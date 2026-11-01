// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs_test

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/podomy/concord/internal/certs"
)

// Expired CA material fails closed instead of serving transport.
func TestEnsureFailsWithExpiredCA(t *testing.T) {
	setupCA(t)

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	expired := &x509.Certificate{
		SerialNumber:          big.NewInt(7),
		Subject:               pkix.Name{CommonName: "Concord CA"},
		NotBefore:             time.Now().Add(-2 * time.Hour),
		NotAfter:              time.Now().Add(-time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, expired, expired, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	paths, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	writePEMFile(t, paths.CA, "CERTIFICATE", der, 0o644)
	writePEMFile(t, paths.CAKey, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key), 0o600)

	if _, err := certs.Ensure(); err == nil {
		t.Fatal("expected error with expired ca")
	}
}

// A key that does not match the certificate fails closed.
func TestEnsureFailsWithMismatchedKey(t *testing.T) {
	setupCA(t)

	paths, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	writePEMFile(t, paths.CAKey, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(other), 0o600)

	if _, err := certs.Ensure(); err == nil {
		t.Fatal("expected error with mismatched ca key")
	}
}

// A group-readable ca.key fails closed, matching gossip key handling.
func TestEnsureFailsWithPermissiveKeyMode(t *testing.T) {
	setupCA(t)

	paths, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// #nosec G302: deliberately permissive to test fail-closed key handling.
	if err := os.Chmod(paths.CAKey, 0o640); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if _, err := certs.Ensure(); err == nil {
		t.Fatal("expected error with permissive ca key mode")
	}
}

// Successive CAs carry distinct random 128-bit serials, never the fixed 1.
func TestWriteCASerialsAreRandom(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	first := readCASerial(t)
	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	second := readCASerial(t)

	if first.Cmp(second) == 0 {
		t.Fatal("expected distinct ca serials")
	}
	for i, s := range []*big.Int{first, second} {
		if s.Sign() <= 0 || s.BitLen() > 128 {
			t.Fatalf("serial %d out of 128-bit range: %v", i, s)
		}
	}
}

// readCASerial parses the serial of the current ca.crt under the test env.
func readCASerial(t *testing.T) *big.Int {
	t.Helper()

	paths, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// #nosec G304: test reads its own tempdir path.
	raw, err := os.ReadFile(paths.CA)
	if err != nil {
		t.Fatalf("read ca: %v", err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatal("no PEM in ca.crt")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return cert.SerialNumber
}

// writePEMFile writes one PEM block for CA tampering tests.
func writePEMFile(t *testing.T, path, blockType string, der []byte, mode os.FileMode) {
	t.Helper()

	data := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der})
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
