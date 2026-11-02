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

	"github.com/podomy/concord/internal/clock"
)

// WriteCA creates a new self-signed CA and writes ca.crt and ca.key to the
// default certs directory.
//
// Not used on normal node bootstrap. Concord runtime never creates a CA:
// the operator (factory image, flash drive, offline tool) must place ca.crt
// and ca.key before Ensure runs. WriteCA is only for those provisioning
// tools and tests.
//
// Overwriting is never silent: existing files are copied to <name>.bak with
// mode 0600 first, and a warning naming the backup goes to stderr. The
// generation still supersedes: there is no revocation list, the new CA
// simply replaces the old binding (see Ensure).
func WriteCA() error {
	caDER, _, caKey, err := createCA()
	if err != nil {
		return fmt.Errorf("generate CA: %w", err)
	}

	paths, err := DefaultPaths()
	if err != nil {
		return fmt.Errorf("default paths: %w", err)
	}

	err = backupCAFile(paths.CA)
	if err != nil {
		return fmt.Errorf("backup CA cert: %w", err)
	}
	err = backupCAFile(paths.CAKey)
	if err != nil {
		return fmt.Errorf("backup CA key: %w", err)
	}
	if err = writePEM(paths.CA, "CERTIFICATE", caDER); err != nil {
		return fmt.Errorf("write pem: %w", err)
	}
	if err = writePEM(paths.CAKey, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(caKey)); err != nil {
		return fmt.Errorf("write pem: %w", err)
	}
	return nil
}

// backupCAFile copies an existing file at path to path plus ".bak" with
// mode 0600 and warns on stderr naming the backup. A missing file is not
// an error: first provisioning has nothing to keep. WriteCA has no logger,
// so stderr carries the warning; provisioning runs are rare and the
// message must survive any log configuration.
func backupCAFile(path string) error {
	// #nosec G304: paths come from DefaultPaths (local config dir).
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read existing %s: %w", path, err)
	}
	bak := path + ".bak"
	// #nosec G703: bak appends a fixed suffix to a DefaultPaths entry (local config dir).
	if err := os.WriteFile(bak, raw, 0o600); err != nil {
		return fmt.Errorf("back up %s to %s: %w", path, bak, err)
	}
	// WriteFile applies the mode only on creation; an older backup with
	// wider permissions stays tight.
	if err := os.Chmod(bak, 0o600); err != nil {
		return fmt.Errorf("chmod backup %s: %w", bak, err)
	}
	fmt.Fprintf(os.Stderr, "warning: WriteCA replaced %s; previous copy kept at %s (mode 0600)\n", path, bak) //nolint:errcheck // best-effort stderr warning, WriteCA has no logger
	return nil
}

// loadCA reads operator-provided ca.crt and ca.key from paths.
func loadCA(paths Paths) (*x509.Certificate, *rsa.PrivateKey, error) {
	caCert, err := loadCACert(paths.CA)
	if err != nil {
		return nil, nil, err
	}

	caKey, err := loadCAKey(paths.CAKey)
	if err != nil {
		return nil, nil, err
	}

	return caCert, caKey, nil
}

// loadCACert reads and parses the CA certificate at path.
func loadCACert(path string) (*x509.Certificate, error) {
	// #nosec G304: paths come from DefaultPaths (local config dir).
	caPEM, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca cert: %w", err)
	}
	block, _ := pem.Decode(caPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("decode ca cert: no CERTIFICATE PEM in %s", path)
	}
	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse ca cert: %w", err)
	}
	return caCert, nil
}

// loadCAKey reads and parses the CA private key at path.
func loadCAKey(path string) (*rsa.PrivateKey, error) {
	// #nosec G304: paths come from DefaultPaths (local config dir).
	keyPEM, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ca key: %w", err)
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, fmt.Errorf("decode ca key: no PEM in %s", path)
	}
	caKey, err := parseCAKey(keyBlock, path)
	if err != nil {
		return nil, err
	}
	return caKey, nil
}

// parseCAKey parses a decoded CA key block in PKCS1 or PKCS8.
func parseCAKey(block *pem.Block, path string) (*rsa.PrivateKey, error) {
	switch block.Type {
	case "RSA PRIVATE KEY":
		return parsePKCS1RSAKey(block.Bytes)
	case "PRIVATE KEY":
		return parsePKCS8RSAKey(block.Bytes)
	default:
		return nil, fmt.Errorf("decode ca key: unsupported PEM type %q in %s", block.Type, path)
	}
}

// parsePKCS1RSAKey parses DER as PKCS1 RSA.
func parsePKCS1RSAKey(der []byte) (*rsa.PrivateKey, error) {
	caKey, err := x509.ParsePKCS1PrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS#1 ca key: %w", err)
	}
	return caKey, nil
}

// parsePKCS8RSAKey parses DER as PKCS8 and requires RSA.
func parsePKCS8RSAKey(der []byte) (*rsa.PrivateKey, error) {
	keyAny, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS#8 ca key: %w", err)
	}
	caKey, ok := keyAny.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("parse ca key: PKCS#8 key is not an RSA private key") //nolint:perfsprint // plain sentinel, no wrap target
	}
	return caKey, nil
}

// createCA generates an RSA key and a self-signed CA certificate with a
// random 128-bit serial. Fixed serials (such as 1) repeat across
// independently provisioned fleets; 128 random bits fit every X.509
// implementation cap (20 octets).
// Used only by WriteCA (provisioning), never by Ensure or node startup.
func createCA() (caDER []byte, caCert *x509.Certificate, caKey *rsa.PrivateKey, err error) {
	caKey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("rsa generate ca key: %w", err)
	}

	serialLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serial, err := rand.Int(rand.Reader, serialLimit)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("random ca serial: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Concord CA"},
		NotBefore:             clock.Now().Add(-time.Minute),
		NotAfter:              clock.Now().Add(10 * 365 * 24 * time.Hour),
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
