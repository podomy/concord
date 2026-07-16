// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/google/uuid"
)

// Ensure returns the default TLS material paths, creating them if needed.
//
// If ca.crt, node.crt, and node.key are present and pass validation, they are
// reused. Otherwise Mint is called with nodeID and the result is validated
// again. Callers pass the returned Paths into transport TLS loading.
//
// Invalid material is replaced (dev-friendly remint). A production deployment
// that must not rotate the CA silently may want a stricter policy later.
func Ensure(nodeID uuid.UUID) (Paths, error) {
	paths, err := DefaultPaths()
	if err != nil {
		return Paths{}, err
	}

	if err := valid(paths); err == nil {
		return paths, nil
	}
	if err := Mint(nodeID); err != nil {
		return Paths{}, fmt.Errorf("mint: %w", err)
	}
	if err := valid(paths); err != nil {
		return Paths{}, fmt.Errorf("after mint: %w", err)
	}
	return paths, nil
}

// present reports whether all three TLS files exist on disk.
// Missing files and I/O errors are returned as errors; nil means all present.
func present(paths Paths) error {
	for _, p := range []string{paths.CA, paths.Cert, paths.Key} {
		if _, err := os.Stat(p); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("missing %s", p)
			}
			return fmt.Errorf("stat %s: %w", p, err)
		}
	}
	return nil
}

// valid checks that the TLS files exist and form a usable node identity.
//
// Checks: files present; node cert and key load as a pair; CA PEM parses into
// a trust pool; leaf verifies against that CA (signature chain and expiry).
// Failures mean Ensure should mint (or the operator should fix the files).
func valid(paths Paths) error {
	if err := present(paths); err != nil {
		return err
	}

	cert, err := tls.LoadX509KeyPair(paths.Cert, paths.Key)
	if err != nil {
		return fmt.Errorf("node cert/key: %w", err)
	}
	if len(cert.Certificate) == 0 {
		return fmt.Errorf("node cert empty") //nolint:perfsprint // plain error, no wrap target
	}

	caPEM, err := os.ReadFile(paths.CA)
	if err != nil {
		return fmt.Errorf("read ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("ca: no certificates") //nolint:perfsprint // plain error, no wrap target
	}

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("parse node cert: %w", err)
	}
	// Verify checks signature chain and expiry (default EKU: server auth).
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		return fmt.Errorf("node cert verify: %w", err)
	}
	return nil
}
