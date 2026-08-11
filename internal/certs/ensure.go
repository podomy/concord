// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/netip"
	"os"

	"github.com/google/uuid"
)

// Ensure returns the default TLS material paths for transport.
//
// Policy:
//  1. If ca.crt, node.crt, and node.key are present and valid → reuse.
//  2. Else if ca.crt and ca.key are present → mint node.crt/node.key under that CA.
//  3. Else → fail (CA must be provisioned: factory, flash drive, etc.).
//
// Normal node bootstrap does not create a CA. createCA/WriteCA are not on this
// path; the operator must already have placed ca.crt and ca.key. WriteCA is
// only for offline provisioning tools and tests.
//
// Reuse does not remint when only advertiseAddress changes.
func Ensure(nodeID uuid.UUID, advertiseAddress netip.Addr) (Paths, error) {
	paths, err := DefaultPaths()
	if err != nil {
		return Paths{}, err
	}

	if err := valid(paths); err == nil {
		return paths, nil
	}

	if err := requireCA(paths); err != nil {
		return Paths{}, err
	}

	if err := MintNode(nodeID, advertiseAddress); err != nil {
		return Paths{}, fmt.Errorf("mint node: %w", err)
	}

	if err := valid(paths); err != nil {
		return Paths{}, fmt.Errorf("after mint node: %w", err)
	}

	return paths, nil
}

// requireCA checks that ca.crt and ca.key exist (operator-provisioned CA).
func requireCA(paths Paths) error {
	for _, p := range []string{paths.CA, paths.CAKey} {
		if _, err := os.Stat(p); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("CA not provisioned: missing %s (place ca.crt and ca.key under the certs directory)", p)
			}
			return fmt.Errorf("stat %s: %w", p, err)
		}
	}
	return nil
}

// present reports whether ca.crt, node.crt, and node.key exist (runtime trio).
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

// valid checks that runtime TLS files form a usable node identity.
//
// Requires ca.crt + node.crt + node.key. ca.key is not required to run
// transport (only to mint new node certs).
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

	// #nosec G304: paths come from DefaultPaths (local config dir).
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
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		return fmt.Errorf("node cert verify: %w", err)
	}
	return nil
}
