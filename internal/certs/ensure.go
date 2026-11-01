// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs

import (
	"crypto/rsa"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	"github.com/podomy/concord/internal/clock"
)

// Ensure returns the default CA paths for transport after checking the
// operator-provisioned CA material exists, parses, is currently valid, and
// matches its private key. Failures are fatal (fail closed): transport must
// never run on an expired or mismatched trust anchor.
//
// Policy: ca.crt and ca.key must already be on disk (factory, flash drive,
// etc.). No certificates are minted here; node identity lives in the Noise
// static key, not in node.crt. Normal node bootstrap never creates a CA:
// createCA/WriteCA are not on this path. WriteCA is only for offline
// provisioning tools and tests.
//
// Revocation and rotation have no infrastructure in this round (no CRL, no
// reissue path). The accepted mechanism is generation pinning: the highest
// NoiseGeneration seen for a node wins, so a rotated key supersedes the old
// binding without a revocation list. See SignNodeKey/VerifyNodeKey.
func Ensure() (Paths, error) {
	paths, err := DefaultPaths()
	if err != nil {
		return Paths{}, err
	}

	if err := requireCA(paths); err != nil {
		return Paths{}, err
	}

	caCert, caKey, err := loadCA(paths)
	if err != nil {
		return Paths{}, err
	}
	if err := validateCA(caCert, caKey, clock.Now()); err != nil {
		return Paths{}, err
	}

	return paths, nil
}

// requireCA checks that ca.crt and ca.key exist (operator-provisioned CA)
// and that ca.key is mode 0600 or stricter. Fail closed on group or other
// access, matching gossip key handling in peerdiscovery.
func requireCA(paths Paths) error {
	if _, err := os.Stat(paths.CA); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("CA not provisioned: missing %s (place ca.crt and ca.key under the certs directory)", paths.CA)
		}
		return fmt.Errorf("stat %s: %w", paths.CA, err)
	}
	info, err := os.Stat(paths.CAKey)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("CA not provisioned: missing %s (place ca.crt and ca.key under the certs directory)", paths.CAKey)
		}
		return fmt.Errorf("stat %s: %w", paths.CAKey, err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf(
			"refusing ca key %s with mode %04o: want 0600 (group and others must have no access)",
			paths.CAKey,
			perm,
		)
	}
	return nil
}

// validateCA fails closed when the CA certificate is outside its validity
// window at now or the private key does not match the certificate public
// key. Transport must never start on an expired or mismatched anchor.
func validateCA(caCert *x509.Certificate, caKey *rsa.PrivateKey, now time.Time) error {
	if now.Before(caCert.NotBefore) {
		return fmt.Errorf(
			"ca certificate not valid before %s (now %s)",
			caCert.NotBefore.UTC().Format(time.RFC3339),
			now.UTC().Format(time.RFC3339),
		)
	}
	if now.After(caCert.NotAfter) {
		return fmt.Errorf(
			"ca certificate expired %s (now %s)",
			caCert.NotAfter.UTC().Format(time.RFC3339),
			now.UTC().Format(time.RFC3339),
		)
	}
	rsaPub, ok := caCert.PublicKey.(*rsa.PublicKey)
	if !ok {
		return fmt.Errorf("ca certificate key is %T, want *rsa.PublicKey", caCert.PublicKey)
	}
	if caKey.N.Cmp(rsaPub.N) != 0 || caKey.E != rsaPub.E {
		return fmt.Errorf("ca key does not match ca certificate") //nolint:perfsprint // plain sentinel, no wrap target
	}
	return nil
}
