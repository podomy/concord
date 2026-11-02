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
	"github.com/podomy/concord/internal/logs"
)

// expiryWarnThreshold sets the startup warning horizon: a CA expiring
// within 30 days logs "ca certificate expiring soon" with its expiry
// date. Thirty days covers one monthly maintenance visit with margin;
// past expiry Ensure fails closed, so the warning is the only notice.
const expiryWarnThreshold = 30 * 24 * time.Hour

// Ensure returns the default CA paths for transport after checking the
// operator-provisioned CA material exists, parses, is currently valid, and
// matches its private key. Failures are fatal (fail closed): transport must
// never run on an expired or mismatched trust anchor.
//
// Policy: ca.crt and ca.key must already be on disk (factory, flash drive,
// etc.). No certificates are minted here; node identity lives in the Noise
// static key, not in node.crt. Normal node bootstrap never creates a CA:
// createCA/WriteCA are not on this path.
//
// CA expiry and rotation runbook (manual in this round: no CRL, no reissue
// path). Detect: startup logs "ca certificate expiring soon" within 30 days
// of NotAfter (see LogExpiryWarning and EnsureWithLogger), or Ensure fails
// closed past expiry. Rotate: mint a fresh CA offline with the WriteCA
// provisioning tooling, place the same ca.crt and ca.key on every node
// (mode 0600), restart the fleet. Node identity rides the Noise static key,
// not node.crt: after a CA swap, rotate each node's key (bump
// NoiseGeneration) so the new CA signs the binding; the highest generation
// seen wins, which supersedes the old binding without a revocation list.
// See SignNodeKey and VerifyNodeKey.
func Ensure() (Paths, error) {
	paths, _, err := ensure()
	if err != nil {
		return Paths{}, err
	}

	return paths, nil
}

// EnsureWithLogger is Ensure plus a startup expiry warning: when the CA
// expires within expiryWarnThreshold it logs a warning carrying the expiry
// date, then returns the same paths. Startup paths that have a logger
// should prefer it; Ensure stays for logger-less callers (tests, tools).
func EnsureWithLogger(logger *logs.Logger) (Paths, error) {
	paths, caCert, err := ensure()
	if err != nil {
		return Paths{}, err
	}
	LogExpiryWarning(logger, caCert, clock.Now())

	return paths, nil
}

// LogExpiryWarning logs a warning carrying the expiry date when caCert
// expires within expiryWarnThreshold of now. A nil logger or nil cert
// drops it. Expired certs are not warnings: Ensure already fails closed
// on those, so this only fires while renewal is still possible.
func LogExpiryWarning(logger *logs.Logger, caCert *x509.Certificate, now time.Time) {
	if logger == nil || caCert == nil {
		return
	}
	daysLeft, warn := expiringSoon(caCert, now)
	if !warn {
		return
	}
	logger.Warn(
		"ca certificate expiring soon",
		logs.String("not_after", caCert.NotAfter.UTC().Format(time.RFC3339)),
		logs.Int("days_left", daysLeft),
	)
}

// expiringSoon reports whole days until cert expires at now, and whether
// that falls inside the warning horizon. Past expiry is not "soon": it is
// Ensure's fail-closed error, not a warning.
func expiringSoon(caCert *x509.Certificate, now time.Time) (int, bool) {
	left := caCert.NotAfter.Sub(now)
	if left < 0 || left >= expiryWarnThreshold {
		return int(left.Hours() / 24), false
	}
	return int(left.Hours() / 24), true
}

// ensure is the shared core behind Ensure and EnsureWithLogger: it checks
// the operator-provisioned CA material and returns the parsed certificate
// for the expiry warning.
func ensure() (Paths, *x509.Certificate, error) {
	paths, err := DefaultPaths()
	if err != nil {
		return Paths{}, nil, err
	}

	if err := requireCA(paths); err != nil {
		return Paths{}, nil, err
	}

	caCert, caKey, err := loadCA(paths)
	if err != nil {
		return Paths{}, nil, err
	}
	if err := validateCA(caCert, caKey, clock.Now()); err != nil {
		return Paths{}, nil, err
	}

	return paths, caCert, nil
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
