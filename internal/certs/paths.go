// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package certs manages on-disk CA material for Concord nodes.
//
// Same UserConfigDir/concord layout as node config, under concord/certs/:
//
// ca.crt  - fleet trust anchor (operator-provided; required before start).
// ca.key  - CA private key (operator-provided; used only to sign node parcels).
//
// Node identity lives in the Noise static key under concord/noise/, not here.
// Normal bootstrap never creates a CA. The operator supplies ca.crt and ca.key
// (factory, flash drive, etc.). Ensure only checks their presence. WriteCA is
// for provisioning tools, not the running node.
//
// Dir() and DefaultPaths() expose the directory and fixed file names.
package certs

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	caFileName    = "ca.crt"
	caKeyFileName = "ca.key"
)

// Paths holds the on-disk locations for CA material.
//
// CA    - PEM CA certificate (trust anchor).
// CAKey - PEM CA private key (signs node parcels; not used by transport).
type Paths struct {
	CA    string
	CAKey string
}

// Dir returns the auto-determined directory for Concord TLS material
// (~/.config/concord/certs). The directory is created if missing (mode 0700).
func Dir() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	certsDir := filepath.Join(dir, "concord", "certs")
	if err := os.MkdirAll(certsDir, 0o700); err != nil {
		return "", fmt.Errorf("create certs directory: %w", err)
	}

	return certsDir, nil
}

// DefaultPaths returns fixed paths for ca.crt and ca.key under Dir().
// Callers cannot override them.
func DefaultPaths() (Paths, error) {
	dir, err := Dir()
	if err != nil {
		return Paths{}, err
	}

	return Paths{
		CA:    filepath.Join(dir, caFileName),
		CAKey: filepath.Join(dir, caKeyFileName),
	}, nil
}
