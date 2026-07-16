// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package certs manages on-disk TLS material for Concord nodes.
//
// Same UserConfigDir/concord layout as node config, but under concord/certs/
// with three files (ca.crt, node.crt, node.key). That is why we expose Dir()
// and DefaultPaths() instead of a single file path.
package certs

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	caFileName   = "ca.crt"
	certFileName = "node.crt"
	keyFileName  = "node.key"
)

// Paths holds the on-disk locations for this node's TLS material.
//
//	CA   - PEM certificate of the CA that signed Cert (trust anchor)
//	Cert - this node's PEM certificate (presented in mTLS both ways)
//	Key  - this node's PEM private key (pairs with Cert)
type Paths struct {
	CA   string
	Cert string
	Key  string
}

// Dir returns the auto-determined directory for Concord TLS material
// (~/.config/concord/certs). The directory is created if missing (mode 0700).
// Same UserConfigDir root as node config; certs use a subdirectory because
// three files live there rather than one.
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

// DefaultPaths returns the auto-determined paths for ca.crt, node.crt, and
// node.key under Dir(). Paths are fixed by convention; callers cannot override
// them (same idea as node.LoadOrCreateNodeConfig and its config path).
func DefaultPaths() (Paths, error) {
	dir, err := Dir()
	if err != nil {
		return Paths{}, err
	}

	return Paths{
		CA:   filepath.Join(dir, caFileName),
		Cert: filepath.Join(dir, certFileName),
		Key:  filepath.Join(dir, keyFileName),
	}, nil
}
