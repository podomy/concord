// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs_test

import (
	"os"
	"testing"

	"github.com/podomy/concord/internal/certs"
)

// A second WriteCA keeps the superseded files at <name>.bak with mode
// 0600 instead of silently overwriting them.
func TestWriteCABacksUpExisting(t *testing.T) {
	setupCA(t)

	paths, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	// #nosec G304: test reads its own tempdir paths.
	oldCRT, err := os.ReadFile(paths.CA)
	if err != nil {
		t.Fatalf("read ca: %v", err)
	}
	// #nosec G304: test reads its own tempdir paths.
	oldKey, err := os.ReadFile(paths.CAKey)
	if err != nil {
		t.Fatalf("read ca key: %v", err)
	}

	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	for _, tc := range []struct {
		path string
		old  []byte
	}{
		{paths.CA, oldCRT},
		{paths.CAKey, oldKey},
	} {
		bak := tc.path + ".bak"
		// #nosec G304: test reads its own tempdir paths.
		raw, err := os.ReadFile(bak)
		if err != nil {
			t.Fatalf("read backup %s: %v", bak, err)
		}
		if string(raw) != string(tc.old) {
			t.Fatalf("backup %s does not match superseded file", bak)
		}
		info, err := os.Stat(bak)
		if err != nil {
			t.Fatalf("stat backup %s: %v", bak, err)
		}
		if perm := info.Mode().Perm(); perm&0o077 != 0 {
			t.Fatalf("backup %s mode %04o, want 0600", bak, perm)
		}
	}
}

// First provisioning has nothing to keep: no .bak files appear.
func TestWriteCAFirstRunNoBackup(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write ca: %v", err)
	}

	paths, err := certs.Ensure()
	if err != nil {
		t.Fatalf("ensure: %v", err)
	}
	for _, bak := range []string{paths.CA + ".bak", paths.CAKey + ".bak"} {
		if _, err := os.Stat(bak); !os.IsNotExist(err) {
			t.Fatalf("backup %s exists on first run: %v", bak, err)
		}
	}
}
