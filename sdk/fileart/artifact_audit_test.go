// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package fileart

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestAuditMaterializeBlocksSymlinkPrefix covers symlink escape via a
// planted dir symlink inside rootfs. The fetch must never run.
func TestAuditMaterializeBlocksSymlinkPrefix(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outside := t.TempDir()
	link := filepath.Join(root, "link")
	err := os.Symlink(outside, link)
	if err != nil {
		t.Fatal(err)
	}
	called := false
	fetch := func(_ context.Context, _, _ string) error {
		called = true
		return nil
	}
	files := []File{{Path: "/link/evil", Digest: "sha256:abc"}}
	err = Materialize(t.Context(), root, files, fetch)
	if err == nil {
		t.Fatal("Materialize through symlink succeeded, want error")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error = %q, want symlink mention", err)
	}
	if called {
		t.Fatal("fetch ran despite symlink prefix")
	}
}

// TestAuditMaterializeRejectsOutsideRoot covers escape attempts that
// must fail before any fetch, including unclean paths and a dest that
// is itself a symlink.
func TestAuditMaterializeRejectsOutsideRoot(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	pristine := func(_ context.Context, _, _ string) error {
		t.Error("fetch must not run after validation failure")
		return nil
	}
	for _, path := range []string{"/../escape", "/a/../../escape", "/etc/./dots"} {
		files := []File{{Path: path, Digest: "sha256:abc"}}
		if err := Materialize(ctx, t.TempDir(), files, pristine); err == nil {
			t.Errorf("Materialize(%q) = nil, want error", path)
		}
	}

	root := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	err := os.WriteFile(target, []byte("x"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink(target, filepath.Join(root, "linked"))
	if err != nil {
		t.Fatal(err)
	}
	files := []File{{Path: "/linked", Digest: "sha256:abc"}}
	if err := Materialize(ctx, root, files, pristine); err == nil {
		t.Error("Materialize(symlink dest) = nil, want error")
	}
}
