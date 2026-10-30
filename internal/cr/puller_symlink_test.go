// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"archive/tar"
	"context"
	"os"
	"path/filepath"
	"testing"
)

// Absolute symlink targets must not escape the rootfs.
func TestExtractSymlinkRejectsAbsolute(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	hdr := &tar.Header{Name: "link", Linkname: "/etc/passwd"}
	err := extractSymlink(filepath.Join(root, "link"), hdr, root)
	if err == nil {
		t.Fatal("expected error for absolute target")
	}
	if _, statErr := os.Lstat(filepath.Join(root, "link")); !os.IsNotExist(statErr) {
		t.Fatal("absolute symlink was created")
	}
}

// Targets resolving outside the rootfs must be rejected.
func TestExtractSymlinkRejectsEscape(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cases := []struct {
		name   string
		link   string
		target string
	}{
		{name: "top", link: "link", target: "../escape"},
		{name: "nested", link: "a/link", target: "../../escape"},
	}
	for _, tc := range cases {
		linkPath := filepath.Join(root, filepath.FromSlash(tc.link))
		hdr := &tar.Header{Name: tc.link, Linkname: tc.target}
		err := extractSymlink(linkPath, hdr, root)
		if err == nil {
			t.Errorf("%s: expected error for target %q", tc.name, tc.target)
		}
	}
	if _, statErr := os.Lstat(filepath.Join(root, "link")); !os.IsNotExist(statErr) {
		t.Fatal("escaping symlink was created")
	}
	if _, statErr := os.Lstat(filepath.Join(root, "a", "link")); !os.IsNotExist(statErr) {
		t.Fatal("escaping symlink was created")
	}
}

// Valid relative targets inside the rootfs are preserved.
func TestExtractSymlinkAllowsRelative(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	linkPath := filepath.Join(root, "sub", "link")
	hdr := &tar.Header{Name: "sub/link", Linkname: "../other.txt"}
	err := extractSymlink(linkPath, hdr, root)
	if err != nil {
		t.Fatalf("valid relative symlink rejected: %v", err)
	}
	got, err := os.Readlink(linkPath)
	if err != nil {
		t.Fatal(err)
	}
	if got != "../other.txt" {
		t.Fatalf("target = %q, want %q", got, "../other.txt")
	}
}

// End to end entry handling rejects absolute symlink targets.
func TestExtractEntryRejectsAbsoluteSymlink(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	hdr := &tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "/etc/passwd"}
	err := extractEntry(context.Background(), nil, hdr, root)
	if err == nil {
		t.Fatal("expected error for absolute symlink entry")
	}
}
