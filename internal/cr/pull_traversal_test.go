// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"archive/tar"
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Symlink components in the file path must block extraction.
func TestExtractFileRejectsSymlinkPrefix(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "real")
	err := os.MkdirAll(target, 0o750)
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink("real", filepath.Join(root, "link"))
	if err != nil {
		t.Fatal(err)
	}
	hdr := &tar.Header{Name: "link/evil.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: 4}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err = tw.WriteHeader(hdr)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tw.Write([]byte("evil"))
	if err != nil {
		t.Fatal(err)
	}
	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(&buf)
	_, err = tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	err = extractFile(context.Background(), tr, filepath.Join(root, "link", "evil.txt"), hdr, root)
	if err == nil {
		t.Fatal("expected error for symlink prefix")
	}
	if !strings.Contains(err.Error(), "traverses symlink") {
		t.Fatalf("error = %q, want symlink traversal", err.Error())
	}
	_, err = os.Stat(filepath.Join(target, "evil.txt"))
	if err == nil {
		t.Fatal("file was written through symlink")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

// Symlink components in the dir path must block extraction.
func TestExtractDirRejectsSymlinkPrefix(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	target := filepath.Join(root, "real")
	err := os.MkdirAll(target, 0o750)
	if err != nil {
		t.Fatal(err)
	}
	err = os.Symlink("real", filepath.Join(root, "link"))
	if err != nil {
		t.Fatal(err)
	}
	hdr := &tar.Header{Name: "link/subdir", Typeflag: tar.TypeDir, Mode: 0o755}
	err = extractDir(filepath.Join(root, "link", "subdir"), hdr, root)
	if err == nil {
		t.Fatal("expected error for symlink prefix")
	}
	_, err = os.Stat(filepath.Join(target, "subdir"))
	if err == nil {
		t.Fatal("dir was created through symlink")
	} else if !os.IsNotExist(err) {
		t.Fatal(err)
	}
}

// End to end tar with symlink plus file through it must fail closed.
func TestExtractTarRejectsSymlinkTraversal(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := tw.WriteHeader(&tar.Header{Name: "real", Typeflag: tar.TypeDir, Mode: 0o755})
	if err != nil {
		t.Fatal(err)
	}
	err = tw.WriteHeader(&tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "real"})
	if err != nil {
		t.Fatal(err)
	}
	err = tw.WriteHeader(&tar.Header{Name: "link/evil.txt", Typeflag: tar.TypeReg, Mode: 0o644, Size: 4})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tw.Write([]byte("evil"))
	if err != nil {
		t.Fatal(err)
	}
	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = extractTar(context.Background(), &buf, root)
	if err == nil {
		t.Fatal("expected error for symlink traversal")
	}
	_, statErr := os.Stat(filepath.Join(root, "real", "evil.txt"))
	if statErr == nil {
		t.Fatal("file was written through symlink")
	}
}

// A blob with a bad digest must fail before any bytes reach disk.
func TestExtractBlobStreamVerifiesBeforeWrite(t *testing.T) {
	t.Parallel()

	tarBytes := makeTarFile(t, "pwned.txt", []byte("evil"))
	badDigest := testDigest([]byte("something else"))
	rootfs := filepath.Join(t.TempDir(), "rootfs")
	err := os.MkdirAll(rootfs, 0o750)
	if err != nil {
		t.Fatal(err)
	}
	err = extractBlobStream(context.Background(), bytes.NewReader(tarBytes), badDigest, rootfs)
	if err == nil {
		t.Fatal("expected digest mismatch")
	}
	_, statErr := os.Stat(filepath.Join(rootfs, "pwned.txt"))
	if statErr == nil {
		t.Fatal("file was written before digest check")
	} else if !os.IsNotExist(statErr) {
		t.Fatal(statErr)
	}

	goodDigest := testDigest(tarBytes)
	rootfs2 := filepath.Join(t.TempDir(), "rootfs")
	err = os.MkdirAll(rootfs2, 0o750)
	if err != nil {
		t.Fatal(err)
	}
	err = extractBlobStream(context.Background(), bytes.NewReader(tarBytes), goodDigest, rootfs2)
	if err != nil {
		t.Fatalf("valid blob rejected: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(rootfs2, "pwned.txt")) //nolint:gosec // test reads a file it just wrote
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "evil" {
		t.Fatalf("content = %q, want evil", got)
	}
}

// Gzip blobs follow the same verify before write rule.
func TestExtractBlobStreamGzipVerifiesBeforeWrite(t *testing.T) {
	t.Parallel()

	tarBytes := makeTarFile(t, "pwned.txt", []byte("gzip-evil"))
	layer := gzipBytes(t, tarBytes)
	badDigest := testDigest([]byte("other"))
	rootfs := filepath.Join(t.TempDir(), "rootfs")
	err := os.MkdirAll(rootfs, 0o750)
	if err != nil {
		t.Fatal(err)
	}
	err = extractBlobStream(context.Background(), bytes.NewReader(layer), badDigest, rootfs)
	if err == nil {
		t.Fatal("expected digest mismatch")
	}
	_, statErr := os.Stat(filepath.Join(rootfs, "pwned.txt"))
	if statErr == nil {
		t.Fatal("gzip file was written before digest check")
	} else if !os.IsNotExist(statErr) {
		t.Fatal(statErr)
	}
}

// Hardlink escape must fail closed.
func TestExtractHardLinkRejectsEscape(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	hdr := &tar.Header{Name: "link.txt", Typeflag: tar.TypeLink, Linkname: "../escape.txt"}
	err := extractHardLink(filepath.Join(root, "link.txt"), hdr, root)
	if err == nil {
		t.Fatal("expected error for escaping hardlink")
	}
	if !strings.Contains(err.Error(), "escapes root") {
		t.Fatalf("error = %q, want escapes root", err.Error())
	}
}

// Missing hardlink target must fail instead of skipping silently.
func TestExtractHardLinkRejectsMissing(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	err := os.MkdirAll(root, 0o750)
	if err != nil {
		t.Fatal(err)
	}
	hdr := &tar.Header{Name: "link.txt", Typeflag: tar.TypeLink, Linkname: "missing.txt"}
	err = extractHardLink(filepath.Join(root, "link.txt"), hdr, root)
	if err == nil {
		t.Fatal("expected error for missing hardlink target")
	}
	_, statErr := os.Lstat(filepath.Join(root, "link.txt"))
	if statErr == nil {
		t.Fatal("link was created for missing target")
	}
}

// Valid hardlinks inside rootfs succeed with shared content.
func TestExtractHardLinkValid(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	err := os.WriteFile(filepath.Join(root, "target.txt"), []byte("shared"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	hdr := &tar.Header{Name: "link.txt", Typeflag: tar.TypeLink, Linkname: "target.txt"}
	err = extractHardLink(filepath.Join(root, "link.txt"), hdr, root)
	if err != nil {
		t.Fatalf("valid hardlink rejected: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(root, "link.txt")) //nolint:gosec // test reads a file it just wrote
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "shared" {
		t.Fatalf("content = %q, want shared", got)
	}
	a, err := os.Stat(filepath.Join(root, "target.txt"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.Stat(filepath.Join(root, "link.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(a, b) {
		t.Fatal("hardlink does not share inode with target")
	}
}

// Setuid, setgid and sticky bits must be dropped from files.
func TestExtractFileMasksSpecialBits(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	fileHdr := &tar.Header{Name: "prog", Typeflag: tar.TypeReg, Mode: 0o4755}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	err := tw.WriteHeader(&tar.Header{Name: "prog", Typeflag: tar.TypeReg, Mode: 0o4755, Size: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = tw.Write([]byte("hi"))
	if err != nil {
		t.Fatal(err)
	}
	err = tw.Close()
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(&buf)
	_, err = tr.Next()
	if err != nil {
		t.Fatal(err)
	}
	err = extractFile(context.Background(), tr, filepath.Join(root, "prog"), fileHdr, root)
	if err != nil {
		t.Fatalf("extract file: %v", err)
	}
	fi, err := os.Stat(filepath.Join(root, "prog"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o755 {
		t.Fatalf("file perm = %o, want 755", fi.Mode().Perm())
	}
	if fi.Mode()&os.ModeSetuid != 0 || fi.Mode()&os.ModeSetgid != 0 || fi.Mode()&os.ModeSticky != 0 {
		t.Fatalf("file mode keeps special bits: %v", fi.Mode())
	}
}

// Setuid, setgid and sticky bits must be dropped from dirs.
func TestExtractDirMasksSpecialBits(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dirHdr := &tar.Header{Name: "sub", Typeflag: tar.TypeDir, Mode: 0o2755}
	err := extractDir(filepath.Join(root, "sub"), dirHdr, root)
	if err != nil {
		t.Fatalf("extract dir: %v", err)
	}
	di, err := os.Stat(filepath.Join(root, "sub"))
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode()&os.ModeSetuid != 0 || di.Mode()&os.ModeSetgid != 0 || di.Mode()&os.ModeSticky != 0 {
		t.Fatalf("dir mode keeps special bits: %v", di.Mode())
	}
}

// Empty whiteout name must be a no-op and must not clear the parent.
func TestExtractWhiteoutEmptySkipped(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dir := filepath.Join(root, "a")
	err := os.MkdirAll(dir, 0o750)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("keep"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	hdr := &tar.Header{Name: "a/.wh.", Typeflag: tar.TypeReg}
	err = extractEntry(context.Background(), nil, hdr, root)
	if err != nil {
		t.Fatalf("empty whiteout rejected: %v", err)
	}
	_, err = os.Stat(filepath.Join(dir, "keep.txt"))
	if err != nil {
		t.Fatalf("empty whiteout removed parent: %v", err)
	}
}
