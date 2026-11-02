// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"crypto/sha256"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// assertUppercaseStoreRoundTrip stores bytes and requires the same
// bytes back through an uppercase digest.
func assertUppercaseStoreRoundTrip(t *testing.T, store rawBlobDir, digest, upper string, data []byte) {
	t.Helper()
	lowerPath, err := store.blobPath(digest)
	if err != nil {
		t.Fatal(err)
	}
	upperPath, err := store.blobPath(upper)
	if err != nil {
		t.Fatal(err)
	}
	if lowerPath != upperPath {
		t.Fatalf("blobPath differs by case: %q vs %q", lowerPath, upperPath)
	}
	if err := os.MkdirAll(filepath.Dir(lowerPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lowerPath, data, 0o600); err != nil {
		t.Fatal(err)
	}
	rc, err := store.Open(upper)
	if err != nil {
		t.Fatalf("Open uppercase: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close() //nolint:errcheck // test read-only close
	if string(got) != string(data) {
		t.Fatal("uppercase open returned wrong bytes")
	}
}

// Uppercase hex digests are accepted then lowercased at every local
// entry point, while byte comparison stays exact.
func TestRobustUppercaseDigestsNormalized(t *testing.T) {
	t.Parallel()

	data := []byte("robust upper layer")
	digest := testDigest(data)
	upper := "sha256:" + strings.ToUpper(strings.TrimPrefix(digest, "sha256:"))

	lower, err := canonicalDigestHex(upper)
	if err != nil {
		t.Fatalf("canonicalDigestHex(%q): %v", upper, err)
	}
	if "sha256:"+lower != digest {
		t.Fatalf("canonicalDigestHex(%q) = %q, want %q", upper, lower, digest)
	}
	if err := verifyDigest(data, upper); err != nil {
		t.Fatalf("verifyDigest uppercase: %v", err)
	}
	sum := sha256.Sum256(data)
	if err := checkLayerDigest(sum[:], upper); err != nil {
		t.Fatalf("checkLayerDigest uppercase: %v", err)
	}
	assertUppercaseStoreRoundTrip(t, rawBlobDir{root: t.TempDir()}, digest, upper, data)
}

// plantLocalBlob writes bytes with make at the chunk path for digest and
// returns the path.
func plantLocalBlob(t *testing.T, store rawBlobDir, digest string, write func(path string)) string {
	t.Helper()
	path, err := store.blobPath(digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	write(path)
	return path
}

// requireLocalOpenFails requires Open to fail closed for digest with a
// regular-file error.
func requireLocalOpenFails(t *testing.T, store rawBlobDir, digest string) {
	t.Helper()
	_, err := store.Open(digest)
	if err == nil {
		t.Fatalf("Open(%q) succeeded, want fail closed", digest)
	}
	if !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("Open(%q) = %v, want regular-file error", digest, err)
	}
}

// assertLocalOpenFails plants a non-regular file at the chunk path and
// requires Open to fail closed with a regular-file error.
func assertLocalOpenFails(t *testing.T, store rawBlobDir, digest string, write func(path string)) {
	t.Helper()
	plantLocalBlob(t, store, digest, write)
	requireLocalOpenFails(t, store, digest)
}

// Open fails closed on non-regular files: a symlink is rejected without
// following, a FIFO is rejected without blocking, and a dangling link
// is rejected too. Has stays existence-only by design.
func TestRobustLocalOpenFailClosed(t *testing.T) {
	t.Parallel()

	store := rawBlobDir{root: t.TempDir()}
	realDigest := testDigest([]byte("regular"))
	realPath := plantLocalBlob(t, store, realDigest, func(path string) {
		if err := os.WriteFile(path, []byte("regular"), 0o600); err != nil {
			t.Fatal(err)
		}
	})
	if rc, err := store.Open(realDigest); err != nil {
		t.Fatalf("Open regular: %v", err)
	} else {
		_ = rc.Close() //nolint:errcheck // test read-only close
	}

	linkDigest := testDigest([]byte("link"))
	realLink := realPath
	plantLocalBlob(t, store, linkDigest, func(path string) {
		if err := os.Symlink(realLink, path); err != nil {
			t.Fatal(err)
		}
	})
	if !store.Has(linkDigest) {
		t.Fatal("Has(symlink to regular) = false, want existence-only true")
	}
	requireLocalOpenFails(t, store, linkDigest)

	fifoDigest := testDigest([]byte("fifo"))
	assertLocalOpenFails(t, store, fifoDigest, func(path string) {
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
	})

	danglingDigest := testDigest([]byte("dangling"))
	assertLocalOpenFails(t, store, danglingDigest, func(path string) {
		if err := os.Symlink(filepath.Join(t.TempDir(), "nowhere"), path); err != nil {
			t.Fatal(err)
		}
	})
}

// quarantineConfigCase plants corrupt config bytes and requires
// readLocalConfig to fail and remove the file.
func quarantineConfigCase(t *testing.T, store localBlobStore, rbd rawBlobDir) {
	t.Helper()
	genuineCfg := []byte(`{"architecture":"amd64","os":"linux"}`)
	cfgDigest := testDigest(genuineCfg)
	cfgPath, err := rbd.blobPath(cfgDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(cfgPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfgPath, []byte(`{"architecture":"wrong"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readLocalConfig(t.Context(), store, cfgDigest); err == nil {
		t.Fatal("readLocalConfig corrupt succeeded")
	}
	if _, err := os.Stat(cfgPath); !os.IsNotExist(err) {
		t.Fatalf("corrupt config still on disk: %v", err)
	}
}

// quarantineLayerCase plants a corrupt layer and requires the extract
// fast path to fail and remove the file. The extractor discovers the
// store itself via tryLocalStore.
func quarantineLayerCase(t *testing.T, rbd rawBlobDir) {
	t.Helper()
	genuineTar := makeTarFile(t, "payload.txt", []byte("genuine"))
	tarDigest := testDigest(genuineTar)
	tarPath, err := rbd.blobPath(tarDigest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(tarPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tarPath, makeTarFile(t, "payload.txt", []byte("tampered")), 0o600); err != nil {
		t.Fatal(err)
	}
	rootfs := filepath.Join(t.TempDir(), "rootfs")
	if err := os.MkdirAll(rootfs, 0o750); err != nil {
		t.Fatal(err)
	}
	ref := imageReference{registry: "127.0.0.1:1", repository: "testrepo/quarantine", reference: "latest"}
	if err := extractLayerBlob(t.Context(), &http.Client{}, ref, tarDigest, rootfs); err == nil {
		t.Fatal("extractLayerBlob corrupt succeeded")
	}
	if _, err := os.Stat(tarPath); !os.IsNotExist(err) {
		t.Fatalf("corrupt layer still on disk: %v", err)
	}
}

// A verified-read mismatch quarantines the corrupt local copy: the next
// pull starts from a clean miss instead of re-reading bad bytes. Both
// the config and the layer fast paths delete the file.
func TestRobustQuarantineRemovesCorruptLocal(t *testing.T) {
	// No t.Parallel: t.Setenv forbids it.
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)
	putRawBlob(t, dir, []byte("seed layout"))
	store, ok := tryLocalStore()
	if !ok {
		t.Fatal("tryLocalStore missed seeded layout")
	}
	rbd, ok := store.(rawBlobDir)
	if !ok {
		t.Fatal("tryLocalStore returned foreign holder")
	}
	quarantineConfigCase(t, store, rbd)
	quarantineLayerCase(t, rbd)
}

// Every Pull failure path removes the rootfs best effort, not just
// extract failures: parse errors and pre-extract fetch failures clear a
// stale rootfs from a previous attempt.
func TestRobustPullRemovesStaleRootfs(t *testing.T) {
	t.Parallel()

	plantStale := func(t *testing.T) (string, string) {
		t.Helper()
		bundle := t.TempDir()
		marker := filepath.Join(bundle, "rootfs", "stale.txt")
		if err := os.MkdirAll(filepath.Dir(marker), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(marker, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		return bundle, marker
	}
	mustGone := func(t *testing.T, marker string) {
		t.Helper()
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("stale rootfs marker still present: %v", err)
		}
	}

	bundle, marker := plantStale(t)
	if _, err := (&ImagePuller{}).Pull(t.Context(), "", bundle); err == nil {
		t.Fatal("Pull(empty ref) succeeded")
	}
	mustGone(t, marker)

	bundle, marker = plantStale(t)
	ref := "127.0.0.1:1/testrepo/stale:tag"
	if _, err := (&ImagePuller{}).Pull(t.Context(), ref, bundle); err == nil {
		t.Fatal("Pull(dead registry) succeeded")
	}
	mustGone(t, marker)
}
