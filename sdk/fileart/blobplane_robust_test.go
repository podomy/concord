// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package fileart

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// plantPullTemps creates an aged stale temp, a fresh temp, and an
// unrelated file beside the pull dest.
func plantPullTemps(t *testing.T, destDir string) (stale, fresh, other string) {
	t.Helper()
	stale = filepath.Join(destDir, ".pull-stale.part")
	if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	fresh = filepath.Join(destDir, ".pull-fresh.part")
	if err := os.WriteFile(fresh, []byte("in flight"), 0o600); err != nil {
		t.Fatal(err)
	}
	other = filepath.Join(destDir, "keep.txt")
	if err := os.WriteFile(other, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	return stale, fresh, other
}

// assertPullSweep checks the sweep outcome: stale gone, fresh and
// unrelated kept, dest verified, and no leftover pull temp.
func assertPullSweep(t *testing.T, destDir, dest, stale, fresh, other string, payload []byte) {
	t.Helper()
	got, err := os.ReadFile(dest) //nolint:gosec // test reads a file it just pulled
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("pulled bytes differ from pushed bytes")
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale temp survived: %v", err)
	}
	for _, keep := range []string{fresh, other} {
		if _, err := os.Stat(keep); err != nil {
			t.Fatalf("kept file swept: %v", err)
		}
	}
	entries, err := os.ReadDir(destDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, ".pull-") && strings.HasSuffix(name, ".part") && name != ".pull-fresh.part" {
			t.Fatalf("leftover pull temp %q", name)
		}
	}
}

// PullTo sweeps stale pull temps beside dest before fetching: an aged
// .part file vanishes while a fresh in-flight temp and unrelated files
// survive, and the fetched bytes still verify.
func TestRobustPullToSweepsStaleTemps(t *testing.T) {
	t.Parallel()

	client, _ := dialFake(t)
	ctx := t.Context()

	payload := []byte("sweep payload")
	digest, err := client.Push(ctx, payload)
	if err != nil {
		t.Fatalf("push: %v", err)
	}

	destDir := t.TempDir()
	stale, fresh, other := plantPullTemps(t, destDir)
	dest := filepath.Join(destDir, "out")
	if err := client.PullTo(ctx, digest, dest); err != nil {
		t.Fatalf("pull: %v", err)
	}
	assertPullSweep(t, destDir, dest, stale, fresh, other, payload)
}

// PullTo accepts any hex case for the digest, normalizing to canonical
// form before the fetch.
func TestRobustPullToAcceptsUppercaseDigest(t *testing.T) {
	t.Parallel()

	client, _ := dialFake(t)
	ctx := t.Context()

	payload := []byte("upper pull")
	digest, err := client.Push(ctx, payload)
	if err != nil {
		t.Fatalf("push: %v", err)
	}
	upper := "sha256:" + strings.ToUpper(strings.TrimPrefix(digest, "sha256:"))

	dest := filepath.Join(t.TempDir(), "out")
	if err := client.PullTo(ctx, upper, dest); err != nil {
		t.Fatalf("pull uppercase: %v", err)
	}
	got, err := os.ReadFile(dest) //nolint:gosec // test reads a file it just pulled
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(payload) {
		t.Fatal("pulled bytes differ from pushed bytes")
	}
}
