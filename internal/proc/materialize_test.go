// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package proc_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/podomy/concord/internal/proc"
	"github.com/podomy/concord/internal/workload"
)

// fakeFetch writes canned bytes for any digest.
func fakeFetch(content string) func(context.Context, string, string) error {
	return func(_ context.Context, _, dest string) error {
		return os.WriteFile(dest, []byte(content), 0o600)
	}
}

const testDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// Command naming a staged path selects the binary.
func TestBinaryPathCommandWins(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{
		Command: []string{"/app/server"},
		Files: []workload.FileRef{
			{Path: "/app/server", Digest: testDigest},
			{Path: "/app/data", Digest: testDigest, Mode: 0o755},
		},
	}
	got, err := proc.BinaryPath(spec)
	if err != nil {
		t.Fatalf("binary path: %v", err)
	}
	if got.Path != "/app/server" {
		t.Fatalf("binary = %q, want /app/server", got.Path)
	}
}

// Without a command, the sole exec-bit entry wins.
func TestBinaryPathSoleExecBit(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{
		Files: []workload.FileRef{
			{Path: "/app/data", Digest: testDigest},
			{Path: "/app/run", Digest: testDigest, Mode: 0o755},
		},
	}
	got, err := proc.BinaryPath(spec)
	if err != nil {
		t.Fatalf("binary path: %v", err)
	}
	if got.Path != "/app/run" {
		t.Fatalf("binary = %q, want /app/run", got.Path)
	}
}

// Two exec-bit entries without a command fail instead of guessing.
func TestBinaryPathAmbiguousFails(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{
		Files: []workload.FileRef{
			{Path: "/app/a", Digest: testDigest, Mode: 0o755},
			{Path: "/app/b", Digest: testDigest, Mode: 0o755},
		},
	}
	if _, err := proc.BinaryPath(spec); err == nil {
		t.Fatal("expected ambiguous binary error, got nil")
	}
}

// No command and no exec bit fails instead of running data.
func TestBinaryPathNoneFails(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{
		Files: []workload.FileRef{{Path: "/app/data", Digest: testDigest}},
	}
	if _, err := proc.BinaryPath(spec); err == nil {
		t.Fatal("expected missing binary error, got nil")
	}
}

// StageFiles lays out files, exec-bits the binary, and runs it.
func TestStageFilesRoundTrip(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{
		Command: []string{"/app/echoer"},
		Files: []workload.FileRef{
			{Path: "/app/echoer", Digest: testDigest},
			{Path: "/app/data/msg.txt", Digest: testDigest, Mode: 0o644},
		},
	}
	dest := filepath.Join(t.TempDir(), "stage")
	bin, err := proc.StageFiles(context.Background(), fakeFetch("#!/bin/sh\necho staged\n"), spec, dest)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if bin != filepath.Join(dest, "app", "echoer") {
		t.Fatalf("bin = %q", bin)
	}
	info, err := os.Stat(bin)
	if err != nil {
		t.Fatalf("stat bin: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Fatalf("bin mode = %o, want exec bit", info.Mode().Perm())
	}
	content, err := os.ReadFile(filepath.Join(dest, "app", "data", "msg.txt")) //nolint:gosec // test reads a file it just staged
	if err != nil {
		t.Fatalf("read staged data: %v", err)
	}
	if string(content) != "#!/bin/sh\necho staged\n" {
		t.Fatalf("data = %q", content)
	}
}

// Restaging swaps atomically: the old binary keeps serving until rename.
func TestStageFilesAtomicSwap(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{
		Command: []string{"/app/bin"},
		Files:   []workload.FileRef{{Path: "/app/bin", Digest: testDigest, Mode: 0o755}},
	}
	dest := filepath.Join(t.TempDir(), "stage")
	v1 := func(_ context.Context, _, d string) error {
		return os.WriteFile(d, []byte("v1"), 0o755) //nolint:gosec // test stages an executable fixture
	}
	v2 := func(_ context.Context, _, d string) error {
		return os.WriteFile(d, []byte("v2"), 0o755) //nolint:gosec // test stages an executable fixture
	}
	if _, err := proc.StageFiles(context.Background(), v1, spec, dest); err != nil {
		t.Fatalf("stage v1: %v", err)
	}
	bin, err := proc.StageFiles(context.Background(), v2, spec, dest)
	if err != nil {
		t.Fatalf("stage v2: %v", err)
	}
	content, err := os.ReadFile(bin) //nolint:gosec // test reads the path StageFiles just returned
	if err != nil {
		t.Fatalf("read swapped bin: %v", err)
	}
	if string(content) != "v2" {
		t.Fatalf("bin = %q, want v2", content)
	}
	if _, err := os.Stat(dest + ".new"); !os.IsNotExist(err) {
		t.Fatal("staging dir left behind after publish")
	}
}

// A fetch failure aborts before publish: the live stage is untouched.
func TestStageFilesFetchFailureKeepsLive(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{
		Command: []string{"/app/bin"},
		Files: []workload.FileRef{
			{Path: "/app/bin", Digest: testDigest, Mode: 0o755},
			{Path: "/app/missing", Digest: testDigest},
		},
	}
	dest := filepath.Join(t.TempDir(), "stage")
	failer := func(_ context.Context, _, d string) error {
		if filepath.Base(d) == "missing" {
			return errors.New("blob gone")
		}
		return os.WriteFile(d, []byte("x"), 0o600)
	}
	if _, err := proc.StageFiles(context.Background(), failer, spec, dest); err == nil {
		t.Fatal("expected stage error, got nil")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatal("live stage exists after failed publish")
	}
}
