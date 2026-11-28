// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/podomy/concord/internal/workload"
)

// BinaryPath selects the executable entry from spec file refs. When the
// spec names a command, the first word must match a staged path; a
// mismatch is a submit error surfaced here, so the binary never differs
// from argv zero. When the command is empty, a single exec-bit entry
// wins. Anything else is a submit error surfaced here: guessing between
// several binaries would start the wrong one.
func BinaryPath(spec workload.Spec) (workload.FileRef, error) {
	if len(spec.Command) > 0 {
		for _, f := range spec.Files {
			if f.Path == spec.Command[0] {
				return f, nil
			}
		}
		return workload.FileRef{}, fmt.Errorf("command names no staged file: %q", spec.Command[0])
	}
	var found *workload.FileRef
	for i := range spec.Files {
		if spec.Files[i].Mode&0o111 == 0 {
			continue
		}
		if found != nil {
			return workload.FileRef{}, fmt.Errorf("binary ambiguous: %q and %q both executable, set command to choose", found.Path, spec.Files[i].Path)
		}
		ref := spec.Files[i]
		found = &ref
	}
	if found == nil {
		return workload.FileRef{}, errors.New("no binary: command names no staged file and no entry has an exec bit") //nolint:wrapcheck // plain error, no wrap target
	}
	return *found, nil
}

// StageFiles materializes spec file artifacts via fetch into dest and
// reports the host path of the binary. Fetch writes one digest to one
// destination path; the registry file client has that exact shape.
//
// The swap is atomic: files land in dest.new, then dest moves aside to
// dest.old and dest.new takes its place, so a crash never leaves a half
// binary behind. A zero mode means 0600, except on the binary itself,
// which means 0700: the CLI default flow stages `--file` entries without
// a mode and must still produce a runnable binary.
func StageFiles(ctx context.Context, fetch func(ctx context.Context, digest, dest string) error, spec workload.Spec, dest string) (string, error) {
	for _, f := range spec.Files {
		err := f.Validate()
		if err != nil {
			return "", fmt.Errorf("stage files: %w", err)
		}
	}
	bin, err := BinaryPath(spec)
	if err != nil {
		return "", err
	}
	staging := dest + ".new"
	old := dest + ".old"
	err = os.RemoveAll(staging)
	if err != nil {
		return "", fmt.Errorf("stage files clear staging: %w", err)
	}
	err = os.MkdirAll(staging, 0o750)
	if err != nil {
		return "", fmt.Errorf("stage files make staging: %w", err)
	}
	for _, f := range spec.Files {
		err := stageOne(ctx, fetch, f, staging, f == bin)
		if err != nil {
			return "", err
		}
	}
	err = os.RemoveAll(old)
	if err != nil {
		return "", fmt.Errorf("stage files clear retired: %w", err)
	}
	if _, err := os.Lstat(dest); err == nil {
		err = os.Rename(dest, old)
		if err != nil {
			return "", fmt.Errorf("stage files retire live: %w", err)
		}
	}
	err = os.Rename(staging, dest)
	if err != nil {
		return "", fmt.Errorf("stage files publish: %w", err)
	}
	_ = os.RemoveAll(old) //nolint:errcheck // best-effort cleanup of the retired stage
	binRel := strings.TrimPrefix(filepath.Clean(bin.Path), string(filepath.Separator))
	return filepath.Join(dest, binRel), nil
}

// stageOne fetches one file ref into the staging root and sets its mode.
// A zero mode means 0600, except on the binary itself, which means 0700
// so the default CLI flow stages a runnable binary without a mode flag.
func stageOne(ctx context.Context, fetch func(ctx context.Context, digest, dest string) error, f workload.FileRef, staging string, isBin bool) error {
	rel := strings.TrimPrefix(filepath.Clean(f.Path), string(filepath.Separator))
	target := filepath.Join(staging, rel)
	if target != staging && !strings.HasPrefix(target, staging+string(filepath.Separator)) {
		return fmt.Errorf("stage file escapes staging: %q", f.Path)
	}
	err := os.MkdirAll(filepath.Dir(target), 0o750)
	if err != nil {
		return fmt.Errorf("stage file make parent: %w", err)
	}
	err = fetch(ctx, f.Digest, target)
	if err != nil {
		return fmt.Errorf("stage file fetch %q: %w", f.Path, err)
	}
	mode := os.FileMode(f.Mode)
	if f.Mode == 0 {
		mode = 0o600
		if isBin {
			mode = 0o700
		}
	}
	err = os.Chmod(target, mode)
	if err != nil {
		return fmt.Errorf("stage file chmod %q: %w", f.Path, err)
	}
	return nil
}
