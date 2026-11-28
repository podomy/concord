// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"os"
	"testing"
)

// Comm fields with spaces and parens still parse: tick fields count
// from the final closing paren, not from whitespace splits.
func TestParseProcStatTicksWithSpacedComm(t *testing.T) {
	t.Parallel()

	stat := []byte("1234 (my spaced (proc)) S 1 2 3 4 5 6 7 8 9 10 100 200 0 0 0 0 0")
	got, err := parseProcStatTicks(stat)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != 300 {
		t.Fatalf("ticks = %d, want 300", got)
	}
}

// Short lines fail loudly instead of indexing out of range.
func TestParseProcStatTicksShortLine(t *testing.T) {
	t.Parallel()

	if _, err := parseProcStatTicks([]byte("1 (x) S 1")); err == nil {
		t.Fatal("expected error for short stat, got nil")
	}
}

// VmRSS parses from status text in kB.
func TestParseProcStatusVmRSS(t *testing.T) {
	t.Parallel()

	status := []byte("Name:\ttest\nVmRSS:\t   12345 kB\nVmSize:\t   99999 kB\n")
	got, err := parseProcStatusVmRSS(status)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != 12345 {
		t.Fatalf("memKB = %d, want 12345", got)
	}
}

// A status without VmRSS fails loudly instead of reporting zero.
func TestParseProcStatusVmRSSMissing(t *testing.T) {
	t.Parallel()

	if _, err := parseProcStatusVmRSS([]byte("Name:\ttest\n")); err == nil {
		t.Fatal("expected error for missing VmRSS, got nil")
	}
}

// A live process reports memory; the test binary is resident.
func TestReadProcStatSelf(t *testing.T) {
	t.Parallel()

	_, mem, err := ReadProcStat(os.Getpid())
	if err != nil {
		t.Fatalf("read self: %v", err)
	}
	if mem == 0 {
		t.Fatal("self VmRSS is zero")
	}
}

// A dead pid reports an error and no sample, so callers keep the old one.
func TestReadProcStatDeadPID(t *testing.T) {
	t.Parallel()

	if _, _, err := ReadProcStat(1 << 30); err == nil {
		t.Fatal("expected error for dead pid, got nil")
	}
}
