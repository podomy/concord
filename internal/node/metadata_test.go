// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"errors"
	"strings"
	"testing"
)

// Frequency lines sum across cores; malformed lines are skipped.
func TestParseProcCPUFreqSumsCores(t *testing.T) {
	in := "processor\t: 0\ncpu MHz\t\t: 2400.000\nprocessor\t: 1\ncpu MHz\t\t: 1200.000\ncpu MHz\t\t: bogus\n"
	got, err := parseProcCPUFreq(strings.NewReader(in))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got != 3600.0 {
		t.Fatalf("sum = %v, want 3600", got)
	}
}

// Hosts without frequency lines (ARM, masked hypervisors) report
// ErrNoCPUMHzFound so the caller can degrade to unknown, never zero-as-data.
func TestParseProcCPUFreqMissingIsSentinel(t *testing.T) {
	in := "processor\t: 0\nmodel name\t: ARMv8\n"
	if _, err := parseProcCPUFreq(strings.NewReader(in)); !errors.Is(err, ErrNoCPUMHzFound) {
		t.Fatalf("err = %v, want ErrNoCPUMHzFound", err)
	}
}
