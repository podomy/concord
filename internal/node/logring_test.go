// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Complete lines flush on newline; a partial tail waits for the next
// write instead of emitting a fragment.
func TestLogRingPartialLines(t *testing.T) {
	t.Parallel()

	r := newLogRing(LogStdout)
	if _, err := r.Write([]byte("hel")); err != nil {
		t.Fatal(err)
	}
	if len(r.lines()) != 0 {
		t.Fatalf("fragment flushed early: %+v", r.lines())
	}
	if _, err := r.Write([]byte("lo\nworld\n")); err != nil {
		t.Fatal(err)
	}
	got := r.lines()
	if len(got) != 2 || got[0].Text != "hello" || got[1].Text != "world" {
		t.Fatalf("lines = %+v", got)
	}
	if got[0].Stream != LogStdout || got[0].At.IsZero() {
		t.Fatalf("line = %+v", got[0])
	}
}

// Carriage returns strip and long lines truncate at the cap.
func TestLogRingTruncates(t *testing.T) {
	t.Parallel()

	r := newLogRing(LogStderr)
	if _, err := r.Write([]byte("a\r\n" + strings.Repeat("x", maxLogLineBytes+10) + "\n")); err != nil {
		t.Fatal(err)
	}
	got := r.lines()
	if len(got) != 2 || got[0].Text != "a" {
		t.Fatalf("lines = %+v", got)
	}
	if len(got[1].Text) != maxLogLineBytes || got[1].Stream != LogStderr {
		t.Fatalf("len = %d", len(got[1].Text))
	}
}

// The ring overwrites oldest-first past the cap; memory stays bounded
// no matter how chatty the writer.
func TestLogRingOverwritesOldest(t *testing.T) {
	t.Parallel()

	r := newLogRing(LogStdout)
	for range logRingSize + 5 {
		if _, err := r.Write([]byte("l\n")); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.lines(); len(got) != logRingSize {
		t.Fatalf("len = %d, want %d", len(got), logRingSize)
	}
}

// Sampler merges both streams oldest-first and drops logs with the
// workload.
func TestSamplerLogLines(t *testing.T) {
	t.Parallel()

	s := NewSampler()
	id := uuid.New()
	out := s.LogWriter(id, LogStdout)
	errOut := s.LogWriter(id, LogStderr)
	if _, err := out.Write([]byte("one\n")); err != nil {
		t.Fatal(err)
	}
	if _, err := errOut.Write([]byte("two\n")); err != nil {
		t.Fatal(err)
	}
	got := s.LogLines(id)
	if len(got) != 2 || got[0].Text != "one" || got[1].Text != "two" {
		t.Fatalf("lines = %+v", got)
	}
	if len(s.LogLines(uuid.New())) != 0 {
		t.Fatal("unknown id reported lines")
	}
	s.DropWorkload(id)
	if len(s.LogLines(id)) != 0 {
		t.Fatal("dropped workload kept logs")
	}
}
