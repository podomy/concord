// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"bytes"
	"sync"
	"time"
)

// logRingSize caps one workload's log ring. A thousand lines hold a real
// debugging session; memory stays flat forever. No files, ever: logs are
// inspection state, and unbounded growth belongs nowhere in the daemon.
const logRingSize = 1000

// maxLogLineBytes caps one stored log line. Longer lines truncate; a
// runaway writer must not widen one entry into megabytes.
const maxLogLineBytes = 4096

// LogLine is one timestamped output line with its stream.
type LogLine struct {
	At     time.Time
	Stream string
	Text   string
}

// Log streams name the container file descriptor a line arrived on.
const (
	LogStdout = "stdout"
	LogStderr = "stderr"
)

// logBundle holds one workload's stdout and stderr rings together so the
// sampler tracks a single entry per workload and drops both at once.
type logBundle struct {
	stdout *LogRing
	stderr *LogRing
}

// LogRing is a fixed-size round-robin of output lines: appends overwrite
// the oldest once full. Self-locked: container IO goroutines write while
// the reconciler and IPC handlers read. Use lines() for snapshots;
// follow-style polling compares At against the last printed line.
type LogRing struct {
	mu      sync.Mutex
	pts     []LogLine
	pending []byte
	next    int
	full    bool
	stream  string
	// dropped counts lines overwritten past the cap: backpressure made
	// visible. Monotonic; the fast beat reports it on its debug line.
	dropped uint64
}

// newLogRing creates an empty ring collecting one stream.
func newLogRing(stream string) *LogRing {
	return &LogRing{pts: make([]LogLine, 0, logRingSize), stream: stream}
}

// maxPendingBytes caps one stream's unterminated tail. Completed lines
// truncate at 4KB each; a tail past 64KB with no newline is a runaway
// writer, so the oldest bytes drop instead of growing without bound.
const maxPendingBytes = 64 << 10

// Write appends bytes, flushing complete newline-terminated lines into
// the ring. Partial lines wait in pending for the next write; a stream
// that never terminates its last line keeps it pending, never loses it.
func (r *LogRing) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.pending = append(r.pending, p...)
	if len(r.pending) > maxPendingBytes {
		r.pending = append([]byte(nil), r.pending[len(r.pending)-maxPendingBytes:]...)
	}
	for {
		i := bytes.IndexByte(r.pending, '\n')
		if i < 0 {
			break
		}
		r.add(LogLine{At: time.Now(), Stream: r.stream, Text: truncateLine(r.pending[:i])})
		r.pending = append([]byte(nil), r.pending[i+1:]...)
	}
	return len(p), nil
}

// lines returns the held lines oldest-first.
func (r *LogRing) lines() []LogLine {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.full {
		out := make([]LogLine, len(r.pts))
		copy(out, r.pts)
		return out
	}
	out := make([]LogLine, 0, logRingSize)
	out = append(out, r.pts[r.next:]...)
	out = append(out, r.pts[:r.next]...)
	return out
}

// add appends a line, overwriting the oldest once full and counting the
// overwrite as dropped.
func (r *LogRing) add(line LogLine) {
	if len(r.pts) < logRingSize {
		r.pts = append(r.pts, line)
		r.next = len(r.pts) % logRingSize
		return
	}
	r.pts[r.next] = line
	r.next = (r.next + 1) % logRingSize
	r.full = true
	r.dropped++
}

// Dropped reports lines overwritten past the cap.
func (r *LogRing) Dropped() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

// truncateLine strips a trailing carriage return and caps length.
func truncateLine(line []byte) string {
	line = bytes.TrimSuffix(line, []byte{'\r'})
	if len(line) > maxLogLineBytes {
		line = line[:maxLogLineBytes]
	}
	return string(line)
}

// LogDropped totals overwritten log lines across all workload rings.
// The reconciler's fast beat reports it on its periodic debug line so
// backpressure stays visible without touching the log API.
func (s *Sampler) LogDropped() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var total uint64
	for _, bundle := range s.logs {
		total += bundle.stdout.Dropped() + bundle.stderr.Dropped()
	}
	return total
}
