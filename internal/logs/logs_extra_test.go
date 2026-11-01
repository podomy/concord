// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package logs

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// syncBuffer is a goroutine-safe bytes.Buffer for concurrent logging tests.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends p under lock so concurrent log writes stay race free.
func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n, werr := b.buf.Write(p)
	if werr != nil {
		return n, fmt.Errorf("buffer write: %w", werr)
	}
	return n, nil
}

// String returns a snapshot of buffered output.
func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// Len returns the buffered byte count.
func (b *syncBuffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Len()
}

// TestEnabledFiltersByLevel pins threshold filtering: Init at a level
// enables that severity and above, and disables everything below.
func TestEnabledFiltersByLevel(t *testing.T) {
	t.Parallel()

	for _, initLevel := range []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel} {
		t.Run(levelName(initLevel), func(t *testing.T) {
			t.Parallel()

			logger, _, err := Init(initLevel)
			if err != nil {
				t.Fatal(err)
			}
			for _, level := range []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel} {
				want := level >= initLevel
				if got := logger.Enabled(level); got != want {
					t.Errorf("Init(%v).Enabled(%v) = %v, want %v", initLevel, level, got, want)
				}
			}
		})
	}
}

// levelName renders a Level for subtest names.
func levelName(l Level) string {
	switch l {
	case DebugLevel:
		return "debug"
	case InfoLevel:
		return "info"
	case WarnLevel:
		return "warn"
	case ErrorLevel:
		return "error"
	default:
		return "unknown"
	}
}

// TestInfoLevelDropsDebugWrites pins that debug entries never reach the
// writer at info level while info entries do.
func TestInfoLevelDropsDebugWrites(t *testing.T) {
	t.Parallel()

	var buf syncBuffer
	handler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := &Logger{inner: slog.New(handler)}

	logger.Debug("quiet")
	if buf.Len() != 0 {
		t.Fatalf("debug write at info level produced %d bytes", buf.Len())
	}
	logger.Info("loud")
	if !strings.Contains(buf.String(), "loud") {
		t.Fatalf("info write missing from %q", buf.String())
	}
}

// TestNilLoggerDropsEntries pins that a nil logger disables every level
// and drops entries without panicking.
func TestNilLoggerDropsEntries(t *testing.T) {
	t.Parallel()

	var logger *Logger
	for _, level := range []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel} {
		if logger.Enabled(level) {
			t.Errorf("nil logger Enabled(%v) = true, want false", level)
		}
	}
	logger.Debug("dropped")
	logger.Info("dropped")
	logger.Warn("dropped")
	logger.Error("dropped")
}

// TestConcurrentLogging pins that one logger shared across goroutines
// writes every entry without racing. Run with -race.
func TestConcurrentLogging(t *testing.T) {
	t.Parallel()

	var buf syncBuffer
	logger := &Logger{inner: slog.New(slog.NewJSONHandler(&buf, nil))}

	const workers = 8
	const perWorker = 25

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for n := range perWorker {
				logger.With(Int("worker", worker), Int("n", n)).Info("tick")
			}
		}(i)
	}
	wg.Wait()

	lines := strings.Count(buf.String(), "\n")
	if lines != workers*perWorker {
		t.Fatalf("logged %d lines, want %d", lines, workers*perWorker)
	}
}
