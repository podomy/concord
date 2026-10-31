// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package logs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// Level selects the minimum severity a Logger emits.
type Level slog.Level

// Log levels, matching the ParseLevel vocabulary.
const (
	DebugLevel Level = Level(slog.LevelDebug)
	InfoLevel  Level = Level(slog.LevelInfo)
	WarnLevel  Level = Level(slog.LevelWarn)
	ErrorLevel Level = Level(slog.LevelError)
)

// Attr is a structured log field, mirroring zap.Field in shape.
type Attr = slog.Attr

// String builds a string-valued Attr.
func String(key, val string) Attr {
	return slog.String(key, val)
}

// Int builds an int-valued Attr.
func Int(key string, val int) Attr {
	return slog.Int(key, val)
}

// Uint builds a uint-valued Attr.
func Uint(key string, val uint) Attr {
	return slog.Uint64(key, uint64(val))
}

// Uint16 builds a uint16-valued Attr.
func Uint16(key string, val uint16) Attr {
	return slog.Uint64(key, uint64(val))
}

// Uint64 builds a uint64-valued Attr.
func Uint64(key string, val uint64) Attr {
	return slog.Uint64(key, val)
}

// Strings builds a string-slice-valued Attr.
func Strings(key string, val []string) Attr {
	return slog.Any(key, val)
}

// Bool builds a bool-valued Attr.
func Bool(key string, val bool) Attr {
	return slog.Bool(key, val)
}

// Error builds an error-valued Attr keyed "error". The error stays
// structured instead of being flattened. A nil error records an explicit
// null; zap skipped the field, and this difference is accepted.
func Error(err error) Attr {
	return slog.Any("error", err)
}

// Logger is a JSON slog logger with zap-shaped convenience methods.
type Logger struct {
	inner *slog.Logger
	name  string
}

// toAny renders Attrs as slog arguments.
func toAny(attrs []Attr) []any {
	args := make([]any, len(attrs))
	for i, attr := range attrs {
		args[i] = attr
	}
	return args
}

// Debug logs at debug level. A nil logger drops the entry.
func (l *Logger) Debug(msg string, attrs ...Attr) {
	if l == nil || l.inner == nil {
		return
	}
	l.inner.Debug(msg, toAny(attrs)...)
}

// Info logs at info level. A nil logger drops the entry.
func (l *Logger) Info(msg string, attrs ...Attr) {
	if l == nil || l.inner == nil {
		return
	}
	l.inner.Info(msg, toAny(attrs)...)
}

// Warn logs at warn level. A nil logger drops the entry.
func (l *Logger) Warn(msg string, attrs ...Attr) {
	if l == nil || l.inner == nil {
		return
	}
	l.inner.Warn(msg, toAny(attrs)...)
}

// Error logs at error level. A nil logger drops the entry.
func (l *Logger) Error(msg string, attrs ...Attr) {
	if l == nil || l.inner == nil {
		return
	}
	l.inner.Error(msg, toAny(attrs)...)
}

// Enabled reports whether entries at level pass the logger's level.
func (l *Logger) Enabled(level Level) bool {
	if l == nil || l.inner == nil {
		return false
	}
	return l.inner.Enabled(context.Background(), slog.Level(level))
}

// With returns a logger carrying attrs on every entry. The logger name
// set by Named is preserved.
func (l *Logger) With(attrs ...Attr) *Logger {
	if l == nil || l.inner == nil {
		return NewNop().With(attrs...)
	}
	return &Logger{inner: l.inner.With(toAny(attrs)...), name: l.name}
}

// Named returns a logger tagged with a logger name, mirroring zap.Named:
// the name rides as a single logger field on every entry. Chained names
// join with a dot, so Named("a") then Named("b") renders logger "a.b".
// A user field keyed "logger" collides as before: both keys are emitted
// and the last one wins, matching zap behavior.
func (l *Logger) Named(name string) *Logger {
	if l == nil || l.inner == nil {
		return NewNop().Named(name)
	}
	if name == "" {
		return &Logger{inner: l.inner, name: l.name}
	}
	joined := name
	if l.name != "" {
		joined = l.name + "." + name
	}
	return &Logger{inner: l.inner.With(String("logger", joined)), name: joined}
}

// Sync flushes buffered entries. It stays a no-op for API compatibility:
// the slog JSONHandler writes unbuffered, so there is nothing to flush.
// A future buffered handler must implement a real Sync here.
func (l *Logger) Sync() error {
	return nil
}

// NewNop returns a logger discarding every entry.
func NewNop() *Logger {
	return &Logger{inner: slog.New(slog.NewJSONHandler(io.Discard, nil))}
}

// NewTest returns a logger writing entries to t.Log. Do not share the
// logger with background goroutines that outlive the test: after cleanup
// the handler drops entries silently, and the guard only prevents the
// t.Log-after-test panic.
func NewTest(t *testing.T) *Logger {
	t.Helper()
	closed := &atomic.Bool{}
	t.Cleanup(func() {
		closed.Store(true)
	})
	return &Logger{inner: slog.New(&testHandler{t: t, closed: closed})}
}

// testHandler routes slog records into testing.T.Log.
type testHandler struct {
	t      *testing.T
	attrs  []Attr
	closed *atomic.Bool
}

// Enabled admits every level so tests observe debug entries too.
// Production loggers still filter by level; this difference is accepted.
func (h *testHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

// Handle formats one record into t.Log with its message and fields.
// Records arriving after test cleanup are dropped silently.
func (h *testHandler) Handle(_ context.Context, r slog.Record) error {
	if h.closed != nil && h.closed.Load() {
		return nil
	}
	h.t.Helper()
	parts := make([]string, 0, len(h.attrs)+r.NumAttrs())
	for _, attr := range h.attrs {
		parts = append(parts, attr.String())
	}
	r.Attrs(func(attr slog.Attr) bool {
		parts = append(parts, attr.String())
		return true
	})
	if len(parts) == 0 {
		h.t.Log(r.Message)
		return nil
	}
	h.t.Log(r.Message + " " + strings.Join(parts, " "))
	return nil
}

// WithAttrs returns a handler carrying attrs on later records.
func (h *testHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	merged := make([]Attr, 0, len(h.attrs)+len(attrs))
	merged = append(merged, h.attrs...)
	merged = append(merged, attrs...)
	return &testHandler{t: h.t, attrs: merged, closed: h.closed}
}

// WithGroup keeps the handler flat; groups add no test value.
func (h *testHandler) WithGroup(string) slog.Handler {
	return h
}

// ParseLevel maps a flag value to a Level. Empty means info, so
// unset flags and old callers keep today's behavior. Anything else
// fails loudly instead of silently running at an unintended level.
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return InfoLevel, nil
	case "debug":
		return DebugLevel, nil
	case "warn":
		return WarnLevel, nil
	case "error":
		return ErrorLevel, nil
	default:
		return InfoLevel, fmt.Errorf("unknown log level %q: want debug, info, warn, or error", s)
	}
}

// Init builds a JSON logger on stderr at the given level and returns a
// sync function to call before exit. The slog handler writes unbuffered,
// so sync stays a no-op for API compatibility; a future buffered handler
// must implement a real Sync.
func Init(level Level) (*Logger, func() error, error) {
	var levelVar slog.LevelVar
	levelVar.Set(slog.Level(level))
	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: &levelVar})
	syncLogs := func() error {
		return nil
	}
	return &Logger{inner: slog.New(handler)}, syncLogs, nil
}

// Node returns a logger enriched with the node_id field for correlated logging.
func Node(logger *Logger, nodeID string) *Logger {
	if logger == nil {
		logger = NewNop()
	}
	return logger.With(String("node_id", nodeID))
}
