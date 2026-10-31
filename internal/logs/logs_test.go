// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package logs

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// Levels map by name; empty stays info so unset flags and old callers
// keep today's behavior.
func TestParseLevel(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]Level{
		"":      InfoLevel,
		"info":  InfoLevel,
		"INFO":  InfoLevel,
		"debug": DebugLevel,
		"warn":  WarnLevel,
		"error": ErrorLevel,
	} {
		got, err := ParseLevel(input)
		if err != nil {
			t.Fatalf("ParseLevel(%q) = %v", input, err)
		}
		if got != want {
			t.Fatalf("ParseLevel(%q) = %v, want %v", input, got, want)
		}
	}
	if _, err := ParseLevel("verbose"); err == nil {
		t.Fatal("expected error for unknown level")
	}
}

// Init honors the level: a debug logger writes debug, info does not.
func TestInitRespectsLevel(t *testing.T) {
	t.Parallel()

	debug, _, err := Init(DebugLevel)
	if err != nil {
		t.Fatal(err)
	}
	if !debug.Enabled(DebugLevel) {
		t.Fatal("debug logger drops debug")
	}

	info, _, err := Init(InfoLevel)
	if err != nil {
		t.Fatal(err)
	}
	if info.Enabled(DebugLevel) {
		t.Fatal("info logger writes debug")
	}
}

// Named chains with a dot, so a then b renders logger a.b as one field.
// With preserves the tracked name across the chain.
func TestNamedChaining(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := &Logger{inner: slog.New(slog.NewJSONHandler(&buf, nil))}
	chained := base.Named("a").Named("b")
	if chained.name != "a.b" {
		t.Fatalf("chained name = %q, want %q", chained.name, "a.b")
	}
	chained.Info("hello")
	if !strings.Contains(buf.String(), `"logger":"a.b"`) {
		t.Fatalf("missing joined name in %q", buf.String())
	}

	var buf2 bytes.Buffer
	base2 := &Logger{inner: slog.New(slog.NewJSONHandler(&buf2, nil))}
	throughWith := base2.Named("a").With(String("k", "v")).Named("b")
	if throughWith.name != "a.b" {
		t.Fatalf("name through With = %q, want %q", throughWith.name, "a.b")
	}
	throughWith.Info("hello")
	if !strings.Contains(buf2.String(), `"logger":"a.b"`) {
		t.Fatalf("missing joined name through With in %q", buf2.String())
	}
}

// A user field keyed logger collides as before: both keys are emitted and
// the last one wins, matching zap. The joined name stays present.
func TestNamedUserKeyCollision(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	base := &Logger{inner: slog.New(slog.NewJSONHandler(&buf, nil))}
	base.With(String("logger", "user")).Named("a").Info("hello")
	line := buf.String()
	if !strings.Contains(line, `"logger":"user"`) {
		t.Fatalf("missing user key in %q", line)
	}
	if !strings.Contains(line, `"logger":"a"`) {
		t.Fatalf("missing joined name in %q", line)
	}
}

// NewTest writes normally, then drops after cleanup instead of calling
// t.Log, which would panic. Loggers from the same T stay independent.
func TestNewTestDropsAfterClose(t *testing.T) {
	first := NewTest(t)
	first.Info("before close", String("k", "v"))
	second := NewTest(t)
	second.Info("second works")

	h, ok := first.inner.Handler().(*testHandler)
	if !ok {
		t.Fatal("first handler is not a testHandler")
	}
	oh, ok := second.inner.Handler().(*testHandler)
	if !ok {
		t.Fatal("second handler is not a testHandler")
	}

	// Simulate t.Cleanup after the test ends for the first logger only.
	h.closed.Store(true)
	err := h.Handle(context.Background(), slog.NewRecord(time.Now(), slog.LevelInfo, "direct", 0))
	if err != nil {
		t.Fatalf("Handle after close = %v", err)
	}
	first.Info("after close")
	first.With(String("k", "v")).Info("derived after close")

	if oh.closed.Load() {
		t.Fatal("closing first logger closed the second")
	}
	second.Info("second still open")
}
