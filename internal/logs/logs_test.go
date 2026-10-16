// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package logs

import (
	"testing"

	"go.uber.org/zap/zapcore"
)

// Levels map by name; empty stays info so unset flags and old callers
// keep today's behavior.
func TestParseLevel(t *testing.T) {
	t.Parallel()

	for input, want := range map[string]zapcore.Level{
		"":      zapcore.InfoLevel,
		"info":  zapcore.InfoLevel,
		"INFO":  zapcore.InfoLevel,
		"debug": zapcore.DebugLevel,
		"warn":  zapcore.WarnLevel,
		"error": zapcore.ErrorLevel,
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

	debug, _, err := Init(zapcore.DebugLevel)
	if err != nil {
		t.Fatal(err)
	}
	if !debug.Core().Enabled(zapcore.DebugLevel) {
		t.Fatal("debug logger drops debug")
	}

	info, _, err := Init(zapcore.InfoLevel)
	if err != nil {
		t.Fatal(err)
	}
	if info.Core().Enabled(zapcore.DebugLevel) {
		t.Fatal("info logger writes debug")
	}
}
