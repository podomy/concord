// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalreader

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestJSONLReaderReadCancelledContext(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "journal.jsonl")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write journal: %v", err)
	}

	reader, err := OpenJSONLReaderPath(path)
	if err != nil {
		t.Fatalf("open journal reader: %v", err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("close journal reader: %v", err)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := reader.Read(ctx); err == nil {
		t.Fatalf("expected read cancellation error")
	}
}

func TestJSONLReaderInvalidJSON(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "journal.jsonl")
	if err := os.WriteFile(path, []byte("not-json\n"), 0o600); err != nil {
		t.Fatalf("write journal: %v", err)
	}

	reader, err := OpenJSONLReaderPath(path)
	if err != nil {
		t.Fatalf("open journal reader: %v", err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Fatalf("close journal reader: %v", err)
		}
	})

	if _, err := reader.Read(context.Background()); err == nil {
		t.Fatalf("expected invalid JSON error")
	}
}
