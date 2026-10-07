// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalreader

import (
	"context"
	"errors"
	"io"
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

// ReadWithOffset reports increasing line start offsets, and SeekTo re-reads
// from a reported offset.
func TestJSONLReaderOffsetsAndSeek(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "journal.jsonl")
	line1, line2 := `{"a":1}`, `{"b":22}`
	if err := os.WriteFile(path, []byte(line1+"\n"+line2+"\n"), 0o600); err != nil {
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

	ctx := context.Background()
	mustReadOffset(t, reader, ctx, 0)
	off2 := mustReadOffset(t, reader, ctx, int64(len(line1)+1))
	mustReadEOF(t, reader, ctx)

	if err := reader.SeekTo(off2); err != nil {
		t.Fatalf("seek: %v", err)
	}
	if got := mustReadOffset(t, reader, ctx, off2); got != off2 {
		t.Fatalf("offset after seek = %d, want %d", got, off2)
	}
}

// mustReadOffset reads one event and checks its start offset.
func mustReadOffset(t *testing.T, reader *JSONLReader, ctx context.Context, want int64) int64 {
	t.Helper()
	event, off, err := reader.ReadWithOffset(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if event == nil {
		t.Fatal("expected event, got nil")
	}
	if off != want {
		t.Fatalf("offset = %d, want %d", off, want)
	}
	return off
}

// mustReadEOF reads past the end and expects EOF.
func mustReadEOF(t *testing.T, reader *JSONLReader, ctx context.Context) {
	t.Helper()
	_, _, err := reader.ReadWithOffset(ctx)
	if err == nil {
		t.Fatal("expected EOF")
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
}
