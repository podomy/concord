// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalreader

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/podomy/concord/internal/journal"
)

// JSONLReader reads journal events from a JSONL file sequentially.
// It also tracks byte offsets: every event is one line, so the start
// offset of each line stays valid as long as the file is append-only.
// A crash-torn final line is dropped with EOF: at most one line is
// ever lost, the committed prefix always reads back.
type JSONLReader struct {
	file    *os.File
	scanner *bufio.Scanner
	offset  int64
}

// maxJournalLineBytes caps one journal line. Events are small, but a cap
// far above the 64KB scanner default means one oversized payload fails
// loudly at 10MB instead of mysteriously at 64KB.
const maxJournalLineBytes = 10 * 1024 * 1024

// newJournalScanner scans one event per line with room for large payloads.
func newJournalScanner(file *os.File) *bufio.Scanner {
	scanner := bufio.NewScanner(bufio.NewReader(file))
	scanner.Buffer(make([]byte, 64*1024), maxJournalLineBytes)
	return scanner
}

// getJournalPath returns the auto-determined path for the local journal file.
func getJournalPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return "", fmt.Errorf("create config directory: %w", err)
	}

	return filepath.Join(appDir, "journal.jsonl"), nil
}

// OpenJSONLReader opens the journal file for reading and returns a reader that
// iterates over events sequentially. Each call creates a fresh file handle starting
// at the beginning of the journal.
func OpenJSONLReader() (*JSONLReader, error) {
	path, err := getJournalPath()
	if err != nil {
		return nil, err
	}

	return OpenJSONLReaderPath(path)
}

// OpenJSONLReaderPath opens the journal file at the provided path for reading.
func OpenJSONLReaderPath(path string) (*JSONLReader, error) {
	// #nosec G304: journal paths are local runtime configuration.
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open journal for reading: %w", err)
	}

	scanner := newJournalScanner(file)

	return &JSONLReader{file: file, scanner: scanner}, nil
}

// Read reads the next event from the journal.
// It returns io.EOF when all events have been read.
func (r *JSONLReader) Read(ctx context.Context) (*journal.Event, error) {
	event, _, err := r.ReadWithOffset(ctx)
	if err != nil {
		return nil, err
	}
	return event, nil
}

// ReadWithOffset reads the next event and reports the byte offset where
// its line starts. The caller can Seek back to a reported offset later:
// offsets stay valid because the journal only ever appends whole lines.
//
// Crash recovery: a torn final line (crash between write and newline)
// fails to parse. When the unparsable line runs to end of file with no
// trailing newline it is the partial tail, so it is dropped and EOF is
// reported: at most the torn line is lost, the prefix rebuilds. A
// corrupt line in the middle of the file still errors loudly instead of
// silently skipping history.
func (r *JSONLReader) ReadWithOffset(ctx context.Context) (*journal.Event, int64, error) {
	select {
	case <-ctx.Done():
		return nil, 0, fmt.Errorf("read cancelled: %w", ctx.Err())
	default:
	}

	if !r.scanner.Scan() {
		serr := r.scanner.Err()
		if serr != nil {
			return nil, 0, fmt.Errorf("scan journal: %w", serr)
		}
		return nil, 0, io.EOF
	}

	text := r.scanner.Text()
	start := r.offset
	// One newline byte follows every line; the writer always appends it.
	r.offset += int64(len(text)) + 1

	var event journal.Event
	err := json.Unmarshal([]byte(text), &event)
	if err != nil {
		if isTornTail(r.file, start, int64(len(text))) {
			r.offset = start
			return nil, 0, io.EOF
		}
		return nil, 0, fmt.Errorf("unmarshal journal event: %w", err)
	}

	return &event, start, nil
}

// isTornTail reports whether the line starting at start with bodyLen
// bytes is an unterminated tail: the file holds nothing past the body,
// so no newline ever landed. A stat failure returns false, keeping the
// loud parse error instead of hiding corruption behind a guess.
func isTornTail(file *os.File, start, bodyLen int64) bool {
	info, err := file.Stat()
	if err != nil {
		return false
	}
	return info.Size() <= start+bodyLen
}

// SeekTo positions the reader at a byte offset previously reported by
// ReadWithOffset (or zero for the file start). The offset must be a line
// boundary; seeking mid-line reads garbage.
func (r *JSONLReader) SeekTo(offset int64) error {
	_, err := r.file.Seek(offset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("seek journal: %w", err)
	}
	r.scanner = newJournalScanner(r.file)
	r.offset = offset
	return nil
}

// Close closes the underlying journal file.
func (r *JSONLReader) Close() error {
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("close journal reader: %w", err)
	}

	return nil
}
