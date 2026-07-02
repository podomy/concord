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

	"github.com/podomy/concord/src/journal"
)

// JSONLReader reads journal events from a JSONL file sequentially.
type JSONLReader struct {
	file    *os.File
	scanner *bufio.Scanner
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

	scanner := bufio.NewScanner(bufio.NewReader(file))

	return &JSONLReader{file: file, scanner: scanner}, nil
}

// Read reads the next event from the journal.
// It returns io.EOF when all events have been read.
func (r *JSONLReader) Read(ctx context.Context) (*journal.Event, error) {
	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("read cancelled: %w", ctx.Err())
	default:
	}

	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return nil, fmt.Errorf("scan journal: %w", err)
		}
		return nil, io.EOF
	}

	var event journal.Event
	if err := json.Unmarshal([]byte(r.scanner.Text()), &event); err != nil {
		return nil, fmt.Errorf("unmarshal journal event: %w", err)
	}

	return &event, nil
}

// Close closes the underlying journal file.
func (r *JSONLReader) Close() error {
	if err := r.file.Close(); err != nil {
		return fmt.Errorf("close journal reader: %w", err)
	}

	return nil
}
