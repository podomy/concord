// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type JSONL struct {
	file *os.File
	mu   sync.Mutex
}

// Append marshals the event and appends it as a JSON line to the journal file.
// It checks for context cancellation before acquiring the write lock.
func (j *JSONL) Append(ctx context.Context, event Event) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("append cancelled: %w", ctx.Err())
	default:
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal journal event: %w", err)
	}

	if _, err := j.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write journal event: %w", err)
	}

	if err := j.file.Sync(); err != nil {
		return fmt.Errorf("sync journal: %w", err)
	}

	return nil
}

// Close flushes and closes the underlying journal file.
func (j *JSONL) Close() error {
	if err := j.file.Close(); err != nil {
		return fmt.Errorf("close journal: %w", err)
	}

	return nil
}

// getJournalPath returns the auto-determined path for the local journal file.
func getJournalPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return "", fmt.Errorf("create node config directory: %w", err)
	}

	return filepath.Join(appDir, "journal.jsonl"), nil
}

// TestGetJournalPath returns the auto-determined path for the local journal file.
func TestGetJournalPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return "", fmt.Errorf("create node config directory: %w", err)
	}

	return filepath.Join(appDir, "test_journal.jsonl"), nil
}

// OpenJSONL opens a JSONL journal file at the auto-determined path, creating it if it doesn't exist.
func OpenJSONL() (*JSONL, error) {
	path, err := getJournalPath()
	if err != nil {
		return nil, err
	}

	return OpenJSONLPath(path)
}

// OpenJSONLPath opens a JSONL journal file at the provided path, creating it if it doesn't exist.
func OpenJSONLPath(path string) (*JSONL, error) {
	// #nosec G304: journal paths are local runtime configuration.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}

	return &JSONL{file: file}, nil
}

// TestOpenJSONL opens a JSONL journal file at the auto-determined path, creating it if it doesn't exist.
func TestOpenJSONL() (*JSONL, error) {
	path, err := TestGetJournalPath()
	if err != nil {
		return nil, err
	}

	// #nosec G304: journal paths are local runtime configuration.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}

	return &JSONL{file: file}, nil
}

func TestDeleteOpenJSONL() error {
	path, err := TestGetJournalPath()
	if err != nil {
		return fmt.Errorf("get test journal path: %w", err)
	}

	err = os.Remove(path)
	if err != nil {
		return fmt.Errorf("remove test journal: %w", err)
	}

	return nil
}
