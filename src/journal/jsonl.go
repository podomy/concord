// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

type JSONL struct {
	file *os.File
	mu   sync.Mutex
}

func (j *JSONL) Append(ctx context.Context, event Event) error {
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

func (j *JSONL) Close() error {
	if err := j.file.Close(); err != nil {
		return fmt.Errorf("close journal: %w", err)
	}

	return nil
}

func OpenJSONL(path string) (*JSONL, error) {
	// #nosec G304: journal paths are local runtime configuration.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}

	return &JSONL{file: file}, nil
}
