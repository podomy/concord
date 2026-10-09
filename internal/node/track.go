// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// trackLine is one persisted trail point. Keys stay Go-case like every
// other local JSON-lines payload in this package's domain.
type trackLine struct {
	At  time.Time `json:"At"`
	Lat float64   `json:"Lat"`
	Lon float64   `json:"Lon"`
}

// TrackLog is the durable position trail: an append-only local file holding
// every gated trail point oldest-first. It is the full history the
// in-memory 720-point ring cannot keep: restarts and ring overwrite lose
// nothing here. Node-local only, never synced, never in the journal, in
// the same tradition as journal.jsonl: operators grep it, nothing converges
// on it. Rotation and archival are operator-owned, same as the journal.
type TrackLog struct {
	mu sync.Mutex
	f  *os.File
}

// trackLogPath returns the auto-determined path for the local track log,
// next to the node config and the journal.
func trackLogPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return "", fmt.Errorf("create track log directory: %w", err)
	}

	return filepath.Join(appDir, "track.jsonl"), nil
}

// OpenTrackLog opens (creating when absent) the local track log for appends.
func OpenTrackLog() (_ *TrackLog, err error) {
	path, err := trackLogPath()
	if err != nil {
		return nil, err
	}

	// #nosec G304: path is local runtime state, not user-controlled input.
	file, err := os.OpenFile(filepath.Clean(path), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open track log: %w", err)
	}

	return &TrackLog{f: file}, nil
}

// Append writes one trail point as a single JSON line and fsyncs before
// return, so a crash keeps everything up to the last recorded move. One
// write per 10m move is journal-scale traffic, not a hot path.
func (t *TrackLog) Append(at time.Time, lat, lon float64) error {
	line, err := json.Marshal(trackLine{At: at, Lat: lat, Lon: lon})
	if err != nil {
		return fmt.Errorf("marshal track line: %w", err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if _, err := t.f.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append track line: %w", err)
	}
	if err := t.f.Sync(); err != nil {
		return fmt.Errorf("sync track log: %w", err)
	}

	return nil
}

// Close releases the track log file.
func (t *TrackLog) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.f.Close(); err != nil {
		return fmt.Errorf("close track log: %w", err)
	}

	return nil
}
