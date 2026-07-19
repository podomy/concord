// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalreader"
)

const (
	defaultSyncLimit = 100
	maxSyncLimit     = 1000
)

type SyncRequest struct {
	Watermark string `json:"watermark"`
	Limit     int    `json:"limit"`
}

type SyncResponse struct {
	NextWatermark string          `json:"next_watermark"`
	Events        []journal.Event `json:"events"`
}

func postSync(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB cap

	var req SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "sync failed", http.StatusBadRequest)
		return
	}

	limit := req.Limit
	if limit <= 0 {
		limit = defaultSyncLimit
	}
	if limit > maxSyncLimit {
		http.Error(w, "limit is too large (>1000)", http.StatusBadRequest)
		return
	}

	events, nextWatermark, err := loadSyncPage(r.Context(), req.Watermark, limit)
	if err != nil {
		http.Error(w, "sync failed", http.StatusInternalServerError)
		return
	}

	resp := SyncResponse{
		Events:        events,
		NextWatermark: nextWatermark,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "sync failed", http.StatusInternalServerError)
		return
	}
}

// loadSyncPage opens the journal and returns one page after watermark.
// If watermark is set but never found (EOF still skipping), falls back to a
// from-start page so clients cannot stick on a bogus or obsolete cursor.
func loadSyncPage(ctx context.Context, watermark string, limit int) ([]journal.Event, string, error) {
	reader, err := journalreader.OpenJSONLReader()
	if err != nil {
		return nil, "", fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = reader.Close() }() //nolint:errcheck // best-effort

	events, next, found, err := readJournalPage(ctx, reader, watermark, limit)
	if err != nil {
		return nil, "", err
	}
	if found || watermark == "" {
		return events, next, nil
	}

	// Unknown watermark: re-open and page from the beginning.
	reader2, err := journalreader.OpenJSONLReader()
	if err != nil {
		return nil, "", fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = reader2.Close() }() //nolint:errcheck // best-effort

	events, next, _, err = readJournalPage(ctx, reader2, "", limit)
	if err != nil {
		return nil, "", err
	}
	return events, next, nil
}

// readJournalPage scans reader from the current position, skips through watermark
// (exclusive), and returns up to limit events.
//
// found is true if watermark was empty (no skip) or the watermark id was seen.
// If the file ends while still skipping, found is false (unknown/obsolete mark).
// next is the last event id in the page, or watermark if the page is empty and found.
func readJournalPage(
	ctx context.Context,
	reader journalreader.Reader,
	watermark string,
	limit int,
) (events []journal.Event, next string, found bool, err error) {
	skipping := watermark != ""
	found = watermark == ""
	next = watermark
	events = make([]journal.Event, 0, limit)

	for len(events) < limit {
		var event *journal.Event
		event, err = reader.Read(ctx)
		if errors.Is(err, io.EOF) {
			return events, next, found, nil
		}
		if err != nil {
			return nil, "", false, fmt.Errorf("read journal: %w", err)
		}
		if skipping {
			if event.ID.String() == watermark {
				skipping = false
				found = true
			}
			continue
		}
		events = append(events, *event)
		next = event.ID.String()
	}

	return events, next, found, nil
}
