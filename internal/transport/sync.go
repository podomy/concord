// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalreader"
)

const (
	defaultSyncLimit = 100
	maxSyncLimit     = 1000
)

type SyncRequest struct {
	// Cursor is the caller's position in the peer's journal. Empty
	// means from the start.
	Cursor string `json:"cursor"`
	Limit  int    `json:"limit"`
}

type SyncResponse struct {
	// NextCursor is the cursor to send with the next request. Empty
	// means the peer had nothing new.
	NextCursor string          `json:"next_cursor"`
	Events     []journal.Event `json:"events"`
}

// postSync serves one journal page after the request cursor. A nil index
// serves from full scans, which is today's behavior without the offset
// fast path.
func postSync(w http.ResponseWriter, r *http.Request, index *offsetIndex) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB cap.

	var req SyncRequest
	derr := json.NewDecoder(r.Body).Decode(&req)
	if derr != nil {
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

	events, nextCursor, err := loadSyncPage(r.Context(), index, req.Cursor, limit)
	if err != nil {
		http.Error(w, "sync failed", http.StatusInternalServerError)
		return
	}

	resp := SyncResponse{
		Events:     events,
		NextCursor: nextCursor,
	}

	w.Header().Set("Content-Type", "application/json")
	encErr := json.NewEncoder(w).Encode(resp)
	if encErr != nil {
		http.Error(w, "sync failed", http.StatusInternalServerError)
		return
	}
}

// loadSyncPage opens the journal and returns one page after cursor.
// A usable index entry seeks straight to the cursor; any miss or mismatch
// falls back to a full scan, as does a nil index. An unknown cursor
// falls back to a from-start page so clients cannot stick on a bogus or
// obsolete cursor.
func loadSyncPage(ctx context.Context, index *offsetIndex, cursor string, limit int) ([]journal.Event, string, error) {
	if index != nil && cursor != "" {
		events, next, ok, err := loadIndexedPage(ctx, index, cursor, limit)
		if err != nil {
			return nil, "", err
		}
		if ok {
			return events, next, nil
		}
	}

	reader, err := journalreader.OpenJSONLReader()
	if err != nil {
		return nil, "", fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = reader.Close() }() //nolint:errcheck // best-effort

	events, next, found, observed, err := readJournalPage(ctx, reader, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	recordObserved(index, observed)
	if found || cursor == "" {
		return events, next, nil
	}

	// Unknown cursor: re-open and page from the beginning.
	reader2, err := journalreader.OpenJSONLReader()
	if err != nil {
		return nil, "", fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = reader2.Close() }() //nolint:errcheck // best-effort

	events, next, _, observed, err = readJournalPage(ctx, reader2, "", limit)
	if err != nil {
		return nil, "", err
	}
	recordObserved(index, observed)
	return events, next, nil
}

// loadIndexedPage serves one page by seeking to the cursor's indexed offset.
// It returns ok=false when the index has no usable entry, so the caller
// falls back to a full scan. File content is always verified: the event at
// the stored offset must carry the cursor id, otherwise the entry is stale
// and gets healed with the true offset of whatever sits there now.
func loadIndexedPage(ctx context.Context, index *offsetIndex, cursor string, limit int) (events []journal.Event, next string, ok bool, err error) {
	// No entry, corrupt value, or unloadable store: the full scan knows how.
	offset, found := index.lookup(cursor)
	if !found {
		return nil, "", false, nil
	}

	// Jump straight to the stored byte instead of scanning for it.
	reader, err := journalreader.OpenJSONLReader()
	if err != nil {
		return nil, "", false, fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = reader.Close() }() //nolint:errcheck // best-effort

	serr := reader.SeekTo(offset)
	if serr != nil {
		return nil, "", false, fmt.Errorf("seek journal: %w", serr)
	}

	// The byte is only a hint until the event there proves the cursor id.
	event, start, ok, err := verifyCursor(ctx, reader, index, cursor)
	if err != nil {
		return nil, "", false, err
	}
	if !ok {
		return nil, "", false, nil
	}

	// Proven: read the page forward and remember everything walked.
	observed := map[uuid.UUID]int64{event.ID: start}
	var walked map[uuid.UUID]int64
	events, walked, err = readPageForward(ctx, reader, limit)
	if err != nil {
		return nil, "", false, err
	}
	maps.Copy(observed, walked)
	recordObserved(index, observed)

	next = cursor
	if len(events) > 0 {
		next = events[len(events)-1].ID.String()
	}
	return events, next, true, nil
}

// verifyCursor reads the event at the reader's current position and checks
// it carries cursor. A mismatch means the index entry is stale, so it heals
// the entry with the true offset of whatever sits there now and reports
// ok=false for a full-scan fallback.
func verifyCursor(ctx context.Context, reader *journalreader.JSONLReader, index *offsetIndex, cursor string) (event *journal.Event, start int64, ok bool, err error) {
	event, start, err = reader.ReadWithOffset(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, 0, false, nil
		}
		return nil, 0, false, fmt.Errorf("read journal: %w", err)
	}
	if event.ID.String() != cursor {
		recordObserved(index, map[uuid.UUID]int64{event.ID: start})
		return nil, 0, false, nil
	}
	return event, start, true, nil
}

// readPageForward reads up to limit events from the reader's current position
// and reports every walked offset for the index. The reader must already sit
// right after the verified cursor event.
func readPageForward(ctx context.Context, reader *journalreader.JSONLReader, limit int) (events []journal.Event, walked map[uuid.UUID]int64, err error) {
	events = make([]journal.Event, 0, limit)
	walked = make(map[uuid.UUID]int64)
	for len(events) < limit {
		var ev *journal.Event
		var off int64
		ev, off, err = reader.ReadWithOffset(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return events, walked, nil
			}
			return nil, nil, fmt.Errorf("read journal: %w", err)
		}
		events = append(events, *ev)
		walked[ev.ID] = off
	}
	return events, walked, nil
}

// recordObserved stores walked offsets in the index. A nil index skips.
// Write failures are best-effort: the index is advisory, so a missed write
// only means the next request scans again.
func recordObserved(index *offsetIndex, observed map[uuid.UUID]int64) {
	if index == nil || len(observed) == 0 {
		return
	}
	_ = index.record(observed) //nolint:errcheck // advisory index, retry next request
}

// readJournalPage scans reader from the current position, skips through cursor
// (exclusive), and returns up to limit events. It also reports the byte offset
// of every event actually read, so the caller can feed the offset index.
//
// found is false only when a non-empty cursor never appears: unknown or
// obsolete. next is the last event id in the page, or the cursor itself
// when the page is empty.
func readJournalPage(
	ctx context.Context,
	reader *journalreader.JSONLReader,
	cursor string,
	limit int,
) (events []journal.Event, next string, found bool, observed map[uuid.UUID]int64, err error) {
	events = make([]journal.Event, 0, limit)
	observed = make(map[uuid.UUID]int64)
	next = cursor

	// A cursor names the last event already seen; the page starts behind it.
	// An empty cursor skips nothing: first contact starts at event zero.
	if cursor != "" {
		reached, serr := skipThroughCursor(ctx, reader, cursor, observed)
		if serr != nil {
			return nil, "", false, nil, serr
		}
		if !reached {
			return events, next, false, observed, nil
		}
	}

	// Collect the page and remember everything walked.
	var walked map[uuid.UUID]int64
	events, walked, err = readPageForward(ctx, reader, limit)
	if err != nil {
		return nil, "", false, nil, err
	}
	maps.Copy(observed, walked)

	if len(events) > 0 {
		next = events[len(events)-1].ID.String()
	}
	return events, next, true, observed, nil
}

// skipThroughCursor reads until the cursor event appears, recording every
// walked offset. It reports false when the file ends first: the cursor is
// unknown or obsolete and the caller falls back to a from-start page.
func skipThroughCursor(ctx context.Context, reader *journalreader.JSONLReader, cursor string, observed map[uuid.UUID]int64) (bool, error) {
	for {
		event, offset, err := reader.ReadWithOffset(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, fmt.Errorf("read journal: %w", err)
		}
		observed[event.ID] = offset
		if event.ID.String() == cursor {
			return true, nil
		}
	}
}
