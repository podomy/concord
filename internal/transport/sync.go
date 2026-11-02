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
	"os"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalreader"
)

const (
	defaultSyncLimit = 100
	maxSyncLimit     = 1000
)

// SyncRequest asks the peer for journal events after Cursor.
type SyncRequest struct {
	// Cursor is the caller's position in the peer's journal. Empty
	// means from the start.
	Cursor string `json:"cursor"`
	Limit  int    `json:"limit"`
}

// SyncResponse returns one journal page and the next cursor.
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

	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req SyncRequest
	if derr := dec.Decode(&req); derr != nil {
		http.Error(w, "sync failed", http.StatusBadRequest)
		return
	}
	// Reject trailing garbage after the first JSON value: one value per
	// body, no concatenation. A clean body decodes to EOF here; any extra
	// value or syntax error fails closed.
	var extra json.RawMessage
	if terr := dec.Decode(&extra); terr != io.EOF {
		http.Error(w, "sync failed", http.StatusBadRequest)
		return
	}

	// Cursor shape is checked early: a non-UUID cursor can never name an
	// event, so normalize to from-start now for a single-pass serve in
	// loadSyncPage instead of a miss-discovery scan plus a second scan.
	if req.Cursor != "" {
		if _, cerr := uuid.Parse(req.Cursor); cerr != nil {
			req.Cursor = ""
		}
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

// normalizeCursor maps a non-UUID cursor to from-start. A cursor that is
// not a UUID can never match an event id, so serving from the start in one
// pass avoids a miss-discovery scan plus a second scan.
func normalizeCursor(cursor string) string {
	if cursor == "" {
		return ""
	}
	if _, cerr := uuid.Parse(cursor); cerr != nil {
		return ""
	}
	return cursor
}

// tryIndexedPage serves one page through the offset index. It reports
// ok=false when the index is nil, the cursor is empty, or the index has no
// usable entry, so the caller falls back to a full scan.
func tryIndexedPage(ctx context.Context, index *offsetIndex, cursor string, limit int) ([]journal.Event, string, bool, error) {
	if index == nil || cursor == "" {
		return nil, "", false, nil
	}
	events, next, ok, err := loadIndexedPage(ctx, index, cursor, limit)
	if err != nil {
		return nil, "", false, err
	}
	if !ok {
		return nil, "", false, nil
	}
	return events, next, true, nil
}

// loadSyncPage opens the journal and returns one page after cursor.
// A usable index entry seeks straight to the cursor; any miss or mismatch
// falls back to a full scan, as does a nil index. An unknown cursor
// falls back to a from-start page so clients cannot stick on a bogus or
// obsolete cursor.
//
// Cursor shape is validated first: a non-UUID cursor can never match an
// event id, so it serves from the start in a single pass with no
// miss-discovery scan. A valid-but-unknown UUID still costs one scan to
// discover the miss, then serves from the start by seeking the same reader
// to zero instead of re-opening the journal: one open, no second file
// handle, with both passes' offsets recorded.
func loadSyncPage(ctx context.Context, index *offsetIndex, cursor string, limit int) ([]journal.Event, string, error) {
	cursor = normalizeCursor(cursor)
	if events, next, ok, err := tryIndexedPage(ctx, index, cursor, limit); err != nil {
		return nil, "", err
	} else if ok {
		return events, next, nil
	}

	reader, err := journalreader.OpenJSONLReader()
	if err != nil {
		// A missing journal means no local events yet: serve an empty
		// page (200 with Events:[] and empty NextCursor) instead of a
		// 500. Only not-exist takes this path; real open errors such
		// as permissions still fail with 500 below. errors.Is is used
		// because the reader wraps the open error and os.IsNotExist
		// does not traverse wrap chains.
		if errors.Is(err, os.ErrNotExist) {
			return []journal.Event{}, "", nil
		}
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

	// Valid-but-unknown cursor: the first scan proved the miss. Seek the
	// same reader to the start instead of opening a second handle, then
	// page from the beginning.
	if serr := reader.SeekTo(0); serr != nil {
		return nil, "", fmt.Errorf("seek journal: %w", serr)
	}

	events, next, _, observed, err = readJournalPage(ctx, reader, "", limit)
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
		// A missing journal holds no offsets: report a miss so the
		// caller falls back to a full scan, which serves the empty
		// page. Real open errors still fail closed below. errors.Is is
		// used because the reader wraps the open error.
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", false, nil
		}
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
