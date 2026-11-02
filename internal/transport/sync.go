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
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalreader"
	"github.com/podomy/concord/internal/logs"
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
// fast path. It logs nothing; serveSyncPage carries the logger.
//
//nolint:unparam // the (w, r, index) shape is pinned by existing tests; production passes the live index through serveSyncPage.
func postSync(w http.ResponseWriter, r *http.Request, index *offsetIndex) {
	serveSyncPage(w, r, index, nil)
}

// serveSyncPage serves one journal page after the request cursor with a
// logger for outbound validation. A bad count or an unbound cursor with no
// drops fails 500; malformed events are dropped with a warn count and the
// cursor rebinds to the surviving tail instead of failing the page. An
// all-dropped page echoes the request cursor so the client reads it as
// nothing-new.
func serveSyncPage(w http.ResponseWriter, r *http.Request, index *offsetIndex, logger *logs.Logger) {
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

	filtered, dropped, firstReason, verr := checkOutboundPage(events, nextCursor)
	if verr != nil {
		logger.Error("sync page invalid", logs.Error(verr), logs.Int("events", len(events)))
		http.Error(w, "sync failed", http.StatusInternalServerError)
		return
	}
	if dropped > 0 {
		logger.Warn("sync dropped invalid events",
			logs.Int("dropped", dropped),
			logs.Error(firstReason),
			logs.Int("served", len(filtered)),
		)
	}

	out := req.Cursor
	if len(filtered) > 0 {
		// Survivors keep order, so the tail is the resume point. When
		// nothing dropped this equals nextCursor; after drops it moves
		// past the corruption instead of sticking on it.
		out = filtered[len(filtered)-1].ID.String()
	}

	resp := SyncResponse{
		Events:     filtered,
		NextCursor: out,
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

// openJournalReader opens the local journal for serving. It is a variable
// so tests can count opens without touching the reader package: the
// unknown-cursor path must serve from one handle, never two.
var openJournalReader = journalreader.OpenJSONLReader

// minFullScanInterval is the minimum gap between full journal scans for
// one cursor. Sequential pagination uses a fresh cursor per page and never
// trips; a peer stuck on one bogus cursor gets one scan per interval
// instead of one per tick. Indexed seeks are cheap and never gated.
const minFullScanInterval = 30 * time.Second

// maxScanGateEntries bounds the throttle table. Expired entries are swept
// opportunistically past this size, so forgotten cursors cannot grow it.
const maxScanGateEntries = 1024

// scanGate throttles repeat full journal scans per cursor. The zero value
// is not usable; build one with newScanGate. It is safe for concurrent use.
type scanGate struct {
	mu       sync.Mutex
	last     map[string]time.Time
	interval time.Duration
	now      func() time.Time
}

// newScanGate builds a throttle with the given minimum interval and clock.
// The clock is injectable so tests can prove throttling without sleeping.
func newScanGate(interval time.Duration, now func() time.Time) *scanGate {
	return &scanGate{last: make(map[string]time.Time), interval: interval, now: now}
}

// allow reports whether a full journal scan for cursor may proceed. The
// empty cursor always passes: from-start is the honest bootstrap path,
// not a miss. Other cursors pass once per interval; repeats are denied
// until the interval lapses, and the caller serves an empty echo instead.
func (g *scanGate) allow(cursor string) bool {
	if cursor == "" {
		return true
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.now()
	if at, ok := g.last[cursor]; ok && now.Sub(at) < g.interval {
		return false
	}
	g.last[cursor] = now
	if len(g.last) > maxScanGateEntries {
		for c, at := range g.last {
			if now.Sub(at) >= g.interval {
				delete(g.last, c)
			}
		}
	}
	return true
}

// fullScanGate is the process-wide throttle for repeat full scans.
var fullScanGate = newScanGate(minFullScanInterval, time.Now)

// checkOutboundPage validates a page the server is about to encode. Count
// faults fail the page: they prove a server bug. Malformed events are
// skipped with a drop count instead: one corrupt journal line must not
// wedge every peer behind it. Survivors keep order; the caller rebinds
// NextCursor to the surviving tail, or echoes the request cursor when
// nothing survives. Binding is enforced only when nothing was dropped: a
// surviving tail legitimately moves the cursor off the dropped original,
// which is progress past corruption, not a redirect.
func checkOutboundPage(events []journal.Event, next string) (filtered []journal.Event, dropped int, firstReason, err error) {
	if len(events) > maxSyncLimit {
		return nil, 0, nil, fmt.Errorf("sync page holds %d events, over max %d", len(events), maxSyncLimit)
	}
	if next != "" {
		_, perr := uuid.Parse(next)
		if perr != nil {
			return nil, 0, nil, fmt.Errorf("sync page next cursor: %w", perr)
		}
	}
	var nobody netip.AddrPort
	seen := make(map[uuid.UUID]struct{}, len(events))
	filtered = make([]journal.Event, 0, len(events))
	for i := range events {
		verr := validateSyncEvent(nobody, &events[i], i, seen)
		if verr != nil {
			dropped++
			if firstReason == nil {
				firstReason = verr
			}
			continue
		}
		filtered = append(filtered, events[i])
	}
	if dropped == 0 && len(filtered) > 0 && next != filtered[len(filtered)-1].ID.String() {
		return nil, 0, nil, fmt.Errorf("sync page next cursor %q does not match last event %s", next, filtered[len(filtered)-1].ID)
	}
	return filtered, dropped, firstReason, nil
}

// loadSyncPage opens the journal once and returns one page after cursor.
// A usable index entry seeks straight to the cursor; any miss or mismatch
// falls back to a full scan on the same handle after rewinding, never a
// second open. A nil index scans directly. An unknown cursor falls back to
// a from-start page so clients cannot stick on a bogus or obsolete cursor.
//
// Cursor shape is validated first: a non-UUID cursor can never match an
// event id, so it serves from the start in a single pass with no
// miss-discovery scan. Repeat full scans for one cursor are throttled by
// fullScanGate; a denied scan serves an empty echo of the cursor.
func loadSyncPage(ctx context.Context, index *offsetIndex, cursor string, limit int) ([]journal.Event, string, error) {
	cursor = normalizeCursor(cursor)

	reader, err := openJournalReader()
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

	if index != nil && cursor != "" {
		events, next, ok, err := probeIndex(ctx, reader, index, cursor, limit)
		if err != nil {
			return nil, "", err
		}
		if ok {
			return events, next, nil
		}
	}

	if !fullScanGate.allow(cursor) {
		return []journal.Event{}, cursor, nil
	}

	events, next, found, observed, err := readJournalPage(ctx, reader, cursor, limit)
	if err != nil {
		return nil, "", err
	}
	recordObserved(index, observed)
	if found || cursor == "" {
		return events, next, nil
	}

	// Valid-but-unknown cursor: the first scan proved the miss, so serve
	// from the start on this same handle.
	return serveUnknownFromStart(ctx, reader, index, limit)
}

// serveUnknownFromStart pages from the start on an already open reader
// after a miss-discovery scan proved the cursor unknown. It shares the
// handle instead of opening the journal twice.
func serveUnknownFromStart(ctx context.Context, reader *journalreader.JSONLReader, index *offsetIndex, limit int) ([]journal.Event, string, error) {
	if serr := reader.SeekTo(0); serr != nil {
		return nil, "", fmt.Errorf("seek journal: %w", serr)
	}

	events, next, _, observed, err := readJournalPage(ctx, reader, "", limit)
	if err != nil {
		return nil, "", err
	}
	recordObserved(index, observed)
	return events, next, nil
}

// probeIndex serves one indexed page on an already open reader and reports
// hit=true with the page. On a miss it rewinds the reader to the start so
// the caller falls back to a full scan on the same handle. A nil index or
// an empty cursor skips probing: the full scan knows how.
func probeIndex(ctx context.Context, reader *journalreader.JSONLReader, index *offsetIndex, cursor string, limit int) (events []journal.Event, next string, hit bool, err error) {
	if index == nil || cursor == "" {
		return nil, "", false, nil
	}
	events, next, ok, serr := serveIndexedPage(ctx, reader, index, cursor, limit)
	if serr != nil {
		return nil, "", false, serr
	}
	if ok {
		return events, next, true, nil
	}
	// Miss: the reader sits mid-file after the probe, so rewind for the
	// full scan below on this same handle.
	if rerr := reader.SeekTo(0); rerr != nil {
		return nil, "", false, fmt.Errorf("seek journal: %w", rerr)
	}
	return nil, "", false, nil
}

// serveIndexedPage serves one page through the offset index on an already
// open reader. It reports ok=false when the index has no usable entry, so
// the caller rewinds the same handle and falls back to a full scan. File
// content is always verified: the event at the stored offset must carry
// the cursor id, otherwise the entry is stale and gets healed with the
// true offset of whatever sits there now.
func serveIndexedPage(ctx context.Context, reader *journalreader.JSONLReader, index *offsetIndex, cursor string, limit int) (events []journal.Event, next string, ok bool, err error) {
	// No entry, corrupt value, or unloadable store: the full scan knows how.
	offset, found := index.lookup(cursor)
	if !found {
		return nil, "", false, nil
	}

	// Jump straight to the stored byte instead of scanning for it.
	serr := reader.SeekTo(offset)
	if serr != nil {
		return nil, "", false, fmt.Errorf("seek journal: %w", serr)
	}

	// The byte is only a hint until the event there proves the cursor id.
	event, start, ok, verr := verifyCursor(ctx, reader, index, cursor)
	if verr != nil {
		return nil, "", false, verr
	}
	if !ok {
		return nil, "", false, nil
	}

	// Proven: read the page forward and remember everything walked.
	observed := map[uuid.UUID]int64{event.ID: start}
	var walked map[uuid.UUID]int64
	events, walked, rerr := readPageForward(ctx, reader, limit)
	if rerr != nil {
		return nil, "", false, rerr
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

// maxSyncPageBytes caps the encoded size of one served page at about
// 512 KiB. Pages complete inside the fixed write timeouts instead of
// growing with the event limit; the client resumes with NextCursor. At
// least one event always ships, so a single giant event degrades to
// one-per-page instead of stalling the cursor.
//
// TLA drift note (no model edits): the model has no page-size bound;
// the budget only narrows served pages, never the wire format.
const maxSyncPageBytes = 512 << 10

// readPageForward reads up to limit events from the reader's current position
// and reports every walked offset for the index. The reader must already sit
// right after the verified cursor event. Collection also stops past the
// byte budget: the over-budget event is rewound, not skipped, so the next
// page starts with it.
func readPageForward(ctx context.Context, reader *journalreader.JSONLReader, limit int) (events []journal.Event, walked map[uuid.UUID]int64, err error) {
	events = make([]journal.Event, 0, limit)
	walked = make(map[uuid.UUID]int64)
	budget := 0
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
		raw, merr := json.Marshal(ev)
		if merr != nil {
			return nil, nil, fmt.Errorf("marshal journal event: %w", merr)
		}
		if len(events) > 0 && budget+len(raw) > maxSyncPageBytes {
			serr := reader.SeekTo(off)
			if serr != nil {
				return nil, nil, fmt.Errorf("seek journal: %w", serr)
			}
			break
		}
		budget += len(raw)
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
