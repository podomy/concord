// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalreader"
)

// From-start page: empty watermark returns the first limit events.
func TestReadJournalPageFromStart(t *testing.T) {
	t.Parallel()

	e1, e2, e3 := testEvent("a"), testEvent("b"), testEvent("c")
	r := openTempJournal(t, e1, e2, e3)

	got, next, found, err := readJournalPage(context.Background(), r, "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("empty watermark should be found")
	}
	if len(got) != 2 || got[0].ID != e1.ID || got[1].ID != e2.ID {
		t.Fatalf("got %#v", got)
	}
	if next != e2.ID.String() {
		t.Fatalf("next = %q, want e2", next)
	}
}

// After watermark: page is exclusive of the watermark event.
func TestReadJournalPageAfterWatermark(t *testing.T) {
	t.Parallel()

	e1, e2, e3 := testEvent("a"), testEvent("b"), testEvent("c")
	r := openTempJournal(t, e1, e2, e3)

	got, next, found, err := readJournalPage(context.Background(), r, e1.ID.String(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("known watermark should be found")
	}
	if len(got) != 2 || got[0].ID != e2.ID || got[1].ID != e3.ID {
		t.Fatalf("got %#v", got)
	}
	if next != e3.ID.String() {
		t.Fatalf("next = %q, want e3", next)
	}
}

// Cursor already at last event: empty page, watermark unchanged, found true.
func TestReadJournalPageEmptyAfterLast(t *testing.T) {
	t.Parallel()

	e1 := testEvent("a")
	r := openTempJournal(t, e1)

	got, next, found, err := readJournalPage(context.Background(), r, e1.ID.String(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("watermark at end should still be found")
	}
	if len(got) != 0 {
		t.Fatalf("got %d events, want 0", len(got))
	}
	if next != e1.ID.String() {
		t.Fatalf("next = %q, want watermark", next)
	}
}

// Unknown watermark: found false so loadSyncPage can fall back to from-start.
func TestReadJournalPageUnknownWatermark(t *testing.T) {
	t.Parallel()

	e1 := testEvent("a")
	r := openTempJournal(t, e1)
	unknown := uuid.New().String()

	got, next, found, err := readJournalPage(context.Background(), r, unknown, 10)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Fatal("unknown watermark must not be found")
	}
	if len(got) != 0 || next != unknown {
		t.Fatalf("got len=%d next=%q", len(got), next)
	}
}

// Limit caps page size; next is the last event included.
func TestReadJournalPageRespectsLimit(t *testing.T) {
	t.Parallel()

	events := []journal.Event{testEvent("1"), testEvent("2"), testEvent("3"), testEvent("4")}
	r := openTempJournal(t, events...)

	got, next, found, err := readJournalPage(context.Background(), r, "", 3)
	if err != nil {
		t.Fatal(err)
	}
	if !found || len(got) != 3 {
		t.Fatalf("found=%v len=%d", found, len(got))
	}
	if next != events[2].ID.String() {
		t.Fatalf("next = %q", next)
	}
}

func testEvent(label string) journal.Event {
	return journal.NewEvent(uuid.New(), "test."+label, json.RawMessage(`{}`))
}

func openTempJournal(t *testing.T, events ...journal.Event) *journalreader.JSONLReader {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.jsonl")
	// #nosec G304: path is under t.TempDir in tests.
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	enc := json.NewEncoder(f)
	for _, ev := range events {
		if err := enc.Encode(ev); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := journalreader.OpenJSONLReaderPath(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	})
	return r
}
