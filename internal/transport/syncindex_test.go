// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalreader"
	"github.com/podomy/concord/internal/kvstore"
)

// testOffsetIndex opens an isolated offset index for index unit tests.
func testOffsetIndex(t *testing.T) *offsetIndex {
	t.Helper()
	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "bbolt.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("close kv: %v", err)
		}
	})
	return NewOffsetIndex(kv)
}

// writeEnvJournal points XDG_CONFIG_HOME at dir and writes events to the
// resolved journal path, so loadSyncPage and the index build read them.
func writeEnvJournal(t *testing.T, dir string, events ...journal.Event) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", dir)
	path := filepath.Join(dir, "concord", "journal.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
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
}

// collectOffsets maps every event id in path to its line start offset.
func collectOffsets(t *testing.T, path string) map[uuid.UUID]int64 {
	t.Helper()
	r, err := journalreader.OpenJSONLReaderPath(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Errorf("close reader: %v", err)
		}
	})
	out := map[uuid.UUID]int64{}
	for {
		ev, off, err := r.ReadWithOffset(context.Background())
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out
			}
			t.Fatal(err)
		}
		out[ev.ID] = off
	}
}

// A fresh index misses everything, including non-uuid cursors.
func TestOffsetIndexLookupMissOnEmpty(t *testing.T) {
	t.Parallel()

	index := testOffsetIndex(t)
	if _, ok := index.lookup(uuid.New().String()); ok {
		t.Fatal("empty index should miss")
	}
	if _, ok := index.lookup("not-a-uuid"); ok {
		t.Fatal("non-uuid cursor should miss")
	}
}

// Recorded offsets come back with their exact values.
func TestOffsetIndexRecordLookupRoundtrip(t *testing.T) {
	t.Parallel()

	index := testOffsetIndex(t)
	a, b := uuid.New(), uuid.New()
	if err := index.record(map[uuid.UUID]int64{a: 0, b: 12345}); err != nil {
		t.Fatal(err)
	}
	off, ok := index.lookup(a.String())
	if !ok || off != 0 {
		t.Fatalf("lookup a = %d, %v", off, ok)
	}
	off, ok = index.lookup(b.String())
	if !ok || off != 12345 {
		t.Fatalf("lookup b = %d, %v", off, ok)
	}
}

// A short value is corrupt and must miss, never decode garbage.
func TestOffsetIndexCorruptValueIsMiss(t *testing.T) {
	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "bbolt.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("close kv: %v", err)
		}
	})
	index := NewOffsetIndex(kv)
	id := uuid.New()
	key, err := id.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.DB().Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketNameSyncIndex))
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}
		if err := b.Put(key, []byte("short")); err != nil {
			return fmt.Errorf("put corrupt value: %w", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, ok := index.lookup(id.String()); ok {
		t.Fatal("corrupt value should miss")
	}
}

// Indexed hit: with real offsets recorded, the page after e2 is e3..e5.
func TestLoadSyncPageIndexedHit(t *testing.T) {
	dir := t.TempDir()
	events := []journal.Event{testEvent("1"), testEvent("2"), testEvent("3"), testEvent("4"), testEvent("5")}
	writeEnvJournal(t, dir, events...)
	actual := collectOffsets(t, filepath.Join(dir, "concord", "journal.jsonl"))
	index := testOffsetIndex(t)
	if err := index.record(actual); err != nil {
		t.Fatal(err)
	}

	got, next, err := loadSyncPage(context.Background(), index, events[1].ID.String(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].ID != events[2].ID || got[2].ID != events[4].ID {
		t.Fatalf("got %d events, want e3..e5", len(got))
	}
	if next != events[4].ID.String() {
		t.Fatalf("next = %q", next)
	}
}

// Stale entry: the offset of e4 stored under e2's id falls back to a full
// scan, serves the correct page, and heals e2's entry.
func TestLoadSyncPageIndexMismatchFallsBackAndHeals(t *testing.T) {
	dir := t.TempDir()
	events := []journal.Event{testEvent("1"), testEvent("2"), testEvent("3")}
	writeEnvJournal(t, dir, events...)
	actual := collectOffsets(t, filepath.Join(dir, "concord", "journal.jsonl"))
	index := testOffsetIndex(t)
	stale := map[uuid.UUID]int64{events[1].ID: actual[events[2].ID]}
	if err := index.record(stale); err != nil {
		t.Fatal(err)
	}

	got, next, err := loadSyncPage(context.Background(), index, events[1].ID.String(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != events[2].ID {
		t.Fatalf("got %v, want [e3]", got)
	}
	if next != events[2].ID.String() {
		t.Fatalf("next = %q", next)
	}
	healed, ok := index.lookup(events[1].ID.String())
	if !ok || healed != actual[events[1].ID] {
		t.Fatalf("healed = %d, %v, want %d", healed, ok, actual[events[1].ID])
	}
}

// Nil index serves exactly as before: full scan, correct page.
func TestLoadSyncPageNilIndex(t *testing.T) {
	dir := t.TempDir()
	events := []journal.Event{testEvent("1"), testEvent("2")}
	writeEnvJournal(t, dir, events...)

	got, next, err := loadSyncPage(context.Background(), nil, events[0].ID.String(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != events[1].ID {
		t.Fatalf("got %v, want [e2]", got)
	}
	if next != events[1].ID.String() {
		t.Fatalf("next = %q", next)
	}
}

// Empty cursor skips the index and pages from the start.
func TestLoadSyncPageEmptyCursorSkipsIndex(t *testing.T) {
	dir := t.TempDir()
	events := []journal.Event{testEvent("1"), testEvent("2")}
	writeEnvJournal(t, dir, events...)
	index := testOffsetIndex(t)

	got, _, err := loadSyncPage(context.Background(), index, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
}

// Build fills the index from the journal so later lookups hit.
func TestOffsetIndexBuildFillsIndex(t *testing.T) {
	dir := t.TempDir()
	events := []journal.Event{testEvent("1"), testEvent("2"), testEvent("3")}
	writeEnvJournal(t, dir, events...)
	index := testOffsetIndex(t)

	if err := index.build(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if _, ok := index.lookup(ev.ID.String()); !ok {
			t.Fatalf("missing index entry for %v", ev.ID)
		}
	}
}
