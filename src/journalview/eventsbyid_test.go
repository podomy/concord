// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"

	"github.com/podomy/hive/src/journal"
	"github.com/podomy/hive/src/kvstore"
)

func TestEventsByIDGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewEventsByID(kv)

	event := journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))

	if err := view.Apply(ctx, event); err != nil {
		t.Fatalf("apply event: %v", err)
	}

	got, err := view.Get(ctx, event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if got == nil {
		t.Fatalf("expected event got nil")
	}
	if got.ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, got.ID)
	}
}

func TestEventsByIDList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewEventsByID(kv)

	event1 := journal.NewEvent(uuid.New(), "node started", json.RawMessage(`{}`))
	event2 := journal.NewEvent(uuid.New(), "node running", json.RawMessage(`{}`))
	event3 := journal.NewEvent(uuid.New(), "node restrating", json.RawMessage(`{}`))
	sampleJournalEvents := []journal.Event{}
	sampleJournalEvents = append(sampleJournalEvents, event1)
	sampleJournalEvents = append(sampleJournalEvents, event2)
	sampleJournalEvents = append(sampleJournalEvents, event3)
	for _, sampleEvent := range sampleJournalEvents {
		err := view.Apply(ctx, sampleEvent)
		if err != nil {
			t.Fatalf("view apply: %v", err)
		}
	}

	journalEvents, err := view.List(ctx)
	if err != nil {
		t.Fatalf("view list: %v", err)
	}
	sortEventsByTimestamp := cmpopts.SortSlices(func(a, b journal.Event) bool { return a.Timestamp.After(b.Timestamp) })
	// We compare the journals exactly, and before comparing them we sort them
	// in order, because bbolt cursor returns the events in unordered slice.
	// If we don't order both we get an error of the slices not matching.
	if diff := cmp.Diff(sampleJournalEvents, journalEvents, sortEventsByTimestamp); diff != "" {
		t.Fatalf("events mismatch (-want +got):\n%s", diff)
	}
}

// This function sits really awkwardly here, I have no options
// in my mind to place it somewhere better. It is used for the
// tests in the journalview package.
func testKVStore(t *testing.T) *kvstore.KVStore {
	t.Helper()

	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db path: %v", err)
	}

	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Fatalf("test close db: %v", err)
		}
	})

	return kv
}
