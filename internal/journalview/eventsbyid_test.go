// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/journal"
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

func TestEventsByIDApplyCancelledContext(t *testing.T) {
	t.Parallel()

	ctx := cancelledContext()
	kv := testKVStore(t)
	view := NewEventsByID(kv)
	event := journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))

	if err := view.Apply(ctx, event); err == nil {
		t.Fatalf("expected apply cancellation error")
	}
}

func TestEventsByIDGetCancelledContext(t *testing.T) {
	t.Parallel()

	ctx := cancelledContext()
	kv := testKVStore(t)
	view := NewEventsByID(kv)

	if _, err := view.Get(ctx, uuid.New()); err == nil {
		t.Fatalf("expected get cancellation error")
	}
}

func TestEventsByIDGetMissingBucket(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewEventsByID(kv)

	got, err := view.Get(ctx, uuid.New())
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil event, got %v", got)
	}
}

func TestEventsByIDGetMissingKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewEventsByID(kv)
	event := journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))

	if err := view.Apply(ctx, event); err != nil {
		t.Fatalf("apply event: %v", err)
	}

	got, err := view.Get(ctx, uuid.New())
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil event, got %v", got)
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
	sortEventsByTimestamp := func(events []journal.Event) {
		sort.Slice(events, func(i, j int) bool { return events[i].Timestamp.After(events[j].Timestamp) })
	}
	// We compare the journals exactly, and before comparing them we sort them
	// in order, because bbolt cursor returns the events in unordered slice.
	// If we don't order both we get an error of the slices not matching.
	sortEventsByTimestamp(sampleJournalEvents)
	sortEventsByTimestamp(journalEvents)
	if !reflect.DeepEqual(sampleJournalEvents, journalEvents) {
		t.Fatalf("events mismatch: want %#v got %#v", sampleJournalEvents, journalEvents)
	}
}

func TestEventsByIDListCancelledContext(t *testing.T) {
	t.Parallel()

	ctx := cancelledContext()
	kv := testKVStore(t)
	view := NewEventsByID(kv)

	if _, err := view.List(ctx); err == nil {
		t.Fatalf("expected list cancellation error")
	}
}

func TestEventsByIDListMissingBucket(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewEventsByID(kv)

	journalEvents, err := view.List(ctx)
	if err != nil {
		t.Fatalf("view list: %v", err)
	}
	if len(journalEvents) != 0 {
		t.Fatalf("expected no events, got %d", len(journalEvents))
	}
}
