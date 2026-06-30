// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // Projection tests intentionally keep view-specific setup and assertions local.
package journalview

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"

	"github.com/podomy/hive/src/journal"
)

func TestEventsByNodeGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewEventsByNode(kv)

	event := journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))

	if err := view.Apply(ctx, event); err != nil {
		t.Fatalf("apply event: %v", err)
	}

	got, err := view.Get(ctx, event.NodeID, event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if got == nil {
		t.Fatalf("expected event got nil")
	}
	if got.NodeID != event.NodeID {
		t.Fatalf("expected node ID %s, got %s", event.NodeID, got.NodeID)
	}
	if got.ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, got.ID)
	}
}

func TestEventsByNodeList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewEventsByNode(kv)

	nodeID := uuid.New()
	event1 := journal.NewEvent(nodeID, "node.started", json.RawMessage(`{}`))
	event2 := journal.NewEvent(nodeID, "node.running", json.RawMessage(`{}`))
	event3 := journal.NewEvent(uuid.New(), "node.restarting", json.RawMessage(`{}`))
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

	// This is the journal list we expect after doing view.List
	// it won't contain the event3
	expectedJournalEvents := []journal.Event{}
	expectedJournalEvents = append(expectedJournalEvents, event1)
	expectedJournalEvents = append(expectedJournalEvents, event2)

	journalEvents, err := view.List(ctx, nodeID)
	if err != nil {
		t.Fatalf("view list: %v", err)
	}
	sortEventsByTimestamp := cmpopts.SortSlices(func(a, b journal.Event) bool { return a.Timestamp.After(b.Timestamp) })
	// We compare the journals exactly, and before comparing them we sort them
	// in order, because bbolt cursor returns the events in unordered slice.
	// If we don't order both we get an error of the slices not matching.
	if diff := cmp.Diff(expectedJournalEvents, journalEvents, sortEventsByTimestamp); diff != "" {
		t.Fatalf("events mismatch (-want +got):\n%s", diff)
	}
}
