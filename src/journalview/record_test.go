// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/kvstore"
)

func TestRecordEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	j, err := journal.OpenJSONLPath(filepath.Join(t.TempDir(), "journal.jsonl"))
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	t.Cleanup(func() {
		if err = j.Close(); err != nil {
			t.Fatalf("close journal: %v", err)
		}
	})

	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Fatalf("close kv: %v", err)
		}
	})

	event := journal.NewEvent(uuid.New(), "node started", json.RawMessage(`{}`))

	eventsByID := NewEventsByID(kv)
	eventsByNode := NewEventsByNode(kv)
	eventsByType := NewEventsByType(kv)
	views := []View{eventsByID, eventsByNode, eventsByType}

	if err := RecordEvent(ctx, j, views, event); err != nil {
		t.Fatalf("record event: %v", err)
	}
	requireRecordedEvent(t, ctx, eventsByID, eventsByNode, eventsByType, event)
}

func TestRecordEventCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	j, err := journal.OpenJSONLPath(filepath.Join(t.TempDir(), "journal.jsonl"))
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	t.Cleanup(func() {
		if err := j.Close(); err != nil {
			t.Fatalf("close journal: %v", err)
		}
	})

	if err := RecordEvent(ctx, j, nil, journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))); err == nil {
		t.Fatalf("expected record event cancellation error")
	}
}

func requireRecordedEvent(
	t *testing.T,
	ctx context.Context,
	eventsByID *EventsByID,
	eventsByNode *EventsByNode,
	eventsByType *EventsByType,
	event journal.Event,
) {
	t.Helper()

	gotByID, err := eventsByID.Get(ctx, event.ID)
	if err != nil {
		t.Fatalf("get event by id: %v", err)
	}
	if gotByID == nil {
		t.Fatalf("expected event by id got nil")
	}
	if gotByID.ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, gotByID.ID)
	}

	gotByNode, err := eventsByNode.Get(ctx, event.NodeID, event.ID)
	if err != nil {
		t.Fatalf("get event by node: %v", err)
	}
	if gotByNode == nil {
		t.Fatalf("expected event by node got nil")
	}
	if gotByNode.ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, gotByNode.ID)
	}

	gotByType, err := eventsByType.Get(ctx, event.Type, event.ID)
	if err != nil {
		t.Fatalf("get event by type: %v", err)
	}
	if gotByType == nil {
		t.Fatalf("expected event by type got nil")
	}
	if gotByType.ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, gotByType.ID)
	}
}
