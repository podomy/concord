// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
	"github.com/podomy/concord/src/kvstore"
)

func TestRecordEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	j, err := journal.OpenJSONLPath(testJournalPath(t))
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	t.Cleanup(func() {
		if err = j.Close(); err != nil {
			t.Fatalf("close journal: %v", err)
		}
	})

	event := journal.NewEvent(uuid.New(), "node started", json.RawMessage(`{}`))
	st, err := testOpenStores(t)
	if err != nil {
		t.Fatalf("test open stores: %v", err)
	}
	t.Cleanup(func() {
		if err := st.kv.Close(); err != nil {
			t.Fatalf("close kv store: %v", err)
		}
	})

	eventsByID, eventsByNode, eventsByType := newViews(st.kv)
	views := viewList(eventsByID, eventsByNode, eventsByType)

	if err := recordEvent(ctx, j, views, event); err != nil {
		t.Fatalf("record event: %v", err)
	}
	requireRecordedEvent(t, ctx, eventsByID, eventsByNode, eventsByType, event)
}

func TestRecordEventCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	j, err := journal.OpenJSONLPath(testJournalPath(t))
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	t.Cleanup(func() {
		if err := j.Close(); err != nil {
			t.Fatalf("close journal: %v", err)
		}
	})

	if err := recordEvent(ctx, j, nil, journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))); err == nil {
		t.Fatalf("expected record event cancellation error")
	}
}

func TestRebuildViews(t *testing.T) {
	ctx := context.Background()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	j, err := journal.OpenJSONL()
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	event := journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))
	if err = j.Append(ctx, event); err != nil {
		t.Fatalf("append event: %v", err)
	}
	if err = j.Close(); err != nil {
		t.Fatalf("close journal: %v", err)
	}

	st, err := testOpenStores(t)
	if err != nil {
		t.Fatalf("test open stores: %v", err)
	}
	t.Cleanup(func() {
		if err := st.kv.Close(); err != nil {
			t.Fatalf("close kv store: %v", err)
		}
	})

	eventsByID, eventsByNode, eventsByType := newViews(st.kv)
	views := viewList(eventsByID, eventsByNode, eventsByType)
	if err := rebuildViews(ctx, views); err != nil {
		t.Fatalf("rebuild views: %v", err)
	}
	requireRecordedEvent(t, ctx, eventsByID, eventsByNode, eventsByType, event)
}

func TestRebuildViewsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := rebuildViews(ctx, nil); err == nil {
		t.Fatalf("expected rebuild views cancellation error")
	}
}

func requireRecordedEvent(
	t *testing.T,
	ctx context.Context,
	eventsByID *journalview.EventsByID,
	eventsByNode *journalview.EventsByNode,
	eventsByType *journalview.EventsByType,
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

// openStores initialises the bbolt key-value store and the JSONL journal.
func testOpenStores(t *testing.T) (*stores, error) {
	t.Helper()

	kvStore, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		return nil, fmt.Errorf("open test kv store: %w", err)
	}

	return &stores{kv: kvStore}, nil
}

// testJournalPath returns the path used by runtime tests for journal storage.
func testJournalPath(t *testing.T) string {
	t.Helper()

	return filepath.Join(t.TempDir(), "journal.jsonl")
}
