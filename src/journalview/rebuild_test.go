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

	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Fatalf("close kv: %v", err)
		}
	})

	eventsByID := NewEventsByID(kv)
	eventsByNode := NewEventsByNode(kv)
	eventsByType := NewEventsByType(kv)
	views := []View{eventsByID, eventsByNode, eventsByType}

	if err := RebuildViews(ctx, views); err != nil {
		t.Fatalf("rebuild views: %v", err)
	}
	requireRecordedEvent(t, ctx, eventsByID, eventsByNode, eventsByType, event)
}

func TestRebuildViewsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := RebuildViews(ctx, nil); err == nil {
		t.Fatalf("expected rebuild views cancellation error")
	}
}
