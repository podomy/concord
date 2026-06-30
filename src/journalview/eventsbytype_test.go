// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//nolint:dupl // Projection tests intentionally keep view-specific setup and assertions local.
package journalview

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/hive/src/journal"
)

func TestEventsByTypeGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewEventsByType(kv)

	event := journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))

	if err := view.Apply(ctx, event); err != nil {
		t.Fatalf("apply event: %v", err)
	}

	got, err := view.Get(ctx, event.Type, event.ID)
	if err != nil {
		t.Fatalf("get event: %v", err)
	}
	if got == nil {
		t.Fatalf("expected event got nil")
	}
	if got.Type != event.Type {
		t.Fatalf("expected event type %s, got %s", event.Type, got.Type)
	}
	if got.ID != event.ID {
		t.Fatalf("expected event ID %s, got %s", event.ID, got.ID)
	}
}
