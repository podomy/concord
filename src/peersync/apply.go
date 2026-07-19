// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
)

// EventByID can check whether this node already stored an event id.
// That is all apply needs for dedup. *journalview.EventsByID implements it
// (Get returns nil, nil when the id is unknown).
type EventByID interface {
	Get(ctx context.Context, id uuid.UUID) (*journal.Event, error)
}

// ApplyEvents appends peer events into the local journal and views, skipping
// any event whose id is already present (idempotent under pull replay).
// Returns how many events were newly applied.
func ApplyEvents(
	ctx context.Context,
	j journal.Journal,
	views []journalview.View,
	byID EventByID,
	events []journal.Event,
) (applied int, err error) {
	for _, event := range events {
		existing, err := byID.Get(ctx, event.ID)
		if err != nil {
			return applied, fmt.Errorf("lookup event %s: %w", event.ID, err)
		}
		if existing != nil {
			continue
		}
		if err := journalview.RecordEvent(ctx, j, views, event); err != nil {
			return applied, fmt.Errorf("record event %s: %w", event.ID, err)
		}
		applied++
	}
	return applied, nil
}
