// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"fmt"

	"github.com/podomy/concord/src/journalreader"
	"github.com/podomy/concord/src/journalview"
	"github.com/podomy/concord/src/kvstore"
)

// newViews initializes all journal views backed by the provided key-value store.
func newViews(kv *kvstore.KVStore) (*journalview.EventsByID, *journalview.EventsByNode, *journalview.EventsByType) {
	return journalview.NewEventsByID(kv), journalview.NewEventsByNode(kv), journalview.NewEventsByType(kv)
}

// viewList returns views as the common View interface.
func viewList(eventsByID *journalview.EventsByID, eventsByNode *journalview.EventsByNode, eventsByType *journalview.EventsByType) []journalview.View {
	return []journalview.View{eventsByID, eventsByNode, eventsByType}
}

// rebuildViews reconstructs each view from the journal using a fresh reader per view.
func rebuildViews(ctx context.Context, views []journalview.View) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancellation: %w", ctx.Err())
	default:
	}

	for _, view := range views {
		if err := rebuildView(ctx, view); err != nil {
			return err
		}
	}

	return nil
}

// rebuildView reconstructs one view from a fresh journal reader.
func rebuildView(ctx context.Context, view journalview.View) error {
	jr, err := journalreader.OpenJSONLReader()
	if err != nil {
		return fmt.Errorf("open journal reader: %w", err)
	}

	if err = view.Rebuild(ctx, jr); err != nil {
		if closeErr := jr.Close(); closeErr != nil {
			return fmt.Errorf("view rebuild: %w; close journal reader: %w", err, closeErr)
		}

		return fmt.Errorf("view rebuild: %w", err)
	}

	if err = jr.Close(); err != nil {
		return fmt.Errorf("close journal reader: %w", err)
	}

	return nil
}
