// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
)

// recordEvent appends an event to the journal and applies it to every configured view.
func recordEvent(ctx context.Context, j journal.Journal, views []journalview.View, event journal.Event) error {
	if err := j.Append(ctx, event); err != nil {
		return fmt.Errorf("append event: %w", err)
	}

	for _, view := range views {
		if err := view.Apply(ctx, event); err != nil {
			return fmt.Errorf("apply event to view: %w", err)
		}
	}

	return nil
}

// recordEventAndLog appends an event, applies it to views, and logs the persisted event.
func recordEventAndLog(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	views []journalview.View,
	event journal.Event,
	message string,
	fields ...zap.Field,
) error {
	if err := recordEvent(ctx, j, views, event); err != nil {
		return err
	}

	fields = append(fields,
		zap.String("node_id", event.NodeID.String()),
		zap.String("event_id", event.ID.String()),
		zap.String("event_type", event.Type),
	)
	logger.Info(message, fields...)

	return nil
}
