// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/journal"
)

// RecordNodeStarted creates a node.started event and persists it.
func RecordNodeStarted(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	views []View,
	nodeID uuid.UUID,
	memberlistAddress netip.AddrPort,
) error {
	payload, err := json.Marshal(NodeStarted{MemberlistAddress: memberlistAddress.String()}) //nolint:errchkjson // checked for symmetry with fallible producers
	if err != nil {
		return fmt.Errorf("marshal startup event: %w", err)
	}

	event := journal.NewEvent(nodeID, EventTypeNodeStarted, payload)
	if err := RecordEventAndLog(ctx, logger, j, views, event, "node runtime started",
		zap.String("memberlist_address", memberlistAddress.String()),
	); err != nil {
		return fmt.Errorf("append startup event: %w", err)
	}

	return nil
}

// RecordEvent appends an event to the journal and applies it to every configured view.
// Known-type payloads are shape-checked first so producer bugs fail loudly at
// write time instead of diverging views silently. Unknown types always
// record: the journal is a complete log regardless of reader version,
// and typed views skip what they do not know.
func RecordEvent(ctx context.Context, j journal.Journal, views []View, event journal.Event) error {
	if err := ValidatePayload(event.Type, event.Payload); err != nil {
		return fmt.Errorf("validate event payload: %w", err)
	}
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

// RecordEventAndLog appends an event, applies it to views, and logs the persisted event.
func RecordEventAndLog(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	views []View,
	event journal.Event,
	message string,
	fields ...zap.Field,
) error {
	if err := RecordEvent(ctx, j, views, event); err != nil {
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
