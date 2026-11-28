// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package journalview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sync"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/journal"
)

// RecordNodeStarted creates a node.started event and persists it.
func RecordNodeStarted(
	ctx context.Context,
	logger *logs.Logger,
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
		logs.String("memberlist_address", memberlistAddress.String()),
	); err != nil {
		return fmt.Errorf("append startup event: %w", err)
	}

	return nil
}

// notifyMu guards notifyFn so setting and clearing the observer races
// nothing with the RecordEvent fast path. Runtime sets before loops
// start and clears after they drain; the mutex only orders the rare
// writes against concurrent reads.
var notifyMu sync.RWMutex

// notifyFn observes locally originated journal events after they commit.
// Runtime sets it to publish a pull hint on the weld bus. It runs on the
// recording goroutine, so it must never block. ApplyEvents uses
// RecordEventSilent and never triggers it, which keeps relayed events
// from rehinting. Nil means no hints.
var notifyFn func(event journal.Event)

// SetNotify installs the observer for locally recorded events. A nil
// function disables hints. Call before background loops start.
func SetNotify(fn func(event journal.Event)) {
	notifyMu.Lock()
	defer notifyMu.Unlock()
	notifyFn = fn
}

// ClearNotify disables the observer. Call after background loops drain.
func ClearNotify() {
	notifyMu.Lock()
	defer notifyMu.Unlock()
	notifyFn = nil
}

// loadNotify returns the installed observer without holding the lock
// across the call. Nil means hints are disabled.
func loadNotify() func(event journal.Event) {
	notifyMu.RLock()
	defer notifyMu.RUnlock()
	return notifyFn
}

// RecordEventSilent appends an event to the journal and applies it to every configured view.
// Known-type payloads are shape-checked first so producer bugs fail loudly at
// write time instead of diverging views silently. Unknown types always
// record: the journal is a complete log regardless of reader version,
// and typed views skip what they do not know.
func RecordEventSilent(ctx context.Context, j journal.Journal, views []View, event journal.Event) error {
	if err := ValidatePayload(event.Type, event.Payload); err != nil {
		return fmt.Errorf("validate event payload: %w", err)
	}
	if err := j.Append(ctx, event); err != nil {
		return fmt.Errorf("append event: %w", err)
	}

	var applyErr error
	for _, view := range views {
		err := view.Apply(ctx, event)
		if err != nil {
			applyErr = errors.Join(applyErr, fmt.Errorf("apply event to view: %w", err))
		}
	}
	if applyErr != nil {
		return fmt.Errorf("apply event to views: %w", applyErr)
	}

	return nil
}

// RecordEvent appends an event like RecordEventSilent, then observes it
// through the Notify observer for hint publication. Relay apply must use
// RecordEventSilent instead so one event hints once, at its origin.
func RecordEvent(ctx context.Context, j journal.Journal, views []View, event journal.Event) error {
	err := RecordEventSilent(ctx, j, views, event)
	if err != nil {
		return err
	}
	fn := loadNotify()
	if fn != nil {
		fn(event)
	}
	return nil
}

// RecordEventAndLog appends an event, applies it to views, and logs the persisted event.
func RecordEventAndLog(
	ctx context.Context,
	logger *logs.Logger,
	j journal.Journal,
	views []View,
	event journal.Event,
	message string,
	fields ...logs.Attr,
) error {
	err := RecordEvent(ctx, j, views, event)
	if err != nil {
		return fmt.Errorf("record event: %w", err)
	}

	fields = append(fields,
		logs.String("node_id", event.NodeID.String()),
		logs.String("event_id", event.ID.String()),
		logs.String("event_type", event.Type),
	)
	logger.Info(message, fields...)

	return nil
}
