// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journal

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID        uuid.UUID       `json:"id"`
	Type      string          `json:"type"`
	NodeID    uuid.UUID       `json:"node_id"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

// NewEvent creates a new Event with a random ID, the current UTC timestamp, and the given parameters.
func NewEvent(nodeID uuid.UUID, eventType string, payload json.RawMessage) Event {
	return Event{
		ID:        uuid.New(),
		Type:      eventType,
		NodeID:    nodeID,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

// Journal is the interface for appending events to a persistent event log.
type Journal interface {
	Append(ctx context.Context, event Event) error
}
