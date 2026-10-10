// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/workload"
)

// Journal event types. Every type the fleet writes is named here; no
// producer or view may use a bare type string. Unknown types (a newer
// binary's events replayed on an older one) are always recordable and
// never interpretable: the journal keeps their bytes, typed views skip
// them. Validation is shape, not completeness: missing fields decode to
// zero values, so additive payload evolution never breaks old readers.
const (
	// EventTypeWorkloadSpec is a workload desired-state copy: initial
	// assignment at epoch zero, or an orphan reassignment with the epoch
	// incremented. Payload: workload.Spec.
	EventTypeWorkloadSpec = "workload.spec"
	// EventTypeWorkloadUnhealthy is a signal-action sickness edge.
	// Payload: workload.Unhealthy.
	EventTypeWorkloadUnhealthy = "workload.unhealthy"
	// EventTypeWorkloadInstancePrefix prefixes per-instance lifecycle
	// events (workload.instance.running, workload.instance.stopped).
	// Payload: workload.Instance.
	EventTypeWorkloadInstancePrefix = "workload.instance."
	// EventTypeNodeStarted marks a daemon boot. Payload: NodeStarted.
	EventTypeNodeStarted = "node.started"
	// EventTypePeerSeen, EventTypePeerUpdated, EventTypePeerLost track
	// membership edges observed from memberlist. Payload: PeerEvent.
	EventTypePeerSeen    = "peer.seen"
	EventTypePeerUpdated = "peer.updated"
	EventTypePeerLost    = "peer.lost"
	// EventTypePeerKeyPinned pins a node's first-seen Noise identity.
	// Payload: KeyPin.
	EventTypePeerKeyPinned = "peer.keypinned"
)

// NodeStarted is the payload of a node.started event.
type NodeStarted struct {
	MemberlistAddress string `json:"memberlist_address"`
}

// PeerEvent is the payload of peer membership edge events. Address and
// State stay strings so producers convert once at the edge; the catalog
// never imports peer packages back.
type PeerEvent struct {
	PeerID  uuid.UUID `json:"peer_id"`
	Address string    `json:"address"`
	State   string    `json:"state"`
}

// Known reports whether the type is a catalogued event type. The
// workload instance family matches by prefix; everything else by exact
// type.
func Known(eventType string) bool {
	switch eventType {
	case EventTypeWorkloadSpec,
		EventTypeWorkloadUnhealthy,
		EventTypeNodeStarted,
		EventTypePeerSeen,
		EventTypePeerUpdated,
		EventTypePeerLost,
		EventTypePeerKeyPinned:
		return true
	}
	return strings.HasPrefix(eventType, EventTypeWorkloadInstancePrefix)
}

// ValidatePayload checks a payload against its type's shape by decoding
// into the payload struct. Unknown types always validate: recordability
// must never depend on the reader's version. Malformed JSON fails for
// every type, including unknown ones.
func ValidatePayload(eventType string, payload json.RawMessage) error {
	if !json.Valid(payload) {
		return fmt.Errorf("malformed payload for %q", eventType)
	}
	var target any
	switch {
	case eventType == EventTypeWorkloadSpec:
		target = &workload.Spec{}
	case eventType == EventTypeWorkloadUnhealthy:
		target = &workload.Unhealthy{}
	case strings.HasPrefix(eventType, EventTypeWorkloadInstancePrefix):
		target = &workload.Instance{}
	case eventType == EventTypeNodeStarted:
		target = &NodeStarted{}
	case eventType == EventTypePeerSeen ||
		eventType == EventTypePeerUpdated ||
		eventType == EventTypePeerLost:
		target = &PeerEvent{}
	case eventType == EventTypePeerKeyPinned:
		target = &KeyPin{}
	default:
		return nil
	}
	if err := json.Unmarshal(payload, target); err != nil {
		return fmt.Errorf("payload shape for %q: %w", eventType, err)
	}
	return nil
}
