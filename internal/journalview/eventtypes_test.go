// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"encoding/json"
	"testing"
)

// Every production event type is known; near-misses and future types
// are not, and that must stay a readable list, not folklore.
func TestKnown(t *testing.T) {
	t.Parallel()

	known := []string{
		EventTypeWorkloadSpec,
		EventTypeWorkloadUnhealthy,
		EventTypeWorkloadInstancePrefix + "running",
		EventTypeNodeStarted,
		EventTypePeerSeen,
		EventTypePeerUpdated,
		EventTypePeerLost,
		EventTypePeerKeyPinned,
	}
	for _, typ := range known {
		if !Known(typ) {
			t.Errorf("Known(%q) = false", typ)
		}
	}

	for _, typ := range []string{"", "node started", "workload", "workload.speculative", "peer.keypinned.v2", "test.event"} {
		if Known(typ) {
			t.Errorf("Known(%q) = true", typ)
		}
	}
}

// Each family decodes into its payload struct; malformed JSON fails for
// every type including unknown ones; unknown types with valid JSON pass
// so old readers keep recording new writers' events.
func TestValidatePayload(t *testing.T) {
	t.Parallel()

	valid := map[string]json.RawMessage{
		EventTypeWorkloadSpec:                 json.RawMessage(`{"ID":"3a2fb4cf-e9d0-4b80-bc1b-c6448c938a77"}`),
		EventTypeWorkloadUnhealthy:            json.RawMessage(`{"WorkloadID":"3a2fb4cf-e9d0-4b80-bc1b-c6448c938a77"}`),
		EventTypeWorkloadInstancePrefix + "x": json.RawMessage(`{"SpecID":"3a2fb4cf-e9d0-4b80-bc1b-c6448c938a77"}`),
		EventTypeNodeStarted:                  json.RawMessage(`{"memberlist_address":"0.0.0.0:7946"}`),
		EventTypePeerSeen:                     json.RawMessage(`{"peer_id":"3a2fb4cf-e9d0-4b80-bc1b-c6448c938a77"}`),
		EventTypePeerKeyPinned:                json.RawMessage(`{"generation":3}`),
		"future.type":                         json.RawMessage(`{"anything":true}`),
	}
	for typ, payload := range valid {
		if err := ValidatePayload(typ, payload); err != nil {
			t.Errorf("ValidatePayload(%q) = %v", typ, err)
		}
	}

	invalid := map[string]json.RawMessage{
		EventTypeWorkloadSpec:      json.RawMessage(`[1,2]`),
		EventTypeWorkloadUnhealthy: json.RawMessage(`"nope"`),
		EventTypePeerKeyPinned:     json.RawMessage(`{"generation":"three"}`),
		EventTypeNodeStarted:       json.RawMessage(`{`),
		"future.type":              json.RawMessage(`{`),
	}
	for typ, payload := range invalid {
		if err := ValidatePayload(typ, payload); err == nil {
			t.Errorf("ValidatePayload(%q) = nil, want error", typ)
		}
	}
}
