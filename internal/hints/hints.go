// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package hints carries near instant pull hints over the weld bus.
//
// A hint is a tap on the shoulder, not data. The publisher appends a
// journal event locally, then publishes the origin plus tip event id on
// the hints topic. Subscribers wake their pull loop early and run the
// normal Sync with cursor validation and idempotent apply. A lost,
// duplicated, or forged hint changes timing only. The 5s ticker stays
// the guarantee.
//
// NOTE: production builds must replace github.com/podomy/weld with a
// versioned tag or a vendored copy. The go.mod replace to ../weld is a
// local development pointer only and does not reproduce in clean
// checkouts.
package hints

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/podomy/weld/bus"

	"github.com/podomy/concord/internal/logs"
)

// relayRetryBackoff caps the pause after a persistent TakeBytes failure
// so a broken backend cannot hot spin or flood logs. The wait still
// exits promptly when ctx ends.
const relayRetryBackoff = 50 * time.Millisecond

// TopicName is the bus topic carrying hints.
const TopicName = "/concord/hints"

// TypeName is the bus type name for hint payloads.
const TypeName = "concord__msg__Hint"

// Hint names a journal tip that a peer may fetch.
type Hint struct {
	// Origin is the node that appended the event.
	Origin uuid.UUID
	// Tip is the appended event id.
	Tip uuid.UUID
}

// hintWire is the JSON shape of a hint on the bus.
type hintWire struct {
	Origin string `json:"origin"`
	Tip    string `json:"tip"`
}

// NewChannel returns the coalesced hint channel for one pull loop.
// Size 1 keeps at most one pending wakeup. A full channel means a wakeup
// is already queued, and the queued pull covers everything appended so
// far, so SendHint drops rather than blocks.
func NewChannel() chan Hint {
	return make(chan Hint, 1)
}

// SendHint enqueues one hint without blocking. A hint already queued
// covers the new one, so a full channel keeps its entry and the new
// hint drops. Hints are advisory, the ticker heals any drop.
func SendHint(ch chan<- Hint, h Hint) {
	select {
	case ch <- h:
	default:
	}
}

// Publish serializes one hint onto the bus.
func Publish(ctx context.Context, backend bus.Backend, h Hint) error {
	wire, err := json.Marshal(hintWire{Origin: h.Origin.String(), Tip: h.Tip.String()}) //nolint:errchkjson // two UUID strings, encoding cannot fail
	if err != nil {
		return fmt.Errorf("hints: marshal hint: %w", err)
	}
	msg := bus.OutgoingMessage{
		TopicName: TopicName,
		TypeName:  TypeName,
		Payload:   wire,
		Leg:       bus.LegData,
	}
	err = backend.PublishBytes(ctx, msg)
	if err != nil {
		return fmt.Errorf("hints: publish hint: %w", err)
	}
	return nil
}

// Decode extracts a hint from one bus message. It reports false when
// the message is for another topic or carries a malformed payload, so
// callers skip it and keep waiting.
func Decode(in bus.IncomingMessage) (Hint, bool) {
	if in.TopicName != TopicName {
		return Hint{}, false
	}
	var wire hintWire
	err := json.Unmarshal(in.Payload, &wire)
	if err != nil {
		return Hint{}, false
	}
	origin, err := uuid.Parse(wire.Origin)
	if err != nil {
		return Hint{}, false
	}
	tip, err := uuid.Parse(wire.Tip)
	if err != nil {
		return Hint{}, false
	}
	return Hint{Origin: origin, Tip: tip}, true
}

// RunRelay takes bus messages and forwards hints to out. It blocks
// until ctx ends. Take failures other than cancellation log and retry
// after a short backoff so a broken backend cannot spin or flood logs.
// Malformed or off topic messages skip quietly. Sends never block:
// a queued hint already covers the newcomer.
func RunRelay(ctx context.Context, logger *logs.Logger, backend bus.Backend, out chan<- Hint) {
	for {
		err := ctx.Err()
		if err != nil {
			return
		}
		in, err := backend.TakeBytes(ctx)
		if err != nil {
			err2 := ctx.Err()
			if err2 != nil {
				return
			}
			logger.Warn("hints relay take failed", logs.Error(err))
			timer := time.NewTimer(relayRetryBackoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}
		h, ok := Decode(in)
		if !ok {
			continue
		}
		if h.Origin == uuid.Nil {
			continue
		}
		SendHint(out, h)
	}
}
