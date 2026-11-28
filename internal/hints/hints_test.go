// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package hints_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/weld/bus"

	"github.com/podomy/concord/internal/hints"
	"github.com/podomy/concord/internal/logs"
)

// openTestBackend returns a joined in-memory backend.
func openTestBackend(t *testing.T) bus.Backend {
	t.Helper()
	ctx := context.Background()
	info := bus.DiscoveryInfo{
		ParticipantName: "concord-hints-test",
		DomainID:        0,
		Range:           bus.DiscoveryRangeLocalhost,
	}
	backend, err := bus.OpenMQTT(ctx, bus.DefaultMQTTConfig(), info)
	if err != nil {
		t.Fatalf("open mqtt: %v", err)
	}
	t.Cleanup(func() {
		err := backend.Close()
		if err != nil {
			t.Errorf("close backend: %v", err)
		}
	})
	return backend
}

// A published hint takes back with origin and tip intact.
func TestHintBusRoundTrip(t *testing.T) {
	t.Parallel()
	backend := openTestBackend(t)
	want := hints.Hint{Origin: uuid.New(), Tip: uuid.New()}
	err := hints.Publish(context.Background(), backend, want)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	in, err := backend.TakeBytes(ctx)
	if err != nil {
		t.Fatalf("take: %v", err)
	}
	got, ok := hints.Decode(in)
	if !ok {
		t.Fatalf("decode rejected own publish on %q", in.TopicName)
	}
	if got != want {
		t.Fatalf("hint = %+v, want %+v", got, want)
	}
}

// Off topic messages never decode as hints.
func TestHintDecodeSkipsForeignTopic(t *testing.T) {
	t.Parallel()
	in := bus.IncomingMessage{TopicName: "/other", TypeName: "x", Payload: []byte(`{}`)}
	if _, ok := hints.Decode(in); ok {
		t.Fatal("foreign topic decoded as hint")
	}
}

// A queued hint covers the newcomer, so sends never block.
func TestHintSendCoalesced(t *testing.T) {
	t.Parallel()
	ch := hints.NewChannel()
	first := hints.Hint{Origin: uuid.New(), Tip: uuid.New()}
	hints.SendHint(ch, first)
	hints.SendHint(ch, hints.Hint{Origin: uuid.New(), Tip: uuid.New()})
	select {
	case got := <-ch:
		if got != first {
			t.Fatalf("kept = %+v, want first %+v", got, first)
		}
	default:
		t.Fatal("hint channel empty after send")
	}
}

// waitRelayHint waits for one relayed hint or fails.
func waitRelayHint(t *testing.T, ch <-chan hints.Hint, what string) hints.Hint {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(2 * time.Second):
		t.Fatalf("relay did not forward %s", what)
		return hints.Hint{}
	}
}

// RunRelay forwards valid hints, skips foreign, malformed, and nil
// origin messages, and exits promptly when ctx ends.
func TestHintRelayForwardsValidSkipsRest(t *testing.T) {
	t.Parallel()
	backend := openTestBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan hints.Hint, 8)
	done := make(chan struct{})
	go func() {
		defer close(done)
		hints.RunRelay(ctx, logs.NewNop(), backend, out)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("RunRelay did not stop")
		}
	}()

	publishCtx := context.Background()

	first := hints.Hint{Origin: uuid.New(), Tip: uuid.New()}
	err := hints.Publish(publishCtx, backend, first)
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	got := waitRelayHint(t, out, "first valid hint")
	if got != first {
		t.Fatalf("first hint = %+v, want %+v", got, first)
	}

	foreign := bus.OutgoingMessage{
		TopicName: "/other",
		TypeName:  "x",
		Payload:   []byte(`{}`),
		Leg:       bus.LegData,
	}
	err = backend.PublishBytes(publishCtx, foreign)
	if err != nil {
		t.Fatalf("publish foreign: %v", err)
	}
	malformed := bus.OutgoingMessage{
		TopicName: hints.TopicName,
		TypeName:  hints.TypeName,
		Payload:   []byte(`{bad`),
		Leg:       bus.LegData,
	}
	err = backend.PublishBytes(publishCtx, malformed)
	if err != nil {
		t.Fatalf("publish malformed: %v", err)
	}
	nilOrigin := hints.Hint{Origin: uuid.Nil, Tip: uuid.New()}
	err = hints.Publish(publishCtx, backend, nilOrigin)
	if err != nil {
		t.Fatalf("publish nil origin: %v", err)
	}

	second := hints.Hint{Origin: uuid.New(), Tip: uuid.New()}
	err = hints.Publish(publishCtx, backend, second)
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	got = waitRelayHint(t, out, "second valid hint after skips")
	if got != second {
		t.Fatalf("second hint = %+v, want %+v", got, second)
	}

	select {
	case extra := <-out:
		t.Fatalf("unexpected extra hint: %+v", extra)
	default:
	}
}

// The production DDS path constructs without network. MQTT covers the
// in-memory take path above, so this checks NewDDS validates config and
// OpenDDS failure stays wrapped for the ticker only fallback.
func TestDDSBackendConstructionValidatesConfig(t *testing.T) {
	t.Parallel()
	cfg := bus.DefaultDDSConfig()
	backend, err := bus.NewDDS(cfg)
	if err != nil {
		t.Fatalf("new dds with default config: %v", err)
	}
	err = backend.Close()
	if err != nil {
		t.Fatalf("close dds backend: %v", err)
	}

	bad := cfg
	bad.DomainID = bus.MaxDomainID + 1
	_, err = bus.NewDDS(bad)
	if err == nil {
		t.Fatal("expected NewDDS to reject domain above maximum")
	}
	if !strings.Contains(err.Error(), "dds:") {
		t.Fatalf("dds validation error should wrap dds cause, got %v", err)
	}

	info := bus.DiscoveryInfo{
		ParticipantName: "concord-hints-dds-test",
		DomainID:        0,
		Range:           bus.DiscoveryRangeLocalhost,
	}
	_, err = bus.OpenDDS(context.Background(), bad, info)
	if err == nil {
		t.Fatal("expected OpenDDS to fail on invalid config")
	}
}
