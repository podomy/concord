// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/journal"
)

// pinEvent builds a peer.keypinned event for tests.
func pinEvent(nodeID uuid.UUID, pub []byte, generation uint64) journal.Event {
	payload, err := json.Marshal(KeyPin{NodeID: nodeID, PublicKey: pub, Generation: generation})
	if err != nil {
		panic(err)
	}
	return journal.Event{ID: uuid.New(), Type: EventTypePeerKeyPinned, NodeID: uuid.New(), Payload: payload}
}

func TestPinnedKeysFirstPinSticks(t *testing.T) {
	t.Parallel()

	view := NewPinnedKeys(testKVStore(t))
	ctx := context.Background()
	id := uuid.New()
	pub := bytes.Repeat([]byte{0x07}, 32)

	if err := view.Apply(ctx, pinEvent(id, pub, 0)); err != nil {
		t.Fatalf("apply pin: %v", err)
	}

	got, err := view.Get(ctx, id)
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if got == nil {
		t.Fatal("expected pin, got none")
	}
	if !bytes.Equal(got.PublicKey, pub) || got.Generation != 0 {
		t.Fatalf("pin = %+v, want gen 0 test key", got)
	}
}

func TestPinnedKeysHigherGenerationReplaces(t *testing.T) {
	t.Parallel()

	view := NewPinnedKeys(testKVStore(t))
	ctx := context.Background()
	id := uuid.New()

	if err := view.Apply(ctx, pinEvent(id, bytes.Repeat([]byte{0x07}, 32), 0)); err != nil {
		t.Fatalf("apply gen 0: %v", err)
	}
	if err := view.Apply(ctx, pinEvent(id, bytes.Repeat([]byte{0x08}, 32), 1)); err != nil {
		t.Fatalf("apply gen 1: %v", err)
	}

	got, err := view.Get(ctx, id)
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if got.Generation != 1 || !bytes.Equal(got.PublicKey, bytes.Repeat([]byte{0x08}, 32)) {
		t.Fatalf("pin = %+v, want gen 1 key", got)
	}
}

func TestPinnedKeysSameGenerationKeepsExisting(t *testing.T) {
	t.Parallel()

	view := NewPinnedKeys(testKVStore(t))
	ctx := context.Background()
	id := uuid.New()
	first := bytes.Repeat([]byte{0x07}, 32)

	if err := view.Apply(ctx, pinEvent(id, first, 0)); err != nil {
		t.Fatalf("apply first: %v", err)
	}
	if err := view.Apply(ctx, pinEvent(id, bytes.Repeat([]byte{0x08}, 32), 0)); err != nil {
		t.Fatalf("apply second: %v", err)
	}

	got, err := view.Get(ctx, id)
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if !bytes.Equal(got.PublicKey, first) {
		t.Fatal("same-generation pin replaced the first pin")
	}
}

func TestPinnedKeysLowerGenerationIgnored(t *testing.T) {
	t.Parallel()

	view := NewPinnedKeys(testKVStore(t))
	ctx := context.Background()
	id := uuid.New()

	if err := view.Apply(ctx, pinEvent(id, bytes.Repeat([]byte{0x08}, 32), 1)); err != nil {
		t.Fatalf("apply gen 1: %v", err)
	}
	if err := view.Apply(ctx, pinEvent(id, bytes.Repeat([]byte{0x07}, 32), 0)); err != nil {
		t.Fatalf("apply gen 0: %v", err)
	}

	got, err := view.Get(ctx, id)
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if got.Generation != 1 {
		t.Fatalf("generation = %d, want 1", got.Generation)
	}
}

func TestPinnedKeysIgnoresOtherTypes(t *testing.T) {
	t.Parallel()

	view := NewPinnedKeys(testKVStore(t))
	ctx := context.Background()

	event := journal.Event{ID: uuid.New(), Type: EventTypeWorkloadSpec, NodeID: uuid.New()}
	if err := view.Apply(ctx, event); err != nil {
		t.Fatalf("apply other type: %v", err)
	}

	got, err := view.Get(ctx, uuid.New())
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if got != nil {
		t.Fatalf("expected no pin, got %+v", got)
	}
}

func TestPinnedKeysRejectsShortKey(t *testing.T) {
	t.Parallel()

	view := NewPinnedKeys(testKVStore(t))

	if err := view.Apply(context.Background(), pinEvent(uuid.New(), []byte("short"), 0)); err == nil {
		t.Fatal("short key accepted")
	}
}
