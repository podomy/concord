// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalreader"
	"github.com/podomy/concord/internal/kvstore"
)

// Invariants.
// 1. Only peer.keypinned events affect this view.
// 2. One pin per node ID: the highest generation wins, so rotation
//    supersedes every older pin regardless of arrival order.
// 3. Same generation keeps the existing pin: first-in-replay wins. A second
//    distinct key at one generation only arises under CA misuse; it heals on
//    the next generation bump, which supersedes both.
// 4. Rebuild replays the journal in order and converges with live Apply.
// 5. Malformed peer.keypinned payloads return an error.

const bucketNamePinnedKeys = "pinnedkeys"

// KeyPin is one pinned Noise identity: the peer's node ID, static public key,
// and the rotation generation the key was seen at.
type KeyPin struct {
	NodeID     uuid.UUID `json:"node_id"`
	PublicKey  []byte    `json:"public_key"`
	Generation uint64    `json:"generation"`
}

// PinnedKeys maintains first-seen Noise identities keyed by node ID.
type PinnedKeys struct {
	kvStore *kvstore.KVStore
}

// NewPinnedKeys creates a new PinnedKeys view backed by the given KVStore.
func NewPinnedKeys(kv *kvstore.KVStore) *PinnedKeys {
	return &PinnedKeys{
		kvStore: kv,
	}
}

// putPin stores pin unless a pin at an equal or higher generation is already
// stored for the node.
func (v *PinnedKeys) putPin(b *bolt.Bucket, pin KeyPin) error {
	if len(pin.PublicKey) != 32 {
		return fmt.Errorf("pinned key must be 32 bytes, got %d", len(pin.PublicKey))
	}

	key, err := pin.NodeID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	if stored := b.Get(key); stored != nil {
		var existing KeyPin
		if err := json.Unmarshal(stored, &existing); err != nil {
			return fmt.Errorf("deserialization: %w", err)
		}
		if existing.Generation >= pin.Generation {
			return nil
		}
	}

	serialized, err := json.Marshal(pin)
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	if err := b.Put(key, serialized); err != nil {
		return fmt.Errorf("kv put: %w", err)
	}

	return nil
}

// Apply records the pin carried by a peer.keypinned event, ignoring every
// other event type.
func (v *PinnedKeys) Apply(ctx context.Context, event journal.Event) error {
	if err := checkContext(ctx, "context cancelled"); err != nil {
		return err
	}

	if event.Type != EventTypePeerKeyPinned {
		return nil
	}

	var pin KeyPin
	if err := json.Unmarshal(event.Payload, &pin); err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}

	err := v.kvStore.DB().Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketNamePinnedKeys))
		if err != nil {
			return fmt.Errorf("kv bucket creation: %w", err)
		}

		return v.putPin(b, pin)
	})
	if err != nil {
		return fmt.Errorf("kv update: %w", err)
	}

	return nil
}

// resetBucket deletes the existing pinnedkeys bucket if present and creates a
// fresh one.
func (v *PinnedKeys) resetBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	if err := tx.DeleteBucket([]byte(bucketNamePinnedKeys)); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
		return nil, fmt.Errorf("kv delete bucket: %w", err)
	}

	b, err := tx.CreateBucket([]byte(bucketNamePinnedKeys))
	if err != nil {
		return nil, fmt.Errorf("kv create bucket: %w", err)
	}

	return b, nil
}

// Rebuild reconstructs the PinnedKeys view by resetting the storage bucket
// and replaying all peer.keypinned journal events in order.
func (v *PinnedKeys) Rebuild(ctx context.Context, jr journalreader.Reader) error {
	if err := checkContext(ctx, "context cancelled"); err != nil {
		return err
	}

	err := v.kvStore.DB().Update(func(tx *bolt.Tx) error {
		b, err := v.resetBucket(tx)
		if err != nil {
			return err
		}

		return v.replayEvents(ctx, jr, b)
	})
	if err != nil {
		return fmt.Errorf("kv update: %w", err)
	}

	return nil
}

// replayEvents reads pin events sequentially from the journal reader and
// folds them into the bucket under the view invariants.
func (v *PinnedKeys) replayEvents(ctx context.Context, jr journalreader.Reader, b *bolt.Bucket) error {
	for {
		event, err := readEvent(ctx, jr)
		if err != nil {
			return err
		}
		if event == nil {
			return nil
		}

		if event.Type != EventTypePeerKeyPinned {
			continue
		}

		var pin KeyPin
		if err := json.Unmarshal(event.Payload, &pin); err != nil {
			return fmt.Errorf("json unmarshal: %w", err)
		}

		if err := v.putPin(b, pin); err != nil {
			return fmt.Errorf("put pin: %w", err)
		}
	}
}

// Get returns the pin stored for a node ID, or nil when nothing is pinned.
func (v *PinnedKeys) Get(ctx context.Context, id uuid.UUID) (*KeyPin, error) {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return nil, err
	}

	key, err := id.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serialization: %w", err)
	}

	var pin *KeyPin
	err = v.kvStore.DB().View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNamePinnedKeys))
		if b == nil {
			return nil
		}

		stored := b.Get(key)
		if stored == nil {
			return nil
		}

		var decoded KeyPin
		if err := json.Unmarshal(stored, &decoded); err != nil {
			return fmt.Errorf("deserialization: %w", err)
		}

		pin = &decoded

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kv view: %w", err)
	}

	return pin, nil
}

// RecordKeyPin appends a peer.keypinned event and applies it to views.
func RecordKeyPin(ctx context.Context, j journal.Journal, views []View, selfID uuid.UUID, pin KeyPin) error {
	payload, err := json.Marshal(pin)
	if err != nil {
		return fmt.Errorf("marshal key pin: %w", err)
	}

	event := journal.NewEvent(selfID, EventTypePeerKeyPinned, payload)
	if err := RecordEvent(ctx, j, views, event); err != nil {
		return fmt.Errorf("record key pin: %w", err)
	}

	return nil
}
