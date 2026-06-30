// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/google/uuid"

	"github.com/podomy/hive/src/journal"
	"github.com/podomy/hive/src/journalreader"
	"github.com/podomy/hive/src/kvstore"
)

const bucketNameEventsByNode = "eventsbynode"

type EventsByNode struct {
	kvStore *kvstore.KVStore
}

type EventsByNodeKey struct {
	NodeID uuid.UUID `json:"node_id"`
	ID     uuid.UUID `json:"id"`
}

func NewEventsByNode(kv *kvstore.KVStore) *EventsByNode {
	return &EventsByNode{
		kvStore: kv,
	}
}

func (e *EventsByNode) putEvent(b *bolt.Bucket, event journal.Event) error {
	serializedNodeID, err := event.NodeID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	serializedID, err := event.ID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	key := make([]byte, 0, len(serializedNodeID)+len(serializedID))
	key = append(key, serializedNodeID...)
	key = append(key, serializedID...)

	serializedEvent, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	err = b.Put(key, serializedEvent)
	if err != nil {
		return fmt.Errorf("bucket put kv: %w", err)
	}

	return nil
}

//nolint:dupl // Projection methods intentionally keep bucket-specific logic local.
func (e *EventsByNode) Apply(ctx context.Context, event journal.Event) error {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return err
	}

	db := e.kvStore.DB()
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketNameEventsByNode))
		if err != nil {
			return fmt.Errorf("kv bucket creation: %w", err)
		}

		return e.putEvent(b, event)
	})
	if err != nil {
		return fmt.Errorf("kv update: %w", err)
	}

	return nil
}

func (e *EventsByNode) resetBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	if err := tx.DeleteBucket([]byte(bucketNameEventsByNode)); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
		return nil, fmt.Errorf("kv delete bucket: %w", err)
	}

	b, err := tx.CreateBucket([]byte(bucketNameEventsByNode))
	if err != nil {
		return nil, fmt.Errorf("kv create bucket: %w", err)
	}

	return b, nil
}

func (e *EventsByNode) replayEvents(ctx context.Context, jr journalreader.Reader, b *bolt.Bucket) error {
	for {
		event, err := readEvent(ctx, jr)
		if err != nil {
			return err
		}
		if event == nil {
			return nil
		}

		if err = e.putEvent(b, *event); err != nil {
			return fmt.Errorf("put event: %w", err)
		}
	}
}

//nolint:dupl // Projection methods intentionally keep rebuild flow local to each view.
func (e *EventsByNode) Rebuild(ctx context.Context, jr journalreader.Reader) error {
	if err := checkContext(ctx, "context cancelled"); err != nil {
		return err
	}

	db := e.kvStore.DB()
	err := db.Update(func(tx *bolt.Tx) error {
		b, err := e.resetBucket(tx)
		if err != nil {
			return err
		}

		return e.replayEvents(ctx, jr, b)
	})
	if err != nil {
		return fmt.Errorf("kv update: %w", err)
	}

	return nil
}

func (e *EventsByNode) Get(ctx context.Context, nodeID, id uuid.UUID) (*journal.Event, error) {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return nil, err
	}

	serializedNodeID, err := nodeID.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serialization: %w", err)
	}
	serializedID, err := id.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serialization: %w", err)
	}

	key := make([]byte, 0, len(serializedNodeID)+len(serializedID))
	key = append(key, serializedNodeID...)
	key = append(key, serializedID...)

	kv := e.kvStore.DB()

	var event *journal.Event
	err = kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameEventsByNode))
		if b == nil {
			return nil
		}

		serializedEvent := b.Get(key)
		if serializedEvent == nil {
			return nil
		}

		var decoded journal.Event
		err = json.Unmarshal(serializedEvent, &decoded)
		if err != nil {
			return fmt.Errorf("deserialization: %w", err)
		}

		event = &decoded

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kv view: %w", err)
	}

	return event, nil
}

//nolint:dupl // Projection list methods intentionally keep bucket-specific cursor logic local.
func (e *EventsByNode) List(ctx context.Context, nodeID uuid.UUID) ([]journal.Event, error) {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return nil, err
	}

	serializedNodeID, err := nodeID.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serialization: %w", err)
	}

	prefix := serializedNodeID

	kv := e.kvStore.DB()

	events := []journal.Event{}
	err = kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameEventsByNode))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var event journal.Event
			err = json.Unmarshal(v, &event)
			if err != nil {
				return fmt.Errorf("deserialization: %w", err)
			}

			events = append(events, event)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kv view: %w", err)
	}

	return events, nil
}
