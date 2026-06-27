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

	"github.com/podomy/hive/src/journal"
	"github.com/podomy/hive/src/journalreader"
	"github.com/podomy/hive/src/kvstore"
)

const bucketNameEventsByType = "eventsbytype"

type EventsByType struct {
	kv kvstore.KVStore
}

type EventsByTypeKey struct {
	EventType string    `json:"event_type"`
	ID        uuid.UUID `json:"id"`
}

func (e *EventsByType) putEvent(b *bolt.Bucket, event journal.Event) error {
	key := EventsByTypeKey{
		EventType: event.Type,
		ID:        event.ID,
	}

	serializedKey, err := json.Marshal(key)
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	serializedEvent, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	err = b.Put(serializedKey, serializedEvent)
	if err != nil {
		return fmt.Errorf("kv put: %w", err)
	}

	return nil
}

//nolint:dupl // Projection methods intentionally keep bucket-specific logic local.
func (e *EventsByType) Apply(ctx context.Context, event journal.Event) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	default:
	}

	kv := e.kv.DB()

	err := kv.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketNameEventsByType))
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

func (e *EventsByType) resetBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	if err := tx.DeleteBucket([]byte(bucketNameEventsByType)); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
		return nil, fmt.Errorf("kv delete bucket: %w", err)
	}

	b, err := tx.CreateBucket([]byte(bucketNameEventsByType))
	if err != nil {
		return nil, fmt.Errorf("kv create bucket: %w", err)
	}

	return b, nil
}

func (e *EventsByType) replayEvents(ctx context.Context, jr journalreader.Reader, b *bolt.Bucket) error {
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
func (e *EventsByType) Rebuild(ctx context.Context, jr journalreader.Reader) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	default:
	}

	kv := e.kv.DB()

	err := kv.Update(func(tx *bolt.Tx) error {
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
