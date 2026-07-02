// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalreader"
	"github.com/podomy/concord/src/kvstore"
)

const bucketNameEventsByType = "eventsbytype"

type EventsByType struct {
	kv *kvstore.KVStore
}

type EventsByTypeKey struct {
	EventType string    `json:"event_type"`
	ID        uuid.UUID `json:"id"`
}

func NewEventsByType(kv *kvstore.KVStore) *EventsByType {
	return &EventsByType{
		kv: kv,
	}
}

func (e *EventsByType) putEvent(b *bolt.Bucket, event journal.Event) error {
	serializedEventType := []byte(event.Type)

	serializedID, err := event.ID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	key := make([]byte, 0, len(serializedEventType)+len(serializedID))
	key = append(key, serializedEventType...)
	// a separator, while UUID is a fixed 16 byte type,
	// the string can be arbitrarily large, we need something
	// to know when it ends
	key = append(key, 0)
	key = append(key, serializedID...)

	serializedEvent, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	err = b.Put(key, serializedEvent)
	if err != nil {
		return fmt.Errorf("kv put: %w", err)
	}

	return nil
}

//nolint:dupl // Projection methods intentionally keep bucket-specific logic local.
func (e *EventsByType) Apply(ctx context.Context, event journal.Event) error {
	if err := checkContext(ctx, "context cancelled"); err != nil {
		return err
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
	if err := checkContext(ctx, "context cancelled"); err != nil {
		return err
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

func (e *EventsByType) Get(ctx context.Context, eventType string, id uuid.UUID) (*journal.Event, error) {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return nil, err
	}

	serializedType := []byte(eventType)
	serializedID, err := id.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serialization: %w", err)
	}

	key := make([]byte, 0, len(serializedType)+len(serializedID))
	key = append(key, serializedType...)
	// separator for the string type, because it can be
	// arbitrarily large
	key = append(key, 0)
	key = append(key, serializedID...)

	kv := e.kv.DB()

	var event *journal.Event
	err = kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameEventsByType))
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
func (e *EventsByType) List(ctx context.Context, eventType string) ([]journal.Event, error) {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return nil, err
	}

	serializedType := []byte(eventType)

	prefix := make([]byte, 0, len(serializedType)+1)
	prefix = append(prefix, serializedType...)
	// separator for the string type, because it can be
	// arbitrarily large
	prefix = append(prefix, 0)

	kv := e.kv.DB()

	events := []journal.Event{}
	err := kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameEventsByType))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			var event journal.Event
			err := json.Unmarshal(v, &event)
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
