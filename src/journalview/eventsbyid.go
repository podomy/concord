// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalreader"
	"github.com/podomy/concord/src/kvstore"
)

const bucketNameEventsByID = "eventsbyid"

type EventsByID struct {
	kvStore *kvstore.KVStore
}

func NewEventsByID(kv *kvstore.KVStore) *EventsByID {
	return &EventsByID{
		kvStore: kv,
	}
}

func (e *EventsByID) putEvent(b *bolt.Bucket, event journal.Event) error {
	serializedEvent, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	serializedEventID, err := event.ID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	key := make([]byte, 0, len(serializedEventID))
	key = append(key, serializedEventID...)

	err = b.Put(key, serializedEvent)
	if err != nil {
		return fmt.Errorf("bucket put kv: %w", err)
	}

	return nil
}

//nolint:dupl // Projection methods intentionally keep bucket-specific logic local.
func (e *EventsByID) Apply(ctx context.Context, event journal.Event) error {
	if err := checkContext(ctx, "context cancelation"); err != nil {
		return err
	}

	kv := e.kvStore.DB()

	err := kv.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketNameEventsByID))
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

func (e *EventsByID) resetBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	if err := tx.DeleteBucket([]byte(bucketNameEventsByID)); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
		return nil, fmt.Errorf("kv bucket deletion: %w", err)
	}

	b, err := tx.CreateBucket([]byte(bucketNameEventsByID))
	if err != nil {
		return nil, fmt.Errorf("kv bucket creation: %w", err)
	}

	return b, nil
}

func (e *EventsByID) replayEvents(ctx context.Context, jr journalreader.Reader, b *bolt.Bucket) error {
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

func readEvent(ctx context.Context, jr journalreader.Reader) (*journal.Event, error) {
	if err := checkContext(ctx, "context cancelation during rebuild"); err != nil {
		return nil, err
	}

	event, err := jr.Read(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading error: %w", err)
	}

	return event, nil
}

//nolint:dupl // Projection methods intentionally keep rebuild flow local to each view.
func (e *EventsByID) Rebuild(ctx context.Context, jr journalreader.Reader) error {
	if err := checkContext(ctx, "context cancelation"); err != nil {
		return err
	}

	kv := e.kvStore.DB()

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

func (e *EventsByID) Get(ctx context.Context, id uuid.UUID) (*journal.Event, error) {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return nil, err
	}

	serializedID, err := id.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("serialization: %w", err)
	}

	key := make([]byte, 0, len(serializedID))
	key = append(key, serializedID...)

	kv := e.kvStore.DB()

	var event *journal.Event
	err = kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameEventsByID))
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

func (e *EventsByID) List(ctx context.Context) ([]journal.Event, error) {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return nil, err
	}

	kv := e.kvStore.DB()

	events := []journal.Event{}
	err := kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameEventsByID))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		for _, v := c.First(); v != nil; _, v = c.Next() {
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
