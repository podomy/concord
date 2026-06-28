// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	bolt "go.etcd.io/bbolt"
	berrors "go.etcd.io/bbolt/errors"

	"github.com/podomy/hive/src/journal"
	"github.com/podomy/hive/src/journalreader"
	"github.com/podomy/hive/src/kvstore"
)

const bucketNameEventsByID = "eventsbyid"

type EventsByID struct {
	kvStore kvstore.KVStore
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
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelation: %w", ctx.Err())
	default:
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
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelation during rebuild: %w", err)
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
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelation: %w", ctx.Err())
	default:
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
