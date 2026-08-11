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
	"github.com/podomy/concord/internal/workload"
)

// Invariants.
// 1. Only workload.spec events affect this view.
// 2. There is one stored spec per workload.Spec.ID.
// 3. Applying a newer spec replaces the previous spec.
// 4. Removed=true remains stored as a tombstone.
// 5. Rebuild replays the journal in order and produces the same final state.
// 6. Malformed workload.spec payloads return an error.

const bucketNameWorkloads = "workloads"

// Workloads maintains a view of workload specs derived from workload.spec journal events.
type Workloads struct {
	kvStore *kvstore.KVStore
}

// NewWorkloads creates a new Workloads view backed by the given KVStore.
func NewWorkloads(kv *kvstore.KVStore) *Workloads {
	return &Workloads{
		kvStore: kv,
	}
}

// putEvent unmarshals a workload spec event payload and stores the spec in the bucket keyed by spec ID.
func (e *Workloads) putEvent(b *bolt.Bucket, event journal.Event) error {
	var spec workload.Spec
	err := json.Unmarshal(event.Payload, &spec)
	if err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}

	serializedSpec, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	serializedSpecID, err := spec.ID.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}

	key := make([]byte, 0, len(serializedSpecID))
	key = append(key, serializedSpecID...)

	err = b.Put(key, serializedSpec)
	if err != nil {
		return fmt.Errorf("bucket put kv: %w", err)
	}

	return nil
}

// Apply updates the view if the event is a workload.spec event, storing or updating the workload spec.
func (e *Workloads) Apply(ctx context.Context, event journal.Event) error {
	if err := checkContext(ctx, "context cancelation"); err != nil {
		return err
	}

	if event.Type != "workload.spec" {
		// We simply ignore anything that has incorrect type.
		// Apply is always used for every single element in a journal.
		return nil
	}

	kv := e.kvStore.DB()

	err := kv.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketNameWorkloads))
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

// resetBucket deletes the existing workloads bucket if present and creates a fresh one.
func (e *Workloads) resetBucket(tx *bolt.Tx) (*bolt.Bucket, error) {
	if err := tx.DeleteBucket([]byte(bucketNameWorkloads)); err != nil && !errors.Is(err, berrors.ErrBucketNotFound) {
		return nil, fmt.Errorf("kv bucket deletion: %w", err)
	}

	b, err := tx.CreateBucket([]byte(bucketNameWorkloads))
	if err != nil {
		return nil, fmt.Errorf("kv bucket creation: %w", err)
	}

	return b, nil
}

// replayEvents reads events sequentially from the journal reader and puts workload.spec events into the bucket.
func (e *Workloads) replayEvents(ctx context.Context, jr journalreader.Reader, b *bolt.Bucket) error {
	for {
		event, err := readEvent(ctx, jr)
		if err != nil {
			return err
		}
		if event == nil {
			return nil
		}
		if event.Type != "workload.spec" {
			// We simply ignore any event that has an incorrect type.
			continue
		}

		if err = e.putEvent(b, *event); err != nil {
			return fmt.Errorf("put event: %w", err)
		}
	}
}

// Rebuild reconstructs the Workloads view by resetting the storage bucket and replaying all workload.spec events.
//
//nolint:dupl // Projection methods intentionally keep rebuild flow local to each view.
func (e *Workloads) Rebuild(ctx context.Context, jr journalreader.Reader) error {
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

// Get retrieves a workload spec by its unique ID.
func (e *Workloads) Get(ctx context.Context, id uuid.UUID) (*workload.Spec, error) {
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

	var spec *workload.Spec
	err = kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameWorkloads))
		if b == nil {
			return nil
		}

		serializedSpec := b.Get(key)
		if serializedSpec == nil {
			return nil
		}

		var decoded workload.Spec
		err = json.Unmarshal(serializedSpec, &decoded)
		if err != nil {
			return fmt.Errorf("deserialization: %w", err)
		}

		spec = &decoded

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kv view: %w", err)
	}

	return spec, nil
}

// List retrieves all stored workload specs.
func (e *Workloads) List(ctx context.Context) ([]workload.Spec, error) {
	if err := checkContext(ctx, "context cancellation"); err != nil {
		return nil, err
	}

	kv := e.kvStore.DB()

	specs := []workload.Spec{}
	err := kv.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameWorkloads))
		if b == nil {
			return nil
		}

		c := b.Cursor()

		for _, v := c.First(); v != nil; _, v = c.Next() {
			var spec workload.Spec
			err := json.Unmarshal(v, &spec)
			if err != nil {
				return fmt.Errorf("deserialization: %w", err)
			}

			specs = append(specs, spec)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("kv view: %w", err)
	}

	return specs, nil
}
