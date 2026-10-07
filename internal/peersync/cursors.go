// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"fmt"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/podomy/concord/internal/kvstore"
)

// cursorSet holds one sync cursor per peer: the id of the last journal
// event already pulled from that peer. A missing key means nothing has
// been pulled yet, so the next pull starts from the peer's first event.
//
// The set lives in process memory only. A restart starts from empty
// cursors and re-pulls from event zero; idempotent apply by event id
// discards the overlap. See docs/cursors.md for the full mechanics.
type cursorSet map[uuid.UUID]string

// newCursorSet creates an empty per-peer cursor set.
func newCursorSet() cursorSet {
	return cursorSet{}
}

// lookup returns the stored cursor for peer, or "" when nothing has been
// pulled from them yet (first contact).
func (c cursorSet) lookup(peer uuid.UUID) string {
	return c[peer]
}

// advance moves peer's cursor to next, unless next is empty. An empty
// NextCursor means the peer had nothing new, so the stored cursor
// must be kept, never cleared. It reports whether the cursor moved.
func (c cursorSet) advance(peer uuid.UUID, next string) bool {
	if next == "" {
		return false
	}
	c[peer] = next
	return true
}

// bucketNameCursors is the bbolt bucket holding persisted sync cursors.
const bucketNameCursors = "synccursors"

// cursorStore persists per-peer sync cursors in bbolt so a restart
// resumes mid-history instead of re-pulling every peer journal from
// event zero. The store is local progress only: cursors never enter
// the journal because no other node can use them.
type cursorStore struct {
	kvStore *kvstore.KVStore
}

// NewCursorStore creates a cursor store backed by the given KVStore.
func NewCursorStore(kv *kvstore.KVStore) *cursorStore {
	return &cursorStore{kvStore: kv}
}

// load reads all persisted cursors. A missing bucket means nothing was
// stored yet and yields an empty set, not an error.
func (s *cursorStore) load() (cursorSet, error) {
	cursors := newCursorSet()
	err := s.kvStore.DB().View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameCursors))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			id, err := uuid.FromBytes(k)
			if err != nil {
				return fmt.Errorf("parse peer id: %w", err)
			}
			cursors[id] = string(v)
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("load cursors: %w", err)
	}
	return cursors, nil
}

// save persists peer's cursor. Callers save only after the in-memory
// cursor advanced on successful apply, so the persisted cursor never
// runs ahead of applied state. A failed save costs only a future
// re-pull; idempotent apply absorbs the overlap.
func (s *cursorStore) save(peer uuid.UUID, cursor string) error {
	key, err := peer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}
	err = s.kvStore.DB().Update(func(tx *bolt.Tx) error {
		b, berr := tx.CreateBucketIfNotExists([]byte(bucketNameCursors))
		if berr != nil {
			return fmt.Errorf("kv bucket creation: %w", berr)
		}
		perr := b.Put(key, []byte(cursor))
		if perr != nil {
			return fmt.Errorf("kv put: %w", perr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	return nil
}

// remove drops peer's persisted cursor, e.g. after explicit departure.
// A missing entry is not an error.
func (s *cursorStore) remove(peer uuid.UUID) error {
	key, err := peer.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}
	err = s.kvStore.DB().Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameCursors))
		if b == nil {
			return nil
		}
		derr := b.Delete(key)
		if derr != nil {
			return fmt.Errorf("kv delete: %w", derr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("remove cursor: %w", err)
	}
	return nil
}
