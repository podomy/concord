// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/transport"
)

// bucketBlobCursors is the bbolt bucket holding blob fetch cursors: one
// next chunk index per peer and root digest. Journal cursors live in
// synccursors; blob progress is separate because it resumes byte
// assembly, not event order.
const bucketBlobCursors = "blobcursors"

// blobCursorStore persists per peer blob cursors so a restart or a
// partition resumes mid blob instead of refetching from chunk zero.
// Cursors are monotonic: saves only advance, never retreat, so a stale
// write cannot rewind completed work. A nil KVStore keeps RAM-only
// behavior for tests.
type blobCursorStore struct {
	mu sync.Mutex
	kv *kvstore.KVStore
	// mem mirrors persisted cursors for fast resume and backs
	// RAM-only mode when kv is nil.
	mem map[string]uint64
}

// newBlobCursorStore creates a cursor store. A nil kv keeps cursors in
// memory only; production passes the shared KVStore for persistence.
func newBlobCursorStore(kv *kvstore.KVStore) *blobCursorStore {
	return &blobCursorStore{kv: kv, mem: make(map[string]uint64)}
}

// blobCursorKey encodes the cursor key for peer and root. Roots are
// validated digests before use, so the key cannot carry path or
// separator tricks beyond the single split delimiter.
func blobCursorKey(peer uuid.UUID, root string) string {
	return peer.String() + "|" + root
}

// load returns the next chunk index to fetch from peer for root. A
// missing bucket or entry yields zero, not an error. A corrupt value
// self heals: the key is deleted from the database and memory and
// zero is returned so the next save can proceed. Delete failure
// returns an error so the caller logs and restarts that blob from
// zero. On memory miss the database is consulted, so a stale zero
// never hides a larger persisted cursor.
func (s *blobCursorStore) load(peer uuid.UUID, root string) (uint64, error) {
	key := blobCursorKey(peer, root)
	if cached, ok := s.lookupCached(key); ok {
		return cached, nil
	}
	if s.kv == nil {
		return 0, nil
	}
	stored, err := s.fetchStored(key)
	if err != nil {
		return 0, fmt.Errorf("load blob cursor: %w", err)
	}
	if len(stored) == 0 {
		return 0, nil
	}
	if len(stored) != 8 {
		if cerr := s.clearPersisted(key); cerr != nil {
			return 0, fmt.Errorf("clear corrupt blob cursor: %w", cerr)
		}
		s.dropCached(key)
		return 0, nil
	}
	next := binary.BigEndian.Uint64(stored)
	s.storeCached(key, next)
	return next, nil
}

// lookupCached returns the memory cached cursor for key.
func (s *blobCursorStore) lookupCached(key string) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next, ok := s.mem[key]
	return next, ok
}

// storeCached records next in memory.
func (s *blobCursorStore) storeCached(key string, next uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mem[key] = next
}

// dropCached removes key from memory.
func (s *blobCursorStore) dropCached(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mem, key)
}

// fetchStored reads the persisted cursor bytes for key. A missing
// bucket or entry yields nil, not an error.
func (s *blobCursorStore) fetchStored(key string) ([]byte, error) {
	var stored []byte
	err := s.kv.DB().View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketBlobCursors))
		if b == nil {
			return nil
		}
		v := b.Get([]byte(key))
		if len(v) == 0 {
			return nil
		}
		stored = append([]byte(nil), v...)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("view blob cursor: %w", err)
	}
	return stored, nil
}

// clearPersisted deletes the persisted cursor for key.
func (s *blobCursorStore) clearPersisted(key string) error {
	err := s.kv.DB().Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketBlobCursors))
		if b == nil {
			return nil
		}
		if derr := b.Delete([]byte(key)); derr != nil {
			return fmt.Errorf("kv delete: %w", derr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("clear blob cursor: %w", err)
	}
	return nil
}

// writePersisted stores next as the persisted cursor for key.
func (s *blobCursorStore) writePersisted(key string, next uint64) error {
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], next)
	err := s.kv.DB().Update(func(tx *bolt.Tx) error {
		b, berr := tx.CreateBucketIfNotExists([]byte(bucketBlobCursors))
		if berr != nil {
			return fmt.Errorf("kv bucket creation: %w", berr)
		}
		if perr := b.Put([]byte(key), raw[:]); perr != nil {
			return fmt.Errorf("kv put: %w", perr)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("write blob cursor: %w", err)
	}
	return nil
}

// save persists next as the cursor for peer and root, advancing only.
// It holds mu across the read compare write sequence so concurrent
// saves cannot interleave a stale read between load and update.
// A next at or behind the stored value skips the write, which keeps
// retries idempotent and cursors monotonic across duplicate stores.
func (s *blobCursorStore) save(peer uuid.UUID, root string, next uint64) error {
	if !transport.ValidBlobDigest(root) {
		return fmt.Errorf("invalid root digest %q", root)
	}
	key := blobCursorKey(peer, root)
	s.mu.Lock()
	defer s.mu.Unlock()
	if next <= s.mem[key] {
		return nil
	}
	if s.kv == nil {
		s.mem[key] = next
		return nil
	}
	stored, err := s.fetchStored(key)
	if err != nil {
		return fmt.Errorf("load blob cursor: %w", err)
	}
	if len(stored) == 0 {
		return s.persistLocked(key, next)
	}
	if len(stored) != 8 {
		if cerr := s.clearPersisted(key); cerr != nil {
			return fmt.Errorf("clear corrupt blob cursor: %w", cerr)
		}
		delete(s.mem, key)
		return s.persistLocked(key, next)
	}
	if dbNext := binary.BigEndian.Uint64(stored); dbNext >= next {
		s.mem[key] = dbNext
		return nil
	}
	return s.persistLocked(key, next)
}

// persistLocked writes next to the database and memory. The caller
// holds mu across the read compare write sequence.
func (s *blobCursorStore) persistLocked(key string, next uint64) error {
	if err := s.writePersisted(key, next); err != nil {
		return fmt.Errorf("save blob cursor: %w", err)
	}
	s.mem[key] = next
	return nil
}

// Delete removes the cursor for key from memory and the database.
// A missing key is not an error. Completed roots call this so the
// bucket stays bounded; a stale entry is otherwise harmless because
// HasRaw skips present chunks and the final verify hashes content.
func (s *blobCursorStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.kv != nil {
		err := s.kv.DB().Update(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(bucketBlobCursors))
			if b == nil {
				return nil
			}
			if derr := b.Delete([]byte(key)); derr != nil {
				return fmt.Errorf("kv delete: %w", derr)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("delete blob cursor: %w", err)
		}
	}
	delete(s.mem, key)
	return nil
}
