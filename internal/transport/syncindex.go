// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalreader"
	"github.com/podomy/concord/internal/kvstore"
)

// bucketNameSyncIndex is the bbolt bucket mapping journal event ids to
// byte offsets in journal.jsonl.
const bucketNameSyncIndex = "syncindex"

// indexBatchSize caps offsets per write transaction so a large backfill
// never holds one giant transaction.
const indexBatchSize = 500

// offsetSize is the encoded size of one file offset.
const offsetSize = 8

// offsetIndex maps journal event ids to byte offsets in the local journal
// file, so serving a page seeks straight to the cursor instead of scanning
// from the file start. The index is advisory only: every offset is verified
// against file content before use, and any miss or mismatch falls back to
// a full scan. Stale entries after a journal wipe heal by overwrite on
// first use.
type offsetIndex struct {
	kvStore *kvstore.KVStore
}

// NewOffsetIndex creates an offset index backed by the given KVStore.
func NewOffsetIndex(kv *kvstore.KVStore) *offsetIndex {
	return &offsetIndex{kvStore: kv}
}

// lookup returns the stored offset for cursor. It reports false when the
// cursor is not an event id, has no entry, fails to load, or holds a
// corrupt value. All four mean the same thing: serve from a full scan.
func (s *offsetIndex) lookup(cursor string) (int64, bool) {
	// The cursor must name an event; anything else can never have an entry.
	id, err := uuid.Parse(cursor)
	if err != nil {
		return 0, false
	}
	key, err := id.MarshalBinary()
	if err != nil {
		return 0, false
	}

	// Copy the value out: bbolt memory belongs to the transaction.
	var stored []byte
	verr := s.kvStore.DB().View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketNameSyncIndex))
		if b == nil {
			return nil
		}
		v := b.Get(key)
		if len(v) != offsetSize {
			return nil
		}
		stored = append([]byte(nil), v...)
		return nil
	})
	if verr != nil {
		return 0, false
	}
	return decodeOffset(stored)
}

// decodeOffset decodes stored bytes into a file offset. It reports false
// on any other length or on values outside int64 range: both mean a corrupt
// entry, which the caller treats as a miss.
func decodeOffset(stored []byte) (int64, bool) {
	if len(stored) != offsetSize {
		return 0, false
	}
	u := binary.BigEndian.Uint64(stored)
	if u > math.MaxInt64 {
		return 0, false
	}
	return int64(u), true
}

// record stores offsets in one write transaction. An empty set skips the
// transaction entirely.
func (s *offsetIndex) record(offsets map[uuid.UUID]int64) error {
	if len(offsets) == 0 {
		return nil
	}
	err := s.kvStore.DB().Update(func(tx *bolt.Tx) error {
		b, berr := tx.CreateBucketIfNotExists([]byte(bucketNameSyncIndex))
		if berr != nil {
			return fmt.Errorf("kv bucket creation: %w", berr)
		}
		for id, offset := range offsets {
			perr := putOffset(b, id, offset)
			if perr != nil {
				return perr
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("record offsets: %w", err)
	}
	return nil
}

// putOffset encodes one offset and stores it in the bucket. A negative
// offset never comes from the reader, so it fails loudly instead of
// storing garbage that would seek backwards.
func putOffset(b *bolt.Bucket, id uuid.UUID, offset int64) error {
	key, err := id.MarshalBinary()
	if err != nil {
		return fmt.Errorf("serialization: %w", err)
	}
	if offset < 0 {
		return fmt.Errorf("negative offset for %s: %d", id, offset)
	}
	var raw [offsetSize]byte
	binary.BigEndian.PutUint64(raw[:], uint64(offset))
	err = b.Put(key, raw[:])
	if err != nil {
		return fmt.Errorf("kv put: %w", err)
	}
	return nil
}

// build scans the whole journal once and records every offset, in batches.
// It runs in the background at startup; requests serve through the full-scan
// fallback until it catches up.
func (s *offsetIndex) build(ctx context.Context) error {
	reader, err := journalreader.OpenJSONLReader()
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer func() { _ = reader.Close() }() //nolint:errcheck // best-effort

	// Walk the whole file once, flushing in batches so no single
	// transaction grows with the journal.
	batch := make(map[uuid.UUID]int64, indexBatchSize)
	for {
		var event *journal.Event
		var offset int64
		event, offset, err = reader.ReadWithOffset(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("read journal: %w", err)
		}
		batch[event.ID] = offset
		if len(batch) >= indexBatchSize {
			err = s.record(batch)
			if err != nil {
				return err
			}
			batch = make(map[uuid.UUID]int64, indexBatchSize)
		}
	}

	// Flush whatever the last partial batch holds.
	err = s.record(batch)
	if err != nil {
		return err
	}
	return nil
}
