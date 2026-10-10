// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"errors"
	"maps"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/transport"
)

// testCursorStore opens an isolated bbolt cursor store for store unit tests.
func testCursorStore(t *testing.T) CursorStore {
	t.Helper()
	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "bbolt.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("close kv: %v", err)
		}
	})
	return NewCursorStore(kv)
}

// Loading from a fresh database yields an empty set, not an error.
func TestCursorStoreLoadEmpty(t *testing.T) {
	t.Parallel()

	cursorStore := testCursorStore(t)
	got, err := cursorStore.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("len = %d, want 0", len(got))
	}
}

// Saved cursors survive through a new store instance on the same database:
// the restart path.
func TestCursorStoreSaveLoadRoundtrip(t *testing.T) {
	t.Parallel()

	cursorStore := testCursorStore(t)
	a, b := uuid.New(), uuid.New()
	if err := cursorStore.save(a, "mark-a"); err != nil {
		t.Fatal(err)
	}
	if err := cursorStore.save(b, "mark-b"); err != nil {
		t.Fatal(err)
	}

	got, err := cursorStore.load()
	if err != nil {
		t.Fatal(err)
	}
	if got.lookup(a) != "mark-a" || got.lookup(b) != "mark-b" {
		t.Fatalf("loaded = %v", got)
	}
}

// Overwrite replaces, remove drops; removing a missing peer is not an error.
func TestCursorStoreOverwriteAndRemove(t *testing.T) {
	t.Parallel()

	cursorStore := testCursorStore(t)
	peer := uuid.New()
	if err := cursorStore.save(peer, "old"); err != nil {
		t.Fatal(err)
	}
	if err := cursorStore.save(peer, "new"); err != nil {
		t.Fatal(err)
	}
	got, err := cursorStore.load()
	if err != nil {
		t.Fatal(err)
	}
	if got.lookup(peer) != "new" {
		t.Fatalf("cursor = %q, want new", got.lookup(peer))
	}
	if err := cursorStore.remove(peer); err != nil {
		t.Fatal(err)
	}
	if err := cursorStore.remove(uuid.New()); err != nil {
		t.Fatal(err)
	}
	got, err = cursorStore.load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got[peer]; ok {
		t.Fatal("removed peer still present")
	}
}

// memoryCursorStore is an in-memory CursorStore for pull-loop tests: no
// database, no disk, hermetic by construction.
type memoryCursorStore struct {
	mu      sync.Mutex
	cursors cursorSet
}

// newMemoryCursorStore creates an empty in-memory cursor store.
func newMemoryCursorStore() *memoryCursorStore {
	return &memoryCursorStore{cursors: newCursorSet()}
}

// load returns a copy of the stored cursors.
func (m *memoryCursorStore) load() (cursorSet, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := newCursorSet()
	maps.Copy(out, m.cursors)
	return out, nil
}

// save stores peer's cursor.
func (m *memoryCursorStore) save(peer uuid.UUID, cursor string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cursors[peer] = cursor
	return nil
}

// remove drops peer's cursor. A missing entry is not an error.
func (m *memoryCursorStore) remove(peer uuid.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.cursors, peer)
	return nil
}

// A successful syncOne persists the advanced cursor through the store.
func TestSyncOnePersistsCursor(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := keyedNode(peerID, "192.0.2.10:7946")
	ev := mustEvent()
	fake := &fakeSyncer{
		resp: transport.SyncResponse{
			NextCursor: ev.ID.String(),
			Events:     []journal.Event{ev},
		},
	}
	cursorStore := newMemoryCursorStore()
	j := &memJournal{}
	state := pullState{
		syncer:      fake,
		journal:     j,
		byID:        &journalIndex{j: j},
		port:        8443,
		cursors:     newCursorSet(),
		cursorStore: cursorStore,
	}

	if !syncOne(context.Background(), zap.NewNop(), state, member) {
		t.Fatal("expected success")
	}
	got, err := cursorStore.load()
	if err != nil {
		t.Fatal(err)
	}
	if got.lookup(peerID) != ev.ID.String() {
		t.Fatalf("persisted = %q, want %q", got.lookup(peerID), ev.ID.String())
	}
}

// A failed apply advances neither the memory cursor nor the store.
func TestSyncOneFailedApplyPersistsNothing(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := keyedNode(peerID, "192.0.2.10:7946")
	cursorStore := newMemoryCursorStore()
	j := &memJournal{}
	state := pullState{
		syncer:      &fakeSyncer{resp: transport.SyncResponse{NextCursor: "w", Events: []journal.Event{mustEvent()}}},
		journal:     j,
		byID:        &brokenIndex{},
		port:        8443,
		cursors:     newCursorSet(),
		cursorStore: cursorStore,
	}

	if syncOne(context.Background(), zap.NewNop(), state, member) {
		t.Fatal("expected failure")
	}
	got, err := cursorStore.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("store = %v, want empty", got)
	}
}

// brokenIndex fails every lookup, forcing ApplyEvents to fail.
type brokenIndex struct{}

// Get always returns an error.
func (brokenIndex) Get(_ context.Context, _ uuid.UUID) (*journal.Event, error) {
	return nil, errors.New("broken index") //nolint:wrapcheck // plain error, no wrap target
}
