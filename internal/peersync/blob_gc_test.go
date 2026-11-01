// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/blob"
	"github.com/podomy/concord/internal/transport"
	"github.com/podomy/concord/internal/workload"
)

// blobDigestFor returns the sha256 digest for data.
func blobDigestFor(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// badManifestDigest stores bad JSON under its hash and tags it for root.
func badManifestDigest(t *testing.T, store *blob.Store, root string) string {
	t.Helper()
	ctx := context.Background()
	bad := []byte("{bad json")
	dm := blobDigestFor(bad)
	err := store.PutRaw(ctx, dm, bad)
	if err != nil {
		t.Fatalf("PutRaw bad manifest: %v", err)
	}
	tag := strings.TrimPrefix(root, "sha256:")
	err = store.PutTag(blobSyncRepo, tag, dm)
	if err != nil {
		t.Fatalf("PutTag bad manifest: %v", err)
	}
	return dm
}

// TestCorruptManifestErrors proves a corrupt manifest returns an error
// like oversize, not silent incomplete. Error text changed from quiet
// incomplete to surfaced decode failure.
func TestCorruptManifestErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := testBlobStore(t)
	root := "sha256:" + strings.Repeat("d", 64)
	dm := badManifestDigest(t, store, root)
	syncer := New(store, nil, nil)
	_, _, err := syncer.loadTagManifest(dm)
	if err == nil {
		t.Fatalf("loadTagManifest nil error for corrupt manifest")
	}
	if !strings.Contains(err.Error(), "decode") {
		t.Fatalf("unexpected error %v", err)
	}
	complete, err := syncer.isComplete(ctx, root)
	if err == nil {
		t.Fatalf("isComplete nil error for corrupt manifest")
	}
	if complete {
		t.Fatalf("isComplete true for corrupt manifest")
	}
}

// TestChunkSizePrecheck proves an oversize chunk is stat rejected before
// load. The chunk is PutRaw legal (under 32MB) but over ChunkSize, so
// only the pre-check keeps a malicious 32MB chunk from a full read.
func TestChunkSizePrecheck(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := testBlobStore(t)
	data := testBlobData(blob.ChunkSize + 10)
	digest := blobDigestFor(data)
	err := store.PutRaw(ctx, digest, data)
	if err != nil {
		t.Fatalf("PutRaw oversize chunk: %v", err)
	}
	syncer := New(store, nil, nil)
	err = checkChunkSize(store, digest)
	if err == nil {
		t.Fatalf("checkChunkSize nil error for oversize chunk")
	}
	if !strings.Contains(err.Error(), "oversize chunk") {
		t.Fatalf("unexpected error %v", err)
	}
	err = syncer.verifyTrueChunks(ctx, []string{digest})
	if err == nil {
		t.Fatalf("verifyTrueChunks nil error for oversize chunk")
	}
	if !strings.Contains(err.Error(), "oversize chunk") {
		t.Fatalf("unexpected error %v", err)
	}
	err = verifyBlobContent(ctx, store, []string{digest}, "sha256:"+strings.Repeat("e", 64))
	if err == nil {
		t.Fatalf("verifyBlobContent nil error for oversize chunk")
	}
}

// TestPruneWantedDropsTombstoned proves wanted entries whose spec is
// tombstoned or gone are dropped. Bytes stay on disk; only the memory
// set shrinks. Operator owns byte garbage collection.
func TestPruneWantedDropsTombstoned(t *testing.T) {
	t.Parallel()
	store := testBlobStore(t)
	syncer := New(store, nil, nil)
	live := "sha256:" + strings.Repeat("a", 64)
	gone := "sha256:" + strings.Repeat("b", 64)
	dead := "sha256:" + strings.Repeat("c", 64)
	syncer.rememberWanted([]string{live, gone, dead})
	idLive := uuid.New()
	idDead := uuid.New()
	specs := []workload.Spec{
		{ID: idLive, Files: []workload.FileRef{{Path: "/a", Digest: live}}},
		{ID: idDead, Removed: true, Files: []workload.FileRef{{Path: "/b", Digest: dead}}},
	}
	syncer.PruneWanted(specs)
	got := syncer.snapshotWanted()
	if len(got) != 1 || got[0] != live {
		t.Fatalf("PruneWanted = %v want [%s]", got, live)
	}
}

// TestDepartedCursorGC proves per-peer cursors are deleted after the
// peer misses membership for blobCursorDepartedTicks ticks. Completed
// roots keep prompt deletion; this sweep reclaims departed peers.
func TestDepartedCursorGC(t *testing.T) {
	t.Parallel()
	store := testBlobStore(t)
	syncer := New(store, nil, nil)
	peer := uuid.New()
	root := "sha256:" + strings.Repeat("f", 64)
	err := syncer.cursors.save(peer, root, 3)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	other := blobPeer{id: uuid.New()}
	for range blobCursorDepartedTicks {
		syncer.sweepDepartedCursors([]blobPeer{other})
	}
	next, err := syncer.cursors.load(peer, root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if next != 0 {
		t.Fatalf("cursor = %d want 0 after departed sweep", next)
	}
}

// TestDepartedCursorPresentResets proves a present peer never accrues
// absent ticks and its cursors survive the sweep.
func TestDepartedCursorPresentResets(t *testing.T) {
	t.Parallel()
	store := testBlobStore(t)
	syncer := New(store, nil, nil)
	peer := uuid.New()
	root := "sha256:" + strings.Repeat("f", 64)
	err := syncer.cursors.save(peer, root, 2)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	mine := blobPeer{id: peer}
	for range blobCursorDepartedTicks + 2 {
		syncer.sweepDepartedCursors([]blobPeer{mine})
	}
	next, err := syncer.cursors.load(peer, root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if next != 2 {
		t.Fatalf("cursor = %d want 2 for present peer", next)
	}
}

// TestTagCapTruncates proves per-repo discovery caps excess tags with a
// debug skip instead of growing memory or fetch work without bound.
func TestTagCapTruncates(t *testing.T) {
	t.Parallel()
	syncer := New(testBlobStore(t), nil, nil)
	peer := uuid.New()
	many := make([]string, blobMaxTagsPerRepo+10)
	for i := range many {
		many[i] = strings.Repeat("a", 64)
	}
	kept := syncer.capTagList(many, blobSyncRepo, peer)
	if len(kept) != blobMaxTagsPerRepo {
		t.Fatalf("capTagList = %d want %d", len(kept), blobMaxTagsPerRepo)
	}
	keptRepo := syncer.capRepoTotal(many, blobSyncRepo)
	if len(keptRepo) != blobMaxTagsPerRepo {
		t.Fatalf("capRepoTotal = %d want %d", len(keptRepo), blobMaxTagsPerRepo)
	}
	few := many[:10]
	if got := syncer.capTagList(few, blobSyncRepo, peer); len(got) != 10 {
		t.Fatalf("capTagList truncated under cap")
	}
}

// TestDeletePeerIsolates proves DeletePeer removes only that peer cursors.
func TestDeletePeerIsolates(t *testing.T) {
	t.Parallel()
	store := newBlobCursorStore(nil)
	a := uuid.New()
	b := uuid.New()
	root := "sha256:" + strings.Repeat("a", 64)
	err := store.save(a, root, 1)
	if err != nil {
		t.Fatalf("save a: %v", err)
	}
	err = store.save(b, root, 2)
	if err != nil {
		t.Fatalf("save b: %v", err)
	}
	err = store.DeletePeer(a)
	if err != nil {
		t.Fatalf("DeletePeer: %v", err)
	}
	next, err := store.load(a, root)
	if err != nil {
		t.Fatalf("load a: %v", err)
	}
	if next != 0 {
		t.Fatalf("cursor a = %d want 0", next)
	}
	next, err = store.load(b, root)
	if err != nil {
		t.Fatalf("load b: %v", err)
	}
	if next != 2 {
		t.Fatalf("cursor b = %d want 2", next)
	}
}

// TestLiveViewPrunesEachTick proves tick pruning uses the installed live
// view. No live view means no pruning; a set view drops gone digests.
func TestLiveViewPrunesEachTick(t *testing.T) {
	t.Parallel()
	store := testBlobStore(t)
	syncer := New(store, nil, nil)
	keep := "sha256:" + strings.Repeat("a", 64)
	drop := "sha256:" + strings.Repeat("b", 64)
	syncer.rememberWanted([]string{keep, drop})
	id := uuid.New()
	syncer.SetLiveView(func() []workload.Spec {
		return []workload.Spec{{ID: id, Files: []workload.FileRef{{Path: "/a", Digest: keep}}}}
	})
	syncer.pruneTickWanted()
	got := syncer.snapshotWanted()
	if len(got) != 1 || got[0] != keep {
		t.Fatalf("pruneTickWanted = %v want [%s]", got, keep)
	}
	syncer.SetLiveView(nil)
	syncer.rememberWanted([]string{drop})
	syncer.pruneTickWanted()
	got = syncer.snapshotWanted()
	if len(got) != 2 {
		t.Fatalf("nil live view pruned, got %v", got)
	}
}

// TestValidDigestsFilter proves discovery only keeps well formed digests.
func TestValidDigestsFilter(t *testing.T) {
	t.Parallel()
	tags := []string{"sha256:" + strings.Repeat("a", 64), "bogus", "sha256:xyz"}
	clean := filterValidDigests(tags)
	if len(clean) != 1 {
		t.Fatalf("filterValidDigests = %v", clean)
	}
	if !transport.ValidBlobDigest(clean[0]) {
		t.Fatalf("clean digest invalid %q", clean[0])
	}
}
