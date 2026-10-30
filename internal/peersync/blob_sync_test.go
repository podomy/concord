// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/blob"
	"github.com/podomy/concord/internal/transport"
	"github.com/podomy/concord/internal/workload"
)

// testBlobData returns deterministic bytes of size n.
func testBlobData(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i*31 + 7) % 251)
	}
	return out
}

// testBlobStore opens an isolated blob store for sync tests.
func testBlobStore(t *testing.T) *blob.Store {
	t.Helper()
	store, err := blob.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// blobTestRegistry serves a blob.Store over the sync repo HTTP shape:
// blobs by digest, manifests by tag, and the tag list. It mirrors the
// registry contract the syncer relies on.
type blobTestRegistry struct {
	store *blob.Store
}

// testBlobRest validates the method and strips the sync repo prefix.
// It writes 404 on mismatch so callers return directly.
func testBlobRest(w http.ResponseWriter, r *http.Request) (string, bool) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusNotFound)
		return "", false
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/v2/"+transport.BlobRepo+"/")
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return "", false
	}
	return rest, true
}

// dispatchTestServe routes a stripped test path to blob, manifest, or
// tags handlers. Shared by the sync and flaky registries so routing
// stays identical without duplicating branches.
func dispatchTestServe(w http.ResponseWriter, rest string, serveBlob, serveManifest func(string), serveTags func()) {
	if digest, ok := strings.CutPrefix(rest, "blobs/"); ok {
		serveBlob(digest)
		return
	}
	if tag, ok := strings.CutPrefix(rest, "manifests/"); ok {
		serveManifest(tag)
		return
	}
	if rest == "tags/list" {
		serveTags()
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// ServeHTTP routes one registry request to its handler.
func (s *blobTestRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := testBlobRest(w, r)
	if !ok {
		return
	}
	dispatchTestServe(w, rest,
		func(digest string) { s.serveTestBlob(w, digest) },
		func(tag string) { s.serveTestManifest(w, tag) },
		func() { s.serveTestTags(w) },
	)
}

// serveTestBlob answers one blob GET by digest.
func (s *blobTestRegistry) serveTestBlob(w http.ResponseWriter, digest string) {
	if strings.Contains(digest, "/") || !transport.ValidBlobDigest(digest) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	data, err := s.store.GetRaw(digest)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data) //nolint:errcheck,gosec // best-effort test body, no XSS sink
}

// serveTestManifest answers one manifest GET by resolving the tag.
func (s *blobTestRegistry) serveTestManifest(w http.ResponseWriter, tag string) {
	dm, ok := s.store.ResolveTag(transport.BlobRepo, tag)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	raw, err := s.store.GetRaw(dm)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw) //nolint:errcheck,gosec // best-effort test body, no XSS sink
}

// serveTestTags answers the tag listing for the sync repo.
func (s *blobTestRegistry) serveTestTags(w http.ResponseWriter) {
	body, err := json.Marshal(map[string]any{"name": transport.BlobRepo, "tags": s.store.ListTags(transport.BlobRepo)})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body) //nolint:errcheck // best-effort test body
}

// publishChunked splits data and publishes the manifest under the root
// hex tag, returning the root digest.
func publishChunked(t *testing.T, store *blob.Store, data []byte) string {
	t.Helper()
	ctx := context.Background()
	chunks, m := blob.Split(data)
	for i, chunk := range chunks {
		if err := store.PutRaw(ctx, m.Chunks[i], chunk); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := json.Marshal(m) //nolint:errchkjson // struct encoding never fails, fallible path covered elsewhere
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	dm := "sha256:" + hex.EncodeToString(sum[:])
	if err := store.PutRaw(ctx, dm, raw); err != nil {
		t.Fatal(err)
	}
	if err := store.PutTag(transport.BlobRepo, strings.TrimPrefix(m.Root, "sha256:"), dm); err != nil {
		t.Fatal(err)
	}
	return m.Root
}

// testPeer wraps a registry server as a sync peer.
func testPeer(t *testing.T, store *blob.Store) blobPeer {
	t.Helper()
	srv := httptest.NewServer(&blobTestRegistry{store: store})
	t.Cleanup(srv.Close)
	return blobPeer{id: uuid.New(), base: srv.URL + "/v2"}
}

// A published chunked root replicates manifest plus chunks, clears the
// cursor on completion, and refetches idempotently.
func TestBlobSyncChunkedRoot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storeA := testBlobStore(t)
	storeB := testBlobStore(t)
	data := testBlobData(600*1024 + 123)
	root := publishChunked(t, storeA, data)
	peer := testPeer(t, storeA)

	syncer := New(storeB, nil, nil)
	if !syncer.fetchOneRoot(ctx, root, []blobPeer{peer}) {
		t.Fatalf("fetchOneRoot %s failed", root)
	}
	assertBlobComplete(t, syncer, ctx, root)
	assertCursorCleared(t, syncer, peer, root)
	m := assertStoredManifest(t, storeB, root)
	assertReassembled(t, storeB, m, data)
	if !syncer.fetchOneRoot(ctx, root, []blobPeer{peer}) {
		t.Fatalf("refetch %s failed", root)
	}
}

// assertBlobComplete fails when root is not complete.
func assertBlobComplete(t *testing.T, syncer *BlobSyncer, ctx context.Context, root string) {
	t.Helper()
	complete, err := syncer.isComplete(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatalf("isComplete %s false after fetch", root)
	}
}

// assertCursorCleared fails when the cursor for root is not deleted.
func assertCursorCleared(t *testing.T, syncer *BlobSyncer, peer blobPeer, root string) {
	t.Helper()
	next, err := syncer.cursors.load(peer.id, root)
	if err != nil {
		t.Fatal(err)
	}
	if next != 0 {
		t.Fatalf("cursor = %d, want 0 (deleted on complete)", next)
	}
}

// assertStoredManifest returns the stored manifest for root.
func assertStoredManifest(t *testing.T, store *blob.Store, root string) blob.Manifest {
	t.Helper()
	dm, ok := store.ResolveTag(transport.BlobRepo, strings.TrimPrefix(root, "sha256:"))
	if !ok {
		t.Fatalf("no manifest tag for %s", root)
	}
	raw, err := store.GetRaw(dm)
	if err != nil {
		t.Fatal(err)
	}
	var m blob.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.Root != root || len(m.Chunks) != 3 {
		t.Fatalf("manifest = %+v", m)
	}
	return m
}

// assertReassembled fails when stored chunks do not join to data.
func assertReassembled(t *testing.T, store *blob.Store, m blob.Manifest, data []byte) {
	t.Helper()
	var assembled []byte
	for _, chunk := range m.Chunks {
		buf, err := store.GetRaw(chunk)
		if err != nil {
			t.Fatal(err)
		}
		assembled = append(assembled, buf...)
	}
	if string(assembled) != string(data) {
		t.Fatalf("reassembled %d bytes, want %d", len(assembled), len(data))
	}
}

// A corrupt cursor restarts the blob instead of wedging: it clamps to
// zero and HasRaw skips the chunks already stored.
func TestBlobSyncCursorClamp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storeA := testBlobStore(t)
	storeB := testBlobStore(t)
	root := publishChunked(t, storeA, testBlobData(600*1024+123))
	peer := testPeer(t, storeA)

	syncer := New(storeB, nil, nil)
	if err := syncer.cursors.save(peer.id, root, 999); err != nil {
		t.Fatal(err)
	}
	if !syncer.fetchOneRoot(ctx, root, []blobPeer{peer}) {
		t.Fatalf("fetchOneRoot %s failed with clamped cursor", root)
	}
	complete, err := syncer.isComplete(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatalf("isComplete %s false after clamp", root)
	}
}

// Untagged raw content converts to the chunked shape on fetch and
// forwards through the alias tag: a third node syncs from the second
// without ever seeing the monolith.
func TestBlobSyncRawConvertAndForward(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storeA := testBlobStore(t)
	storeB := testBlobStore(t)
	storeC := testBlobStore(t)
	data := testBlobData(600 * 1024)
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if err := storeA.PutRaw(ctx, digest, data); err != nil {
		t.Fatal(err)
	}

	peerA := testPeer(t, storeA)
	syncerB := New(storeB, nil, nil)
	if !syncerB.fetchOneRoot(ctx, digest, []blobPeer{peerA}) {
		t.Fatalf("fetchOneRoot %s failed", digest)
	}
	complete, err := syncerB.isComplete(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatalf("isComplete %s false after convert", digest)
	}
	if storeB.HasRaw(digest) {
		t.Fatalf("converted node keeps monolith %s", digest)
	}

	peerB := testPeer(t, storeB)
	syncerC := New(storeC, nil, nil)
	if !syncerC.fetchOneRoot(ctx, digest, []blobPeer{peerB}) {
		t.Fatalf("forward fetch %s failed", digest)
	}
	complete, err = syncerC.isComplete(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatalf("isComplete %s false after forward", digest)
	}
}

// The empty blob syncs as a chunkless manifest with no chunk fetches.
func TestBlobSyncEmptyBlob(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	storeA := testBlobStore(t)
	storeB := testBlobStore(t)
	root := publishChunked(t, storeA, nil)
	peer := testPeer(t, storeA)

	syncer := New(storeB, nil, nil)
	if !syncer.fetchOneRoot(ctx, root, []blobPeer{peer}) {
		t.Fatalf("fetchOneRoot %s failed", root)
	}
	complete, err := syncer.isComplete(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	if !complete {
		t.Fatalf("isComplete %s false", root)
	}
}

// WantedFromSpecs collects file digests in order, dedups, and skips
// malformed entries.
func TestWantedFromSpecs(t *testing.T) {
	t.Parallel()

	good := "sha256:" + strings.Repeat("a", 64)
	other := "sha256:" + strings.Repeat("b", 64)
	specs := []workload.Spec{
		{Files: []workload.FileRef{{Path: "/a", Digest: good}, {Path: "/b", Digest: "bogus"}}},
		{Files: []workload.FileRef{{Path: "/c", Digest: good}, {Path: "/d", Digest: other}}},
	}
	got := WantedFromSpecs(specs)
	if len(got) != 2 || got[0] != good || got[1] != other {
		t.Fatalf("WantedFromSpecs = %v", got)
	}
}

// Cursor saves only advance: a stale write never rewinds progress, and
// malformed roots fail loudly.
func TestBlobCursorMonotonic(t *testing.T) {
	t.Parallel()

	store := newBlobCursorStore(nil)
	peer := uuid.New()
	if err := store.save(peer, "sha256:"+strings.Repeat("c", 64), 3); err != nil {
		t.Fatal(err)
	}
	if err := store.save(peer, "sha256:"+strings.Repeat("c", 64), 2); err != nil {
		t.Fatal(err)
	}
	next, err := store.load(peer, "sha256:"+strings.Repeat("c", 64))
	if err != nil {
		t.Fatal(err)
	}
	if next != 3 {
		t.Fatalf("cursor = %d, want 3", next)
	}
	if err := store.save(peer, "bogus", 1); err == nil {
		t.Fatalf("save bogus digest succeeded")
	}
}
