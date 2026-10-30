// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"

	"github.com/podomy/concord/internal/blob"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/transport"
)

// resumeFlakyRegistry serves a manifest plus chunks but fails every
// chunk at or after failFrom with 500.
type resumeFlakyRegistry struct {
	store    *blob.Store
	chunks   []string
	failFrom int
	calls    *int
}

// ServeHTTP routes one flaky registry request.
func (s *resumeFlakyRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := testBlobRest(w, r)
	if !ok {
		return
	}
	dispatchTestServe(w, rest,
		func(digest string) { s.serveResumeBlob(w, digest) },
		func(tag string) { s.serveResumeManifest(w, tag) },
		func() { s.serveResumeTags(w) },
	)
}

// shouldFailChunk reports whether chunk digest must fail with 500.
func (s *resumeFlakyRegistry) shouldFailChunk(digest string) bool {
	for i, c := range s.chunks {
		if c == digest && i >= s.failFrom {
			if s.calls != nil {
				*s.calls++
			}
			return true
		}
	}
	return false
}

// serveResumeBlob answers one flaky blob GET.
func (s *resumeFlakyRegistry) serveResumeBlob(w http.ResponseWriter, digest string) {
	if s.shouldFailChunk(digest) {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	data, err := s.store.GetRaw(digest)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data) //nolint:errcheck,gosec // test writes store bytes, no XSS sink
}

// serveResumeManifest answers one flaky manifest GET.
func (s *resumeFlakyRegistry) serveResumeManifest(w http.ResponseWriter, tag string) {
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
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw) //nolint:errcheck,gosec // test writes store bytes, no XSS sink
}

// serveResumeTags answers the flaky tag listing.
func (s *resumeFlakyRegistry) serveResumeTags(w http.ResponseWriter) {
	body, err := json.Marshal(map[string]any{"name": transport.BlobRepo, "tags": s.store.ListTags(transport.BlobRepo)})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body) //nolint:errcheck // test stub
}

// resumePeer serves store over HTTP with flaky chunk failures.
func resumePeer(t *testing.T, store *blob.Store, chunks []string, failFrom int, calls *int) blobPeer {
	t.Helper()
	srv := httptest.NewServer(&resumeFlakyRegistry{store: store, chunks: chunks, failFrom: failFrom, calls: calls})
	t.Cleanup(srv.Close)
	return blobPeer{id: uuid.New(), base: srv.URL + "/v2"}
}

// Partition resume: bad peer stores one chunk then 500s, good peer
// finishes. HasRaw skips the prefix so the retry converges.
func TestBlobResumePartitionFailover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storeA := testBlobStore(t)
	storeB := testBlobStore(t)
	data := testBlobData(600*1024 + 123)
	root := publishChunked(t, storeA, data)
	_, m := blob.Split(data)
	var badCalls int
	bad := resumePeer(t, storeA, m.Chunks, 1, &badCalls)
	good := testPeer(t, storeA)
	syncer := New(storeB, nil, nil)
	ok := syncer.fetchOneRoot(ctx, root, []blobPeer{bad, good})
	if !ok {
		t.Fatalf("fetchOneRoot failed with failover peers")
	}
	if badCalls == 0 {
		t.Fatalf("bad peer never hit, failover untested")
	}
	complete, err := syncer.isComplete(ctx, root)
	if err != nil {
		t.Fatalf("isComplete: %v", err)
	}
	if !complete {
		t.Fatalf("incomplete after failover")
	}
	next, err := syncer.cursors.load(bad.id, root)
	if err != nil {
		t.Fatalf("cursor load: %v", err)
	}
	if next != 0 {
		t.Fatalf("cursor = %d want 0 after clear", next)
	}
	var assembled []byte
	for _, c := range m.Chunks {
		buf, err := storeB.GetRaw(c)
		if err != nil {
			t.Fatalf("GetRaw: %v", err)
		}
		assembled = append(assembled, buf...)
	}
	if string(assembled) != string(data) {
		t.Fatalf("bytes differ after resume")
	}
}

// Crash between PutRaw and cursor save: chunk stored, cursor still
// zero, reopen keeps bytes, fetch skips the stored prefix.
func TestBlobCrashBetweenPutRawAndCursorSave(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storeA := testBlobStore(t)
	dir := t.TempDir()
	storeB1, err := blob.Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	data := testBlobData(600*1024 + 123)
	root := publishChunked(t, storeA, data)
	_, m := blob.Split(data)
	err = storeB1.PutRaw(ctx, m.Chunks[0], data[:blob.ChunkSize])
	if err != nil {
		t.Fatalf("PutRaw first chunk: %v", err)
	}
	if !storeB1.HasRaw(m.Chunks[0]) {
		t.Fatalf("HasRaw false after crash store")
	}
	storeB2, err := blob.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if !storeB2.HasRaw(m.Chunks[0]) {
		t.Fatalf("HasRaw false after reopen")
	}
	good := testPeer(t, storeA)
	syncer := New(storeB2, nil, nil)
	next, err := syncer.cursors.load(good.id, root)
	if err != nil {
		t.Fatalf("cursor load: %v", err)
	}
	if next != 0 {
		t.Fatalf("cursor = %d want 0 unsaved", next)
	}
	ok := syncer.fetchOneRoot(ctx, root, []blobPeer{good})
	if !ok {
		t.Fatalf("fetchOneRoot failed after crash resume")
	}
	complete, err := syncer.isComplete(ctx, root)
	if err != nil {
		t.Fatalf("isComplete: %v", err)
	}
	if !complete {
		t.Fatalf("incomplete after crash resume")
	}
}

// Corrupt 3 byte cursor value self heals to zero and save proceeds.
func TestBlobCorruptCursorSelfHeals(t *testing.T) {
	t.Parallel()
	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "bbolt.db"))
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() {
		_ = kv.Close() //nolint:errcheck // test cleanup
	})
	store := newBlobCursorStore(kv)
	peer := uuid.New()
	root := "sha256:" + strings.Repeat("a", 64)
	key := blobCursorKey(peer, root)
	writeCorruptCursor(t, kv, key)
	assertCursorValue(t, store, peer, root, 0)
	assertCorruptDeleted(t, kv, key)
	if err := store.save(peer, root, 2); err != nil {
		t.Fatalf("save after heal: %v", err)
	}
	assertCursorValue(t, store, peer, root, 2)
}

// writeCorruptCursor stores 3 garbage bytes under key.
func writeCorruptCursor(t *testing.T, kv *kvstore.KVStore, key string) {
	t.Helper()
	err := kv.DB().Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(bucketBlobCursors))
		if err != nil {
			return fmt.Errorf("create cursor bucket: %w", err)
		}
		if err := b.Put([]byte(key), []byte{1, 2, 3}); err != nil {
			return fmt.Errorf("put corrupt cursor: %w", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("write corrupt cursor: %v", err)
	}
}

// assertCursorValue fails when load does not return want.
func assertCursorValue(t *testing.T, store *blobCursorStore, peer uuid.UUID, root string, want uint64) {
	t.Helper()
	next, err := store.load(peer, root)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if next != want {
		t.Fatalf("next = %d want %d", next, want)
	}
}

// assertCorruptDeleted fails when key still exists in the bucket.
func assertCorruptDeleted(t *testing.T, kv *kvstore.KVStore, key string) {
	t.Helper()
	err := kv.DB().View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketBlobCursors))
		if b == nil {
			return nil
		}
		if v := b.Get([]byte(key)); v != nil {
			t.Errorf("corrupt value not deleted")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("view: %v", err)
	}
}

// 404 failover: empty peer 404s, full peer serves the root.
func TestBlobFailover404(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storeA := testBlobStore(t)
	storeB := testBlobStore(t)
	storeEmpty := testBlobStore(t)
	data := testBlobData(300*1024 + 7)
	root := publishChunked(t, storeA, data)
	missing := testPeer(t, storeEmpty)
	good := testPeer(t, storeA)
	syncer := New(storeB, nil, nil)
	ok := syncer.fetchOneRoot(ctx, root, []blobPeer{missing, good})
	if !ok {
		t.Fatalf("fetchOneRoot failed on 404 failover")
	}
	complete, err := syncer.isComplete(ctx, root)
	if err != nil {
		t.Fatalf("isComplete: %v", err)
	}
	if !complete {
		t.Fatalf("incomplete after 404 failover")
	}
}

// forgedRegistry serves the first chunk with forged bytes.
type forgedRegistry struct {
	store   *blob.Store
	forged  string
	payload []byte
}

// ServeHTTP routes one forged registry request.
func (s *forgedRegistry) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest, ok := testBlobRest(w, r)
	if !ok {
		return
	}
	if digest, ok := strings.CutPrefix(rest, "blobs/"); ok {
		s.serveForgedBlob(w, digest)
		return
	}
	if tag, ok := strings.CutPrefix(rest, "manifests/"); ok {
		serveRegistryManifest(w, s.store, tag)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// serveForgedBlob answers one blob GET, forging the first chunk.
func (s *forgedRegistry) serveForgedBlob(w http.ResponseWriter, digest string) {
	if digest == s.forged {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.payload) //nolint:errcheck // fixed test payload
		return
	}
	raw, err := s.store.GetRaw(digest)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw) //nolint:errcheck,gosec // test writes store bytes, no XSS sink
}

// serveRegistryManifest answers one manifest GET by resolving the tag.
func serveRegistryManifest(w http.ResponseWriter, store *blob.Store, tag string) {
	dm, ok := store.ResolveTag(transport.BlobRepo, tag)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	raw, err := store.GetRaw(dm)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw) //nolint:errcheck,gosec // test writes store bytes, no XSS sink
}

// Forged chunk bytes fail the fetch and publish nothing.
func TestBlobDigestMismatchNoPublish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	storeA := testBlobStore(t)
	storeB := testBlobStore(t)
	data := testBlobData(300*1024 + 9)
	root := publishChunked(t, storeA, data)
	_, m := blob.Split(data)
	srv := httptest.NewServer(&forgedRegistry{
		store:   storeA,
		forged:  m.Chunks[0],
		payload: []byte("forged-chunk-bytes"),
	})
	t.Cleanup(srv.Close)
	bad := blobPeer{id: uuid.New(), base: srv.URL + "/v2"}
	assertFetchFails(t, ctx, storeB, root, bad)
}

// assertFetchFails fails when a forged fetch succeeds or publishes.
func assertFetchFails(t *testing.T, ctx context.Context, store *blob.Store, root string, bad blobPeer) {
	t.Helper()
	syncer := New(store, nil, nil)
	if syncer.fetchOneRoot(ctx, root, []blobPeer{bad}) {
		t.Fatalf("fetchOneRoot succeeded with forged chunk")
	}
	complete, err := syncer.isComplete(ctx, root)
	if err != nil {
		t.Fatalf("isComplete: %v", err)
	}
	if complete {
		t.Fatalf("complete true after forgery")
	}
	if _, ok := store.ResolveTag(transport.BlobRepo, strings.TrimPrefix(root, "sha256:")); ok {
		t.Fatalf("manifest tag published after mismatch")
	}
}
