// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package blob

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// makeData builds deterministic pseudo random bytes of length n.
func makeData(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((i*31 + 7) % 256)
	}
	return b
}

// chunkFile maps a chunk digest to its on disk path for tests.
func chunkFile(t *testing.T, dir, digest string) string {
	t.Helper()
	hx := strings.TrimPrefix(digest, "sha256:")
	return filepath.Join(dir, "chunks", hx[:2], hx[2:])
}

// mustOpen creates a store in a temp dir for tests.
func mustOpen(t *testing.T) (*Store, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), DefaultDirName)
	s, err := Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return s, dir
}

// mustPutBlob stores data and fails the test on error.
func mustPutBlob(t *testing.T, s *Store, ctx context.Context, data []byte) string {
	t.Helper()
	digest, err := s.PutBlob(ctx, data)
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	return digest
}

// mustReadAll reads and closes rc, failing on read error.
func mustReadAll(t *testing.T, rc io.ReadCloser) []byte {
	t.Helper()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	err = rc.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	return got
}

// mustPutRaw stores raw bytes and fails on error.
func mustPutRaw(t *testing.T, s *Store, ctx context.Context, digest string, data []byte) {
	t.Helper()
	err := s.PutRaw(ctx, digest, data)
	if err != nil {
		t.Fatalf("PutRaw: %v", err)
	}
}

// mustGetRaw fetches raw bytes and fails on error.
func mustGetRaw(t *testing.T, s *Store, digest string) []byte {
	t.Helper()
	got, err := s.GetRaw(digest)
	if err != nil {
		t.Fatalf("GetRaw: %v", err)
	}
	return got
}

// readTestFile reads a test data file under the temp store.
func readTestFile(t *testing.T, path string) []byte {
	t.Helper()
	buf, err := os.ReadFile(path) //nolint:gosec // test reads a file it just wrote in temp dir
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	return buf
}

// writeTestFile writes data to a temp store path.
func writeTestFile(t *testing.T, path string, data []byte) {
	t.Helper()
	err := os.WriteFile(path, data, 0o600)
	if err != nil {
		t.Fatalf("write file: %v", err)
	}
}

// checkSplitCase validates one Split size class.
func checkSplitCase(t *testing.T, size, wantChunks int) {
	t.Helper()
	data := makeData(size)
	chunks, m := Split(data)
	if len(chunks) != wantChunks {
		t.Fatalf("got %d chunks want %d", len(chunks), wantChunks)
	}
	if len(m.Chunks) != wantChunks {
		t.Fatalf("got %d digests want %d", len(m.Chunks), wantChunks)
	}
	checkSplitManifest(t, m, size)
	checkSplitBytes(t, chunks, m, data)
}

// checkSplitManifest validates manifest fields.
func checkSplitManifest(t *testing.T, m Manifest, size int) {
	t.Helper()
	if m.Version != 1 {
		t.Fatalf("bad version %d", m.Version)
	}
	if m.ChunkSize != ChunkSize {
		t.Fatalf("bad chunk size %d", m.ChunkSize)
	}
	if m.TotalSize != int64(size) {
		t.Fatalf("bad total %d", m.TotalSize)
	}
}

// checkSplitBytes validates chunk digests and reassembly.
func checkSplitBytes(t *testing.T, chunks [][]byte, m Manifest, data []byte) {
	t.Helper()
	got := []byte{}
	for i, c := range chunks {
		sum := sha256.Sum256(c)
		want := "sha256:" + hex.EncodeToString(sum[:])
		if m.Chunks[i] != want {
			t.Fatalf("chunk %d digest mismatch", i)
		}
		if i < len(chunks)-1 && len(c) != ChunkSize {
			t.Fatalf("middle chunk %d size %d", i, len(c))
		}
		got = append(got, c...)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("reassembled bytes differ")
	}
	root, err := manifestRoot(m.Chunks)
	if err != nil {
		t.Fatalf("manifestRoot: %v", err)
	}
	if root != m.Root {
		t.Fatalf("root mismatch")
	}
}

// TestSplit checks chunk counts and manifest fields per size class.
func TestSplit(t *testing.T) {
	cases := []struct {
		name       string
		size       int
		wantChunks int
	}{
		{"empty", 0, 0},
		{"tiny", 13, 1},
		{"one below chunk", ChunkSize - 1, 1},
		{"exact chunk", ChunkSize, 1},
		{"chunk plus one", ChunkSize + 1, 2},
		{"three chunks plus tail", 3*ChunkSize + 100, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkSplitCase(t, tc.size, tc.wantChunks)
		})
	}
}

// checkPutGetBlob round trips one size through the store.
func checkPutGetBlob(t *testing.T, size int) {
	t.Helper()
	s, _ := mustOpen(t)
	ctx := context.Background()
	data := makeData(size)
	digest := mustPutBlob(t, s, ctx, data)
	if !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("bad digest %q", digest)
	}
	if !s.HasBlob(digest) {
		t.Fatalf("HasBlob false after put")
	}
	checkBlobSize(t, s, digest, size)
	checkBlobRead(t, s, digest, data)
	checkBlobIdempotent(t, s, ctx, data, digest)
}

// checkBlobSize validates GetBlobSize.
func checkBlobSize(t *testing.T, s *Store, digest string, size int) {
	t.Helper()
	got, ok := s.GetBlobSize(digest)
	if !ok || got != int64(size) {
		t.Fatalf("GetBlobSize got %d %v", got, ok)
	}
}

// checkBlobRead validates OpenBlob streaming read.
func checkBlobRead(t *testing.T, s *Store, digest string, data []byte) {
	t.Helper()
	rc, size, err := s.OpenBlob(digest)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	if size != int64(len(data)) {
		t.Fatalf("OpenBlob size %d", size)
	}
	got := mustReadAll(t, rc)
	if !bytes.Equal(got, data) {
		t.Fatalf("round trip bytes differ")
	}
}

// checkBlobIdempotent validates re-push returns the same digest.
func checkBlobIdempotent(t *testing.T, s *Store, ctx context.Context, data []byte, digest string) {
	t.Helper()
	again, err := s.PutBlob(ctx, data)
	if err != nil {
		t.Fatalf("second PutBlob: %v", err)
	}
	if again != digest {
		t.Fatalf("digest changed on re-push")
	}
}

// TestPutGetBlob round trips blobs of several sizes through the store.
func TestPutGetBlob(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		{"empty", 0},
		{"tiny", 13},
		{"exact chunk", ChunkSize},
		{"multi chunk", 2*ChunkSize + 777},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkPutGetBlob(t, tc.size)
		})
	}
}

// TestBlobMissing covers unknown and malformed digests.
func TestBlobMissing(t *testing.T) {
	s, _ := mustOpen(t)
	missing := "sha256:" + strings.Repeat("0", 64)
	if s.HasBlob(missing) {
		t.Fatalf("HasBlob true for missing")
	}
	if _, ok := s.GetBlobSize(missing); ok {
		t.Fatalf("GetBlobSize true for missing")
	}
	if _, _, err := s.OpenBlob(missing); err == nil {
		t.Fatalf("OpenBlob nil error for missing")
	}
	bad := []string{"", "sha256:xyz", "md5:" + strings.Repeat("0", 32), "sha256:" + strings.Repeat("G", 64)}
	for _, d := range bad {
		if s.HasBlob(d) {
			t.Fatalf("HasBlob true for %q", d)
		}
		if s.HasRaw(d) {
			t.Fatalf("HasRaw true for %q", d)
		}
		if _, _, err := s.OpenBlob(d); err == nil {
			t.Fatalf("OpenBlob nil error for %q", d)
		}
		if _, err := s.GetRaw(d); err == nil {
			t.Fatalf("GetRaw nil error for %q", d)
		}
	}
}

// TestReopenResume reopens the store on the same dir and reads back.
func TestReopenResume(t *testing.T) {
	dir := filepath.Join(t.TempDir(), DefaultDirName)
	s1, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	ctx := context.Background()
	data := makeData(ChunkSize + 123)
	digest, err := s1.PutBlob(ctx, data)
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	// Reopen simulates a restart. Persisted files back the blob.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if !s2.HasBlob(digest) {
		t.Fatalf("HasBlob false after reopen")
	}
	rc, _, err := s2.OpenBlob(digest)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	err = rc.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("bytes differ after reopen")
	}
}

// expectReadMismatch reads rc expecting a mismatch error.
func expectReadMismatch(t *testing.T, rc io.ReadCloser) {
	t.Helper()
	_, err := io.ReadAll(rc)
	_ = rc.Close() //nolint:errcheck // close after failed read, error already captured
	if err == nil {
		t.Fatalf("expected corruption error")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("unexpected error %v", err)
	}
}

// TestCorruptChunk detects a flipped byte in the middle chunk.
func TestCorruptChunk(t *testing.T) {
	s, dir := mustOpen(t)
	ctx := context.Background()
	data := makeData(2*ChunkSize + 10)
	digest := mustPutBlob(t, s, ctx, data)
	_, m := Split(data)
	target := chunkFile(t, dir, m.Chunks[1])
	buf := readTestFile(t, target)
	buf[0] ^= 0xff
	writeTestFile(t, target, buf)
	rc, _, err := s.OpenBlob(digest)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	expectReadMismatch(t, rc)
}

// checkRawCase covers one PutRaw round trip case.
func checkRawCase(t *testing.T, s *Store, ctx context.Context, dir string, data []byte) {
	t.Helper()
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if s.HasRaw(digest) {
		t.Fatalf("HasRaw true before put")
	}
	mustPutRaw(t, s, ctx, digest, data)
	if !s.HasRaw(digest) {
		t.Fatalf("HasRaw false after put")
	}
	checkRawIdempotent(t, s, ctx, dir, digest, data)
	got := mustGetRaw(t, s, digest)
	if !bytes.Equal(got, data) {
		t.Fatalf("raw bytes differ")
	}
}

// checkRawIdempotent validates re-push skips the write.
func checkRawIdempotent(t *testing.T, s *Store, ctx context.Context, dir, digest string, data []byte) {
	t.Helper()
	before, err := os.Stat(chunkFile(t, dir, digest))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	mustPutRaw(t, s, ctx, digest, data)
	after, err := os.Stat(chunkFile(t, dir, digest))
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatalf("re-push rewrote identical bytes")
	}
}

// TestRawRoundTrip covers PutRaw, HasRaw, and GetRaw verification.
func TestRawRoundTrip(t *testing.T) {
	s, dir := mustOpen(t)
	ctx := context.Background()
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"json", []byte(`{"schemaVersion":2}`)},
		{"layer like", makeData(1024)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkRawCase(t, s, ctx, dir, tc.data)
		})
	}
}

// TestRawMismatch covers digest content mismatch and corruption.
func TestRawMismatch(t *testing.T) {
	s, dir := mustOpen(t)
	ctx := context.Background()
	data := []byte("hello")
	other := "sha256:" + strings.Repeat("1", 64)
	err := s.PutRaw(ctx, other, data)
	if err == nil {
		t.Fatalf("expected digest mismatch error")
	}
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	mustPutRaw(t, s, ctx, digest, data)
	path := chunkFile(t, dir, digest)
	buf := readTestFile(t, path)
	buf[0] ^= 0xff
	writeTestFile(t, path, buf)
	if _, err := s.GetRaw(digest); err == nil {
		t.Fatalf("expected corruption error")
	}
}

// tagCase binds a repo and tag to a digest.
type tagCase struct {
	repo   string
	tag    string
	digest string
}

// putTestTags stores the tag cases for TestTagRoundTrip.
func putTestTags(t *testing.T, s *Store, cases []tagCase) {
	t.Helper()
	for _, tc := range cases {
		err := s.PutTag(tc.repo, tc.tag, tc.digest)
		if err != nil {
			t.Fatalf("PutTag %s:%s: %v", tc.repo, tc.tag, err)
		}
	}
}

// checkTestTags resolves each tag case.
func checkTestTags(t *testing.T, s *Store, cases []tagCase) {
	t.Helper()
	for _, tc := range cases {
		got, ok := s.ResolveTag(tc.repo, tc.tag)
		if !ok || got != tc.digest {
			t.Fatalf("ResolveTag %s:%s got %q %v", tc.repo, tc.tag, got, ok)
		}
	}
}

// checkTagLists validates ListTags outputs.
func checkTagLists(t *testing.T, s *Store) {
	t.Helper()
	tags := s.ListTags("app")
	if len(tags) != 2 || tags[0] != "latest" || tags[1] != "v2" {
		t.Fatalf("ListTags app got %q", tags)
	}
	if got := s.ListTags("library/nginx"); len(got) != 1 || got[0] != "stable" {
		t.Fatalf("ListTags namespaced got %q", got)
	}
	if got := s.ListTags("missing"); len(got) != 0 {
		t.Fatalf("ListTags missing got %q", got)
	}
	if _, ok := s.ResolveTag("app", "nope"); ok {
		t.Fatalf("ResolveTag true for missing tag")
	}
}

// checkTagRetag validates overwriting a tag.
func checkTagRetag(t *testing.T, s *Store, d2 string) {
	t.Helper()
	err := s.PutTag("app", "latest", d2)
	if err != nil {
		t.Fatalf("retag: %v", err)
	}
	got, ok := s.ResolveTag("app", "latest")
	if !ok || got != d2 {
		t.Fatalf("retag resolve got %q %v", got, ok)
	}
}

// TestTagRoundTrip covers put, resolve, and list across repos.
func TestTagRoundTrip(t *testing.T) {
	s, _ := mustOpen(t)
	ctx := context.Background()
	d1 := mustPutBlob(t, s, ctx, makeData(100))
	d2 := mustPutBlob(t, s, ctx, makeData(200))
	cases := []tagCase{
		{"app", "latest", d1},
		{"app", "v2", d2},
		{"library/nginx", "stable", d1},
	}
	putTestTags(t, s, cases)
	checkTestTags(t, s, cases)
	checkTagLists(t, s)
	checkTagRetag(t, s, d2)
}

// TestTagRejectTraversal rejects unsafe repos, tags, and digests.
func TestTagRejectTraversal(t *testing.T) {
	s, _ := mustOpen(t)
	digest := "sha256:" + strings.Repeat("a", 64)
	cases := []struct {
		repo string
		tag  string
	}{
		{"../evil", "x"},
		{"a/../../b", "x"},
		{"", "x"},
		{"app", "../x"},
		{"app", ""},
		{"app", "."},
		{"app", ".."},
		{"app", "a/b"},
	}
	for _, tc := range cases {
		if err := s.PutTag(tc.repo, tc.tag, digest); err == nil {
			t.Fatalf("PutTag accepted %q:%q", tc.repo, tc.tag)
		}
		if _, ok := s.ResolveTag(tc.repo, tc.tag); ok {
			t.Fatalf("ResolveTag accepted %q:%q", tc.repo, tc.tag)
		}
	}
	if err := s.PutTag("app", "x", "not-a-digest"); err == nil {
		t.Fatalf("PutTag accepted bad digest")
	}
	if got := s.ListTags("../evil"); len(got) != 0 {
		t.Fatalf("ListTags traversal got %q", got)
	}
}

// TestContextCancel checks that canceled contexts block writes.
func TestContextCancel(t *testing.T) {
	s, _ := mustOpen(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.PutBlob(ctx, makeData(10)); err == nil {
		t.Fatalf("PutBlob accepted canceled context")
	}
	sum := sha256.Sum256([]byte("x"))
	d := "sha256:" + hex.EncodeToString(sum[:])
	if err := s.PutRaw(ctx, d, []byte("x")); err == nil {
		t.Fatalf("PutRaw accepted canceled context")
	}
}

// TestReaderClosed checks reads after close fail and EOF is stable.
func TestReaderClosed(t *testing.T) {
	s, _ := mustOpen(t)
	ctx := context.Background()
	digest := mustPutBlob(t, s, ctx, makeData(10))
	rc, _, err := s.OpenBlob(digest)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	_, err = io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	_, err = rc.Read(make([]byte, 4))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF got %v", err)
	}
	err = rc.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	if _, err := rc.Read(make([]byte, 4)); err == nil {
		t.Fatalf("expected error after close")
	}
}

// corruptSameSize flips a byte while keeping file size.
func corruptSameSize(t *testing.T, target string) []byte {
	t.Helper()
	buf := readTestFile(t, target)
	corrupt := make([]byte, len(buf))
	copy(corrupt, buf)
	corrupt[0] ^= 0xff
	if bytes.Equal(corrupt, buf) {
		corrupt[0] ^= 0x01
	}
	writeTestFile(t, target, corrupt)
	fi, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if fi.Size() != int64(len(buf)) {
		t.Fatalf("size changed during corrupt setup")
	}
	return buf
}

// checkBlobCorruptRead validates Has stays true but read fails.
func checkBlobCorruptRead(t *testing.T, s *Store, digest string) {
	t.Helper()
	if !s.HasBlob(digest) {
		t.Fatalf("HasBlob should stay true on same size corrupt file")
	}
	rc, _, err := s.OpenBlob(digest)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	_, err = io.ReadAll(rc)
	_ = rc.Close() //nolint:errcheck // close after failed read, error already captured
	if err == nil {
		t.Fatalf("expected read error on corrupt same size chunk")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("unexpected error %v", err)
	}
}

// checkBlobRepair validates re-push repairs and reads back.
func checkBlobRepair(t *testing.T, s *Store, ctx context.Context, data []byte, digest string) {
	t.Helper()
	_, err := s.PutBlob(ctx, data)
	if err != nil {
		t.Fatalf("repair PutBlob: %v", err)
	}
	rc, _, err := s.OpenBlob(digest)
	if err != nil {
		t.Fatalf("OpenBlob after repair: %v", err)
	}
	got := mustReadAll(t, rc)
	if !bytes.Equal(got, data) {
		t.Fatalf("bytes differ after repair")
	}
}

// checkRawCorrupt validates raw Has stays true but GetRaw fails.
func checkRawCorrupt(t *testing.T, s *Store, ctx context.Context, dir string) {
	t.Helper()
	raw := []byte("raw-bytes-here")
	sum := sha256.Sum256(raw)
	rdigest := "sha256:" + hex.EncodeToString(sum[:])
	mustPutRaw(t, s, ctx, rdigest, raw)
	rpath := chunkFile(t, dir, rdigest)
	rbuf := readTestFile(t, rpath)
	rbuf[0] ^= 0xff
	writeTestFile(t, rpath, rbuf)
	if !s.HasRaw(rdigest) {
		t.Fatalf("HasRaw should stay true on same size corrupt file")
	}
	if _, err := s.GetRaw(rdigest); err == nil {
		t.Fatalf("expected GetRaw corruption error")
	}
	mustPutRaw(t, s, ctx, rdigest, raw)
	got := mustGetRaw(t, s, rdigest)
	if !bytes.Equal(got, raw) {
		t.Fatalf("raw bytes differ after repair")
	}
}

// TestCorruptSameSizeRejected proves Has stays existence only while
// reads always hash, and that re-push repairs the corrupt file.
func TestCorruptSameSizeRejected(t *testing.T) {
	s, dir := mustOpen(t)
	ctx := context.Background()
	data := makeData(100)
	digest := mustPutBlob(t, s, ctx, data)
	_, m := Split(data)
	target := chunkFile(t, dir, m.Chunks[0])
	corruptSameSize(t, target)
	checkBlobCorruptRead(t, s, digest)
	checkBlobRepair(t, s, ctx, data, digest)
	checkRawCorrupt(t, s, ctx, dir)
}

// checkBoundsCase validates one TotalSize mutation is rejected.
func checkBoundsCase(t *testing.T, m Manifest, name string, mutate func(*Manifest)) {
	t.Helper()
	cp := m
	cp.Chunks = append([]string(nil), m.Chunks...)
	mutate(&cp)
	if err := checkManifest(m.Root, &cp); err == nil {
		t.Fatalf("expected bounds error for %s", name)
	}
}

// checkOversizeManifest validates oversize files are rejected.
func checkOversizeManifest(t *testing.T, s *Store, dir string) {
	t.Helper()
	big := "sha256:" + strings.Repeat("b", 64)
	bigBuf := make([]byte, maxManifestBytes+1)
	for i := range bigBuf {
		bigBuf[i] = 'x'
	}
	writeTestFile(t, filepath.Join(dir, "manifests", big+".json"), bigBuf)
	if _, _, err := s.OpenBlob(big); err == nil {
		t.Fatalf("expected too large manifest error")
	} else if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("unexpected error %v", err)
	}
}

// checkBadTotalManifest validates bad TotalSize fails on open.
func checkBadTotalManifest(t *testing.T, s *Store) {
	t.Helper()
	_, good := Split(makeData(10))
	good.TotalSize = int64(ChunkSize) + 1
	buf, err := json.Marshal(good) //nolint:errchkjson // Manifest holds only strings and ints
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mp, err := s.manifestPath(good.Root)
	if err != nil {
		t.Fatalf("manifestPath: %v", err)
	}
	writeTestFile(t, mp, buf)
	if _, _, err := s.OpenBlob(good.Root); err == nil {
		t.Fatalf("expected TotalSize bounds error")
	}
}

// TestManifestTotalSizeBounds checks TotalSize against chunk count rules.
func TestManifestTotalSizeBounds(t *testing.T) {
	_, m := Split(makeData(ChunkSize + 10))
	if err := checkManifest(m.Root, &m); err != nil {
		t.Fatalf("valid manifest rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*Manifest)
	}{
		{"zero with chunks", func(mm *Manifest) { mm.TotalSize = 0 }},
		{"too small", func(mm *Manifest) { mm.TotalSize = int64(ChunkSize) }},
		{"too large", func(mm *Manifest) { mm.TotalSize = 3 * int64(ChunkSize) }},
		{"negative", func(mm *Manifest) { mm.TotalSize = -1 }},
		{"empty count", func(mm *Manifest) { mm.TotalSize = 1; mm.Chunks = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			checkBoundsCase(t, m, tc.name, tc.mutate)
		})
	}
	s, dir := mustOpen(t)
	checkOversizeManifest(t, s, dir)
	checkBadTotalManifest(t, s)
}

// makeLeftover builds temp file paths for sweep tests.
func makeLeftover(t *testing.T, dir, digest, chunkDir, tagDir string) []string {
	t.Helper()
	leftover := []string{
		filepath.Join(chunkDir, ".tmp-123"),
		filepath.Join(dir, "manifests", ".tmp-456"),
		filepath.Join(dir, "manifests", digest+".json.tmphold"),
		filepath.Join(tagDir, ".tmp-789"),
	}
	for _, p := range leftover {
		writeTestFile(t, p, []byte("tmp"))
	}
	return leftover
}

// checkSwept validates temp files were removed.
func checkSwept(t *testing.T, leftover []string) {
	t.Helper()
	for _, p := range leftover {
		if strings.HasSuffix(p, ".tmphold") {
			continue
		}
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("tmp file not swept: %s", p)
		}
	}
}

// TestSweepTmpRemovesCrashFiles checks Open sweeps stale temp files.
func TestSweepTmpRemovesCrashFiles(t *testing.T) {
	s, dir := mustOpen(t)
	ctx := context.Background()
	digest := mustPutBlob(t, s, ctx, makeData(10))
	_, m := Split(makeData(10))
	chunkDir := filepath.Dir(chunkFile(t, dir, m.Chunks[0]))
	tagDir := filepath.Join(dir, "tags", "app")
	err := os.MkdirAll(tagDir, 0o700)
	if err != nil {
		t.Fatalf("mkdir tags: %v", err)
	}
	leftover := makeLeftover(t, dir, digest, chunkDir, tagDir)
	// Reopen must sweep the .tmp-* files.
	s2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	_ = s2
	checkSwept(t, leftover)
	if !s.HasBlob(digest) {
		t.Fatalf("blob missing after sweep")
	}
}

// TestListTagsIgnoresTmp checks dotfiles and temp files are skipped.
func TestListTagsIgnoresTmp(t *testing.T) {
	s, dir := mustOpen(t)
	ctx := context.Background()
	d := mustPutBlob(t, s, ctx, makeData(10))
	err := s.PutTag("app", "v1", d)
	if err != nil {
		t.Fatalf("PutTag: %v", err)
	}
	tagDir := filepath.Join(dir, "tags", "app")
	for _, name := range []string{".tmp-1", ".hidden", ".tmp-2.json"} {
		writeTestFile(t, filepath.Join(tagDir, name), []byte("x"))
	}
	tags := s.ListTags("app")
	if len(tags) != 1 || tags[0] != "v1" {
		t.Fatalf("ListTags got %q want [v1]", tags)
	}
}

// TestGetRawCapped checks size caps on raw reads.
func TestGetRawCapped(t *testing.T) {
	s, _ := mustOpen(t)
	ctx := context.Background()
	data := makeData(100)
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	mustPutRaw(t, s, ctx, digest, data)
	if _, err := s.GetRawCapped(digest, 10); err == nil {
		t.Fatalf("expected capped error")
	}
	got, err := s.GetRawCapped(digest, int64(len(data)))
	if err != nil {
		t.Fatalf("GetRawCapped: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("capped bytes differ")
	}
}

// checkManifestCorrupt validates HasBlob fails on bad JSON.
func checkManifestCorrupt(t *testing.T, s *Store, mp, digest string) {
	t.Helper()
	writeTestFile(t, mp, []byte("{bad json"))
	if s.HasBlob(digest) {
		t.Fatalf("HasBlob true for corrupt manifest")
	}
}

// checkManifestRepaired validates repair keeps the digest.
func checkManifestRepaired(t *testing.T, s *Store, ctx context.Context, data []byte, digest string) {
	t.Helper()
	again, err := s.PutBlob(ctx, data)
	if err != nil {
		t.Fatalf("repair PutBlob: %v", err)
	}
	if again != digest {
		t.Fatalf("digest changed on repair")
	}
	rc, _, err := s.OpenBlob(digest)
	if err != nil {
		t.Fatalf("OpenBlob after repair: %v", err)
	}
	got := mustReadAll(t, rc)
	if !bytes.Equal(got, data) {
		t.Fatalf("bytes differ after manifest repair")
	}
}

// TestManifestRepair checks PutBlob rewrites a corrupt manifest.
func TestManifestRepair(t *testing.T) {
	s, _ := mustOpen(t)
	ctx := context.Background()
	data := makeData(100)
	digest := mustPutBlob(t, s, ctx, data)
	mp, err := s.manifestPath(digest)
	if err != nil {
		t.Fatalf("manifestPath: %v", err)
	}
	checkManifestCorrupt(t, s, mp, digest)
	checkManifestRepaired(t, s, ctx, data, digest)
}

// TestTagDotReject checks dot leading tags are rejected.
func TestTagDotReject(t *testing.T) {
	s, _ := mustOpen(t)
	digest := "sha256:" + strings.Repeat("a", 64)
	for _, tag := range []string{".tmp-1", ".hidden", ".tmp-2.json", ".x"} {
		err := s.PutTag("app", tag, digest)
		if err == nil {
			t.Fatalf("PutTag accepted dot tag %q", tag)
		}
		if _, ok := s.ResolveTag("app", tag); ok {
			t.Fatalf("ResolveTag accepted dot tag %q", tag)
		}
	}
}

// TestPathValidation checks chunk and manifest path validation.
func TestPathValidation(t *testing.T) {
	s, _ := mustOpen(t)
	badHex := []string{"", "ab", strings.Repeat("z", 64), "ab/cd", strings.Repeat("a", 63)}
	for _, hx := range badHex {
		if _, err := s.chunkPath(hx); err == nil {
			t.Fatalf("chunkPath accepted %q", hx)
		}
	}
	goodHx := strings.Repeat("a", 64)
	cp, err := s.chunkPath(goodHx)
	if err != nil {
		t.Fatalf("chunkPath rejected good hex: %v", err)
	}
	if cp == "" {
		t.Fatalf("empty chunk path")
	}
	badDig := []string{"", "sha256:xyz", "md5:" + strings.Repeat("0", 64)}
	for _, d := range badDig {
		if _, err := s.manifestPath(d); err == nil {
			t.Fatalf("manifestPath accepted %q", d)
		}
	}
}

// TestParentFsync checks parent dirs exist and chain sync works.
func TestParentFsync(t *testing.T) {
	s, dir := mustOpen(t)
	ctx := context.Background()
	data := makeData(10)
	d, err := s.PutBlob(ctx, data)
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	err = s.PutTag("a/b", "v1", d)
	if err != nil {
		t.Fatalf("PutTag: %v", err)
	}
	for _, p := range []string{
		filepath.Join(dir, "chunks"),
		filepath.Join(dir, "manifests"),
		filepath.Join(dir, "tags", "a", "b"),
	} {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if !fi.IsDir() {
			t.Fatalf("not a dir %s", p)
		}
	}
	leaf := filepath.Join(dir, "tags", "a", "b")
	err = syncChain(leaf, dir)
	if err != nil {
		t.Fatalf("syncChain: %v", err)
	}
}

// TestPutRawCap checks PutRaw rejects data over 32MB.
func TestPutRawCap(t *testing.T) {
	s, _ := mustOpen(t)
	ctx := context.Background()
	big := make([]byte, maxRawBytes+1)
	sum := sha256.Sum256(big)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	err := s.PutRaw(ctx, digest, big)
	if err == nil {
		t.Fatalf("PutRaw accepted oversize raw")
	}
	if !strings.Contains(err.Error(), "too large") {
		t.Fatalf("unexpected error %v", err)
	}
}

// concurrentPutGet runs one concurrent put and read cycle.
func concurrentPutGet(t *testing.T, s *Store, ctx context.Context, data []byte, digest string) {
	t.Helper()
	_, err := s.PutBlob(ctx, data)
	if err != nil {
		t.Errorf("concurrent PutBlob: %v", err)
		return
	}
	rc, _, err := s.OpenBlob(digest)
	if err != nil {
		t.Errorf("concurrent OpenBlob: %v", err)
		return
	}
	_, err = io.ReadAll(rc)
	if err != nil {
		t.Errorf("concurrent read: %v", err)
	}
	_ = rc.Close() //nolint:errcheck // best-effort close in concurrent test
}

// TestConcurrentPutGet checks concurrent put and get is safe.
func TestConcurrentPutGet(t *testing.T) {
	s, _ := mustOpen(t)
	ctx := context.Background()
	data := makeData(1000)
	digest := mustPutBlob(t, s, ctx, data)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			concurrentPutGet(t, s, ctx, data, digest)
		})
	}
	wg.Wait()
}
