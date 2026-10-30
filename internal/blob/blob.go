// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// ChunkSize is the fixed split size for blob chunks.
const ChunkSize = 256 * 1024

// DefaultDirName is the directory name used for blob storage under
// the user config concord dir. Callers pass the full dir to Open.
const DefaultDirName = "blobs"

// digestPrefix marks digests as sha256 in hex form.
const digestPrefix = "sha256:"

// manifestVersion is the only manifest version this package reads.
const manifestVersion = 1

// maxManifestBytes caps stored manifest size before read and decode.
const maxManifestBytes = 1 << 20

// maxRawBytes caps GetRaw to prevent OOM on corrupt large files.
const maxRawBytes = 32 << 20

// Manifest binds an ordered chunk list to a root digest.
type Manifest struct {
	Version   int      `json:"version"`
	ChunkSize int      `json:"chunkSize"`
	TotalSize int64    `json:"totalSize"`
	Chunks    []string `json:"chunks"`
	Root      string   `json:"root"`
}

// Split cuts data into ChunkSize pieces and builds the manifest.
// Chunks holds sha256:hex digests, Root is sha256:hex over the
// concatenated chunk hex digests. Split never fails.
func Split(data []byte) ([][]byte, Manifest) {
	var chunks [][]byte
	digests := []string{}
	hexes := []string{}
	for off := 0; off < len(data); off += ChunkSize {
		end := min(off+ChunkSize, len(data))
		c := data[off:end]
		chunks = append(chunks, c)
		sum := sha256.Sum256(c)
		hx := hex.EncodeToString(sum[:])
		hexes = append(hexes, hx)
		digests = append(digests, digestPrefix+hx)
	}
	sum := sha256.Sum256([]byte(strings.Join(hexes, "")))
	m := Manifest{
		Version:   manifestVersion,
		ChunkSize: ChunkSize,
		TotalSize: int64(len(data)),
		Chunks:    digests,
		Root:      digestPrefix + hex.EncodeToString(sum[:]),
	}
	return chunks, m
}

// Store persists chunks, manifests, and tags under a root dir.
// Manifest file names contain a colon and require a Linux filesystem.
// Store is safe for concurrent use by multiple goroutines. Open and
// SweepTmp must not run at the same time as in-flight puts because
// SweepTmp may remove temp files.
type Store struct {
	dir string
}

// ensureStoreDirs creates the store root and subdirs.
func ensureStoreDirs(dir string) error {
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		return fmt.Errorf("blob: create store dir: %w", err)
	}
	for _, sub := range []string{"chunks", "manifests", "tags"} {
		err = os.MkdirAll(filepath.Join(dir, sub), 0o700)
		if err != nil {
			return fmt.Errorf("blob: create %s dir: %w", sub, err)
		}
	}
	return nil
}

// Open creates the store dirs with mode 0700 and returns the store.
// It sweeps stale .tmp-* files left by crashed atomic writes.
func Open(dir string) (*Store, error) {
	err := ensureStoreDirs(dir)
	if err != nil {
		return nil, err
	}
	err = syncDir(dir)
	if err != nil {
		return nil, err
	}
	s := &Store{dir: dir}
	err = s.SweepTmp()
	if err != nil {
		return nil, err
	}
	return s, nil
}

// sweepState counts failures while walking temp files.
type sweepState struct {
	failures int
	firstErr error
}

// record notes one walk or remove failure.
func (st *sweepState) record(err error) {
	st.failures++
	if st.firstErr == nil {
		st.firstErr = err
	}
}

// isSweepTarget reports temp files left by crashed writes.
func isSweepTarget(d fs.DirEntry) bool {
	if d.IsDir() {
		return false
	}
	return strings.HasPrefix(d.Name(), ".tmp-")
}

// sweepOne removes one temp file, recording failures.
func (st *sweepState) sweepOne(path string, d fs.DirEntry, walkErr error) error {
	if walkErr != nil {
		st.record(walkErr)
		return nil
	}
	if !isSweepTarget(d) {
		return nil
	}
	rmErr := os.Remove(path)
	if rmErr != nil && !os.IsNotExist(rmErr) {
		st.record(rmErr)
	}
	return nil
}

// finish converts sweep failures to an error.
func (st *sweepState) finish(walkErr error) error {
	if walkErr != nil {
		return fmt.Errorf("blob: sweep temp files: %w", walkErr)
	}
	if st.failures > 0 {
		return fmt.Errorf("blob: sweep temp files: %d failures: %w", st.failures, st.firstErr)
	}
	return nil
}

// SweepTmp removes stale .tmp-* files left by interrupted atomic
// writes. It walks the whole store dir so sharded chunks, manifests,
// and nested tag dirs are covered. Cleanup is best effort: it removes
// what it can and returns an error counting walk and remove failures.
func (s *Store) SweepTmp() error {
	var st sweepState
	err := filepath.WalkDir(s.dir, st.sweepOne)
	return st.finish(err)
}

// isHexChar reports whether c is lowercase hex.
func isHexChar(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

// checkHexPart validates 64 char lowercase hex.
func checkHexPart(hx, what string) error {
	if len(hx) != 64 {
		return fmt.Errorf("blob: bad %s length in %q", what, hx)
	}
	for i := range len(hx) {
		if !isHexChar(hx[i]) {
			return fmt.Errorf("blob: bad %s char in %q", what, hx)
		}
	}
	return nil
}

// verifyDigest checks sha256:hex form and returns the hex part.
func verifyDigest(digest string) (string, error) {
	if !strings.HasPrefix(digest, digestPrefix) {
		return "", fmt.Errorf("blob: bad digest prefix in %q", digest)
	}
	hx := strings.TrimPrefix(digest, digestPrefix)
	err := checkHexPart(hx, "digest")
	if err != nil {
		return "", err
	}
	return hx, nil
}

// chunkPath maps chunk hex to dir/chunks/ab/cdef sharding.
// It validates 64 char lowercase hex with no separators.
func (s *Store) chunkPath(hx string) (string, error) {
	err := checkHexPart(hx, "chunk hex")
	if err != nil {
		return "", err
	}
	return filepath.Join(s.dir, "chunks", hx[:2], hx[2:]), nil
}

// manifestPath maps a root digest to its manifest file.
// The file name contains a colon and requires a Linux filesystem.
func (s *Store) manifestPath(digest string) (string, error) {
	_, err := verifyDigest(digest)
	if err != nil {
		return "", fmt.Errorf("blob: bad manifest digest %q: %w", digest, err)
	}
	return filepath.Join(s.dir, "manifests", digest+".json"), nil
}

// checkNameHead rejects empty, dot, and overlong names.
func checkNameHead(s string) error {
	if s == "" || s == "." || s == ".." {
		return fmt.Errorf("blob: bad name %q", s)
	}
	if s[0] == '.' {
		return fmt.Errorf("blob: bad name %q", s)
	}
	if len(s) > 255 {
		return fmt.Errorf("blob: name too long %q", s)
	}
	return nil
}

// checkNameChar rejects separators and controls.
func checkNameChar(c byte, s string) error {
	if c == '/' || c == '\\' || c == 0 {
		return fmt.Errorf("blob: bad name %q", s)
	}
	if c < 0x20 || c == 0x7f {
		return fmt.Errorf("blob: bad name %q", s)
	}
	return nil
}

// checkName rejects path traversal and unsafe tag or repo segments.
// Leading dots are rejected so tags never collide with .tmp-* temp
// files hidden from ListTags and removed by SweepTmp.
func checkName(s string) error {
	err := checkNameHead(s)
	if err != nil {
		return err
	}
	for i := range len(s) {
		err = checkNameChar(s[i], s)
		if err != nil {
			return err
		}
	}
	return nil
}

// tagPath maps repo and tag to dir/tags/<repo>/<tag> after validation.
// Repos may contain slashes for namespaces, each segment is checked.
func (s *Store) tagPath(repo, tag string) (string, error) {
	parts := strings.Split(repo, "/")
	for _, part := range parts {
		err := checkName(part)
		if err != nil {
			return "", fmt.Errorf("blob: bad repo: %w", err)
		}
	}
	err := checkName(tag)
	if err != nil {
		return "", fmt.Errorf("blob: bad tag: %w", err)
	}
	elems := append([]string{s.dir, "tags"}, parts...)
	elems = append(elems, tag)
	return filepath.Join(elems...), nil
}

// statSize stats a regular file, reporting false when absent.
func statSize(path string) (int64, bool) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, false
	}
	if !fi.Mode().IsRegular() {
		return 0, false
	}
	return fi.Size(), true
}

// writeAll writes the full buffer to the file.
func writeAll(f *os.File, data []byte) error {
	off := 0
	for off < len(data) {
		n, err := f.Write(data[off:])
		if err != nil {
			return fmt.Errorf("blob: write temp file: %w", err)
		}
		off += n
	}
	return nil
}

// syncDir fsyncs a directory so renames inside it are durable.
func syncDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // dir is a store-owned directory path
	if err != nil {
		return fmt.Errorf("blob: open dir for sync: %w", err)
	}
	err = f.Sync()
	if err != nil {
		_ = f.Close() //nolint:errcheck // close error folded into sync error
		return fmt.Errorf("blob: sync dir: %w", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("blob: close dir: %w", err)
	}
	return nil
}

// chainScope checks dir is at or under stop.
func chainScope(cleanDir, cleanStop string) (string, bool) {
	rel, err := filepath.Rel(cleanStop, cleanDir)
	if err != nil {
		return cleanDir, false
	}
	if rel == "." {
		return cleanDir, false
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return cleanDir, false
	}
	return rel, true
}

// syncParents fsyncs cur up to stop inclusive.
func syncParents(cur, cleanStop string) error {
	for {
		err := syncDir(cur)
		if err != nil {
			return err
		}
		if cur == cleanStop {
			return nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return nil
		}
		cur = parent
	}
}

// syncChain fsyncs dir and each parent up to stop inclusive.
// It bounds the window where a new dir entry could be lost.
func syncChain(dir, stop string) error {
	cleanDir := filepath.Clean(dir)
	cleanStop := filepath.Clean(stop)
	_, ok := chainScope(cleanDir, cleanStop)
	if !ok {
		return syncDir(cleanDir)
	}
	return syncParents(cleanDir, cleanStop)
}

// abortTemp closes and removes a temp file after a failed step.
func abortTemp(tmp *os.File, tmpName string) {
	_ = tmp.Close()        //nolint:errcheck // close error folded into caller error
	_ = os.Remove(tmpName) //nolint:errcheck // best-effort temp cleanup
}

// finishTemp syncs, closes, and renames the temp file.
func finishTemp(tmp *os.File, tmpName, path string) error {
	err := tmp.Sync()
	if err != nil {
		abortTemp(tmp, tmpName)
		return fmt.Errorf("blob: sync temp file: %w", err)
	}
	err = tmp.Close()
	if err != nil {
		_ = os.Remove(tmpName) //nolint:errcheck // best-effort temp cleanup
		return fmt.Errorf("blob: close temp file: %w", err)
	}
	err = os.Rename(tmpName, path)
	if err != nil {
		_ = os.Remove(tmpName) //nolint:errcheck // best-effort temp cleanup
		return fmt.Errorf("blob: rename temp file: %w", err)
	}
	return nil
}

// writeFileAtomic writes data via a temp file beside dest plus rename,
// then fsyncs the file and the dir chain up to the store root.
func (s *Store) writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		return fmt.Errorf("blob: create parent dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("blob: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	err = writeAll(tmp, data)
	if err != nil {
		abortTemp(tmp, tmpName)
		return err
	}
	err = finishTemp(tmp, tmpName, path)
	if err != nil {
		return err
	}
	err = syncChain(dir, s.dir)
	if err != nil {
		return err
	}
	return nil
}

// manifestRoot recomputes the root digest for a chunk digest list.
func manifestRoot(digests []string) (string, error) {
	hexes := make([]string, 0, len(digests))
	for _, d := range digests {
		hx, err := verifyDigest(d)
		if err != nil {
			return "", fmt.Errorf("blob: bad chunk digest %q: %w", d, err)
		}
		hexes = append(hexes, hx)
	}
	sum := sha256.Sum256([]byte(strings.Join(hexes, "")))
	return digestPrefix + hex.EncodeToString(sum[:]), nil
}

// checkManifestHeader validates version, sizes, and root fields.
func checkManifestHeader(digest string, m *Manifest) error {
	if m.Version != manifestVersion {
		return fmt.Errorf("blob: unsupported manifest version %d", m.Version)
	}
	if m.ChunkSize != ChunkSize {
		return fmt.Errorf("blob: bad manifest chunk size %d", m.ChunkSize)
	}
	if m.TotalSize < 0 {
		return fmt.Errorf("blob: negative manifest size %d", m.TotalSize)
	}
	if m.Root != digest {
		return fmt.Errorf("blob: manifest root mismatch") //nolint:perfsprint // plain sentinel, no wrap target
	}
	return nil
}

// checkManifestCounts validates TotalSize against chunk count.
func checkManifestCounts(m *Manifest) error {
	if m.TotalSize == 0 && len(m.Chunks) != 0 {
		return fmt.Errorf("blob: empty blob must have no chunks") //nolint:perfsprint // plain sentinel, no wrap target
	}
	if m.TotalSize > 0 && len(m.Chunks) == 0 {
		return fmt.Errorf("blob: non-empty blob must have chunks") //nolint:perfsprint // plain sentinel, no wrap target
	}
	n := int64(len(m.Chunks))
	if n == 0 {
		return nil
	}
	if m.TotalSize <= (n-1)*int64(ChunkSize) {
		return fmt.Errorf("blob: total size too small for chunk count") //nolint:perfsprint // plain sentinel, no wrap target
	}
	if m.TotalSize > n*int64(ChunkSize) {
		return fmt.Errorf("blob: total size too large for chunk count") //nolint:perfsprint // plain sentinel, no wrap target
	}
	return nil
}

// checkManifestRoot recomputes and compares the root digest.
func checkManifestRoot(digest string, m *Manifest) error {
	root, err := manifestRoot(m.Chunks)
	if err != nil {
		return fmt.Errorf("blob: manifest root for %q: %w", digest, err)
	}
	if root != digest {
		return fmt.Errorf("blob: manifest root verification failed") //nolint:perfsprint // plain sentinel, no wrap target
	}
	return nil
}

// checkManifest validates a manifest against its expected root digest.
func checkManifest(digest string, m *Manifest) error {
	err := checkManifestHeader(digest, m)
	if err != nil {
		return err
	}
	err = checkManifestCounts(m)
	if err != nil {
		return err
	}
	err = checkManifestRoot(digest, m)
	if err != nil {
		return err
	}
	return nil
}

// readManifestBytes reads a manifest file with a size cap.
func readManifestBytes(mp string) ([]byte, error) {
	size, ok := statSize(mp)
	if !ok {
		return nil, fmt.Errorf("blob: read manifest: %w", os.ErrNotExist)
	}
	if size > int64(maxManifestBytes) {
		return nil, fmt.Errorf("blob: manifest too large %d", size)
	}
	f, err := os.Open(mp) //nolint:gosec // mp is built from a validated digest under the store root
	if err != nil {
		return nil, fmt.Errorf("blob: read manifest: %w", err)
	}
	buf, err := io.ReadAll(io.LimitReader(f, int64(maxManifestBytes)+1))
	if err != nil {
		_ = f.Close() //nolint:errcheck // close error folded into read error
		return nil, fmt.Errorf("blob: read manifest: %w", err)
	}
	err = f.Close()
	if err != nil {
		return nil, fmt.Errorf("blob: close manifest: %w", err)
	}
	return buf, nil
}

// decodeManifest parses and validates manifest bytes.
func decodeManifest(digest string, buf []byte) (Manifest, error) {
	var m Manifest
	if len(buf) > maxManifestBytes {
		return m, fmt.Errorf("blob: manifest too large %d", len(buf))
	}
	err := json.Unmarshal(buf, &m)
	if err != nil {
		return m, fmt.Errorf("blob: decode manifest: %w", err)
	}
	err = checkManifest(digest, &m)
	if err != nil {
		return m, fmt.Errorf("blob: bad manifest %q: %w", digest, err)
	}
	return m, nil
}

// loadManifest reads and validates the manifest for a root digest.
// The file is read with a 1MB cap to bound memory on corrupt files.
func (s *Store) loadManifest(digest string) (Manifest, error) {
	var m Manifest
	_, err := verifyDigest(digest)
	if err != nil {
		return m, fmt.Errorf("blob: bad manifest digest %q: %w", digest, err)
	}
	mp, err := s.manifestPath(digest)
	if err != nil {
		return m, err
	}
	buf, err := readManifestBytes(mp)
	if err != nil {
		return m, err
	}
	return decodeManifest(digest, buf)
}

// fileMatchesHash reports whether path holds bytes hashing to hx.
func fileMatchesHash(path string, data []byte, hx string) (bool, error) {
	size, ok := statSize(path)
	if !ok || size != int64(len(data)) {
		return false, nil
	}
	existing, err := os.ReadFile(path) //nolint:gosec // path is built from a validated hex under the store root
	if err != nil {
		return false, fmt.Errorf("blob: read existing chunk: %w", err)
	}
	esum := sha256.Sum256(existing)
	if hex.EncodeToString(esum[:]) == hx {
		return true, nil
	}
	return false, nil
}

// putChunk stores one chunk, always verifying the hash. A same size
// file is re-read and hashed before skipping the write, so a corrupt
// same size file is overwritten instead of kept.
func (s *Store) putChunk(hx string, data []byte) error {
	sum := sha256.Sum256(data)
	if hex.EncodeToString(sum[:]) != hx {
		return fmt.Errorf("blob: chunk hash mismatch on put") //nolint:perfsprint // plain sentinel, no wrap target
	}
	p, err := s.chunkPath(hx)
	if err != nil {
		return fmt.Errorf("blob: bad chunk hex %q: %w", hx, err)
	}
	hit, err := fileMatchesHash(p, data, hx)
	if err != nil {
		return err
	}
	if hit {
		return nil
	}
	err = s.writeFileAtomic(p, data)
	if err != nil {
		return err
	}
	return nil
}

// storeBlobChunks stores each chunk with context checks.
func (s *Store) storeBlobChunks(ctx context.Context, chunks [][]byte, m Manifest) error {
	for i, c := range chunks {
		err := ctx.Err()
		if err != nil {
			return fmt.Errorf("blob: put blob context: %w", err)
		}
		hx, err := verifyDigest(m.Chunks[i])
		if err != nil {
			return fmt.Errorf("blob: bad chunk digest %q: %w", m.Chunks[i], err)
		}
		err = s.putChunk(hx, c)
		if err != nil {
			return err
		}
	}
	return nil
}

// storeManifestBytes writes the manifest unless a valid one exists.
func (s *Store) storeManifestBytes(m Manifest, buf []byte) error {
	mp, err := s.manifestPath(m.Root)
	if err != nil {
		return err
	}
	_, ok := statSize(mp)
	if ok {
		_, err = s.loadManifest(m.Root)
		if err == nil {
			return nil
		}
	}
	err = s.writeFileAtomic(mp, buf)
	if err != nil {
		return err
	}
	return nil
}

// PutBlob stores data as chunks plus manifest and returns the root
// digest. Re-push of identical data is idempotent and writes zero
// bytes when all files already exist.
func (s *Store) PutBlob(ctx context.Context, data []byte) (string, error) {
	err := ctx.Err()
	if err != nil {
		return "", fmt.Errorf("blob: put blob context: %w", err)
	}
	chunks, m := Split(data)
	err = s.storeBlobChunks(ctx, chunks, m)
	if err != nil {
		return "", err
	}
	buf, err := json.Marshal(m) //nolint:errchkjson // Manifest holds only strings and ints
	if err != nil {
		return "", fmt.Errorf("blob: marshal manifest: %w", err)
	}
	err = s.storeManifestBytes(m, buf)
	if err != nil {
		return "", err
	}
	return m.Root, nil
}

// hasOneChunk reports whether one chunk file exists.
func (s *Store) hasOneChunk(digest string) bool {
	hx, err := verifyDigest(digest)
	if err != nil {
		return false
	}
	cp, err := s.chunkPath(hx)
	if err != nil {
		return false
	}
	_, ok := statSize(cp)
	return ok
}

// HasBlob reports whether the manifest and all chunks are present.
// It is an existence check only. Integrity is enforced on read:
// OpenBlob hashes every chunk before returning its bytes, so a
// corrupt same size file passes HasBlob but fails on read.
func (s *Store) HasBlob(digest string) bool {
	m, err := s.loadManifest(digest)
	if err != nil {
		return false
	}
	for _, c := range m.Chunks {
		if !s.hasOneChunk(c) {
			return false
		}
	}
	return true
}

// GetBlobSize returns the total size recorded in the manifest.
func (s *Store) GetBlobSize(digest string) (int64, bool) {
	m, err := s.loadManifest(digest)
	if err != nil {
		return 0, false
	}
	return m.TotalSize, true
}

// blobReader streams blob bytes one verified chunk at a time. Each
// chunk file is fully read and hash checked before any of its bytes
// are returned, so memory stays flat at one chunk.
type blobReader struct {
	store    *Store
	chunks   []string
	total    int64
	idx      int
	buf      []byte
	off      int
	returned int64
	closed   bool
}

// chunkStat checks stat size rules for one chunk.
func (r *blobReader) chunkStat(d, p string, last bool) error {
	size, ok := statSize(p)
	if !ok {
		return fmt.Errorf("blob: missing chunk %s", d)
	}
	if size > int64(ChunkSize) {
		return fmt.Errorf("blob: oversize chunk %s", d)
	}
	if !last && size != int64(ChunkSize) {
		return fmt.Errorf("blob: short middle chunk %s", d)
	}
	if last && r.total > 0 && size == 0 {
		return fmt.Errorf("blob: empty final chunk %s", d)
	}
	return nil
}

// chunkBytes checks read size rules for one chunk.
func (r *blobReader) chunkBytes(d string, buf []byte, last bool) error {
	if len(buf) > ChunkSize {
		return fmt.Errorf("blob: oversize chunk %s", d)
	}
	if !last && len(buf) != ChunkSize {
		return fmt.Errorf("blob: short middle chunk %s", d)
	}
	if last && r.total > 0 && len(buf) == 0 {
		return fmt.Errorf("blob: empty final chunk %s", d)
	}
	return nil
}

// verifyChunkBytes hashes buf and compares to the digest.
func verifyChunkBytes(d string, buf []byte) error {
	sum := sha256.Sum256(buf)
	if digestPrefix+hex.EncodeToString(sum[:]) != d {
		return fmt.Errorf("blob: chunk hash mismatch %s", d)
	}
	return nil
}

// chunkFile reads and verifies the current chunk.
func (r *blobReader) chunkFile(d, p string, last bool) error {
	buf, err := os.ReadFile(p) //nolint:gosec // p is built from a validated chunk hex under the store root
	if err != nil {
		return fmt.Errorf("blob: read chunk: %w", err)
	}
	err = r.chunkBytes(d, buf, last)
	if err != nil {
		return err
	}
	err = verifyChunkBytes(d, buf)
	if err != nil {
		return err
	}
	r.buf = buf
	r.off = 0
	r.idx++
	return nil
}

// loadNext reads the next chunk file and verifies its hash.
// Size rules are enforced on the bytes actually read, not just on
// stat, so a file swapped between stat and read is still rejected.
func (r *blobReader) loadNext() error {
	d := r.chunks[r.idx]
	hx, err := verifyDigest(d)
	if err != nil {
		return fmt.Errorf("blob: bad chunk digest %q: %w", d, err)
	}
	last := r.idx == len(r.chunks)-1
	p, err := r.store.chunkPath(hx)
	if err != nil {
		return fmt.Errorf("blob: bad chunk path %q: %w", d, err)
	}
	err = r.chunkStat(d, p, last)
	if err != nil {
		return err
	}
	return r.chunkFile(d, p, last)
}

// refill loads the next chunk or reports EOF with size check.
func (r *blobReader) refill() error {
	if r.idx >= len(r.chunks) {
		if r.returned != r.total {
			return fmt.Errorf("blob: size mismatch: got %d want %d", r.returned, r.total)
		}
		return io.EOF
	}
	return r.loadNext()
}

// Read returns the next bytes, verifying each chunk before serving it.
func (r *blobReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("blob: read on closed reader") //nolint:perfsprint // plain sentinel, no wrap target
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.off < len(r.buf) {
		return r.serve(p), nil
	}
	err := r.refill()
	if err != nil {
		return 0, err
	}
	return r.serve(p), nil
}

// serve copies buffered bytes to p and advances the offset.
func (r *blobReader) serve(p []byte) int {
	n := copy(p, r.buf[r.off:])
	r.off += n
	r.returned += int64(n)
	return n
}

// Close releases the reader. Reads after Close fail.
func (r *blobReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.buf = nil
	return nil
}

// OpenBlob returns a streaming reader over the verified blob bytes
// plus the total size. The manifest root is checked before opening.
func (s *Store) OpenBlob(digest string) (io.ReadCloser, int64, error) {
	m, err := s.loadManifest(digest)
	if err != nil {
		return nil, 0, err
	}
	r := &blobReader{store: s, chunks: m.Chunks, total: m.TotalSize}
	return r, m.TotalSize, nil
}

// checkRawInput validates digest, context, and size for PutRaw.
func checkRawInput(ctx context.Context, digest string, data []byte) (string, error) {
	hx, err := verifyDigest(digest)
	if err != nil {
		return "", fmt.Errorf("blob: bad raw digest %q: %w", digest, err)
	}
	err = ctx.Err()
	if err != nil {
		return "", fmt.Errorf("blob: put raw context: %w", err)
	}
	if int64(len(data)) > int64(maxRawBytes) {
		return "", fmt.Errorf("blob: raw too large %d", len(data))
	}
	sum := sha256.Sum256(data)
	if digestPrefix+hex.EncodeToString(sum[:]) != digest {
		return "", fmt.Errorf("blob: raw digest mismatch") //nolint:perfsprint // plain sentinel, no wrap target
	}
	return hx, nil
}

// rawUpToDate reports whether the raw file already matches.
func (s *Store) rawUpToDate(p, digest string, data []byte) (bool, error) {
	size, ok := statSize(p)
	if !ok || size != int64(len(data)) {
		return false, nil
	}
	existing, err := os.ReadFile(p) //nolint:gosec // p is built from a validated hex under the store root
	if err != nil {
		return false, fmt.Errorf("blob: read existing raw: %w", err)
	}
	esum := sha256.Sum256(existing)
	if digestPrefix+hex.EncodeToString(esum[:]) == digest {
		return true, nil
	}
	return false, nil
}

// PutRaw stores bytes under a known digest, as used for OCI configs
// and manifests. Raw entries are for small JSON and metadata only.
// Large bytes must use PutBlob chunked. Data over 32MB is rejected
// to match GetRaw. A same size file is re-read and hashed before
// skipping, so corruption is fixed by re-push instead of kept.
func (s *Store) PutRaw(ctx context.Context, digest string, data []byte) error {
	hx, err := checkRawInput(ctx, digest, data)
	if err != nil {
		return err
	}
	p, err := s.chunkPath(hx)
	if err != nil {
		return fmt.Errorf("blob: bad raw hex %q: %w", hx, err)
	}
	hit, err := s.rawUpToDate(p, digest, data)
	if err != nil {
		return err
	}
	if hit {
		return nil
	}
	err = s.writeFileAtomic(p, data)
	if err != nil {
		return err
	}
	return nil
}

// HasRaw reports whether the raw digest file exists. It is an
// existence check only. GetRaw always re-hashes the bytes, so a
// corrupt same size file passes HasRaw but fails GetRaw.
func (s *Store) HasRaw(digest string) bool {
	hx, err := verifyDigest(digest)
	if err != nil {
		return false
	}
	cp, err := s.chunkPath(hx)
	if err != nil {
		return false
	}
	_, ok := statSize(cp)
	return ok
}

// rawLimit resolves the effective cap for raw reads.
func rawLimit(maxSize int64) int64 {
	limit := int64(maxRawBytes)
	if maxSize > 0 && maxSize < limit {
		limit = maxSize
	}
	return limit
}

// rawPath checks digest and maps it to a chunk file.
func (s *Store) rawPath(digest string) (string, error) {
	hx, err := verifyDigest(digest)
	if err != nil {
		return "", fmt.Errorf("blob: bad raw digest %q: %w", digest, err)
	}
	cp, err := s.chunkPath(hx)
	if err != nil {
		return "", fmt.Errorf("blob: bad raw hex %q: %w", hx, err)
	}
	return cp, nil
}

// readRawFile reads and size checks a raw file.
func readRawFile(cp string, limit int64) ([]byte, error) {
	size, ok := statSize(cp)
	if !ok {
		return nil, fmt.Errorf("blob: read raw: %w", os.ErrNotExist)
	}
	if size > limit {
		return nil, fmt.Errorf("blob: raw too large %d", size)
	}
	buf, err := os.ReadFile(cp) //nolint:gosec // cp is built from a validated digest under the store root
	if err != nil {
		return nil, fmt.Errorf("blob: read raw: %w", err)
	}
	if int64(len(buf)) > limit {
		return nil, fmt.Errorf("blob: raw too large %d", len(buf))
	}
	return buf, nil
}

// verifyRawBytes hashes buf and compares to the digest.
func verifyRawBytes(digest string, buf []byte) error {
	sum := sha256.Sum256(buf)
	if digestPrefix+hex.EncodeToString(sum[:]) != digest {
		return fmt.Errorf("blob: raw hash mismatch") //nolint:perfsprint // plain sentinel, no wrap target
	}
	return nil
}

// GetRaw returns the stored bytes after verifying the hash. Files
// over maxRawBytes are rejected before and after read to bound memory.
func (s *Store) GetRaw(digest string) ([]byte, error) {
	return s.GetRawCapped(digest, int64(maxRawBytes))
}

// GetRawCapped returns the stored bytes after verifying the hash,
// rejecting files larger than ceiling. Non-positive ceiling means
// no extra cap beyond maxRawBytes.
func (s *Store) GetRawCapped(digest string, ceiling int64) ([]byte, error) {
	cp, err := s.rawPath(digest)
	if err != nil {
		return nil, err
	}
	limit := rawLimit(ceiling)
	buf, err := readRawFile(cp, limit)
	if err != nil {
		return nil, err
	}
	err = verifyRawBytes(digest, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// PutTag binds repo and tag to a digest.
// Tags starting with a dot are rejected to avoid collision with
// .tmp-* temp files removed by SweepTmp and hidden from ListTags.
func (s *Store) PutTag(repo, tag, digest string) error {
	_, err := verifyDigest(digest)
	if err != nil {
		return fmt.Errorf("blob: bad tag digest %q: %w", digest, err)
	}
	p, err := s.tagPath(repo, tag)
	if err != nil {
		return err
	}
	err = s.writeFileAtomic(p, []byte(digest))
	if err != nil {
		return err
	}
	return nil
}

// readTagDigest reads and validates a tag file.
func readTagDigest(p string) (string, bool) {
	fi, err := os.Stat(p)
	if err != nil {
		return "", false
	}
	if !fi.Mode().IsRegular() {
		return "", false
	}
	buf, err := os.ReadFile(p) //nolint:gosec // p is built from validated repo and tag under the store root
	if err != nil {
		return "", false
	}
	d := strings.TrimSpace(string(buf))
	_, err = verifyDigest(d)
	if err != nil {
		return "", false
	}
	return d, true
}

// ResolveTag returns the digest bound to repo and tag.
func (s *Store) ResolveTag(repo, tag string) (string, bool) {
	p, err := s.tagPath(repo, tag)
	if err != nil {
		return "", false
	}
	return readTagDigest(p)
}

// checkRepoParts validates each repo segment.
func checkRepoParts(repo string) ([]string, bool) {
	parts := strings.Split(repo, "/")
	for _, part := range parts {
		err := checkName(part)
		if err != nil {
			return nil, false
		}
	}
	return parts, true
}

// isTagVisible reports whether a dir entry is a listable tag.
func isTagVisible(e fs.DirEntry) (string, bool) {
	if e.IsDir() {
		return "", false
	}
	name := e.Name()
	if strings.HasPrefix(name, ".") {
		return "", false
	}
	info, err := e.Info()
	if err != nil {
		return "", false
	}
	if !info.Mode().IsRegular() {
		return "", false
	}
	return name, true
}

// collectTags filters dir entries to visible tag names.
func collectTags(entries []fs.DirEntry) []string {
	out := []string{}
	for _, e := range entries {
		name, ok := isTagVisible(e)
		if !ok {
			continue
		}
		out = append(out, name)
	}
	return out
}

// ListTags returns the sorted tag names bound under a repo.
// Crash temp files and dotfiles are skipped.
func (s *Store) ListTags(repo string) []string {
	out := []string{}
	parts, ok := checkRepoParts(repo)
	if !ok {
		return out
	}
	elems := append([]string{s.dir, "tags"}, parts...)
	entries, err := os.ReadDir(filepath.Join(elems...))
	if err != nil {
		return out
	}
	out = collectTags(entries)
	sort.Strings(out)
	return out
}
