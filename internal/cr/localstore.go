// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/podomy/concord/internal/blob"
)

// localConcordSubdir and localRegistrySubdir are the config subpath for the
// embedded registry data dir. They must stay in sync with
// registry.rootDirPath which builds <config>/concord/zot (historic name kept
// for data compat). The prefix is
// duplicated here because cr does not import registry for one helper; the
// leaf dir name comes from blob.DefaultDirName so a store rename is
// picked up.
const (
	localConcordSubdir  = "concord"
	localRegistrySubdir = "zot"
)

// localBlobStore is the minimal read path a content-addressed blob
// store must provide for the pull fast path. Blobs are keyed by
// digest, so a local hit is byte-identical to the registry copy no
// matter which registry named it. Manifests stay HTTP-only because
// tags are mutable and only the registry can resolve them.
//
// It overlaps blob.Store (HasRaw, GetRaw) and the peersync blobIface
// (HasRaw, GetRaw plus write paths) but stays narrower on purpose:
// the fast path reads raw chunk files directly instead of opening a
// full store, so pulls skip the manifest layer for speed. Every read
// is re-hashed against the digest by the caller, so on-disk layout
// drift can only cause misses and an HTTP fallback, never bad bytes.
type localBlobStore interface {
	Has(digest string) bool
	Open(digest string) (io.ReadCloser, error)
}

// rawBlobDir serves raw blobs straight from this node's embedded
// registry store: <root>/blobs/chunks/<2>/<rest>, sharded by the
// first hex byte pair. It mirrors the raw file layout the store
// writes for registry uploads, but keeps no dependency on it: the
// digest is re-verified on every read, so layout drift can only
// cause misses (and an HTTP fallback), never bad bytes.
type rawBlobDir struct {
	root string
}

// Has reports whether digest names a raw blob file on disk.
func (s rawBlobDir) Has(digest string) bool {
	path, err := s.blobPath(digest)
	if err != nil {
		return false
	}
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return !info.IsDir()
}

// Open streams the raw blob bytes for digest. The caller closes it.
func (s rawBlobDir) Open(digest string) (io.ReadCloser, error) {
	path, err := s.blobPath(digest)
	if err != nil {
		return nil, fmt.Errorf("resolve local blob: %w", err)
	}
	f, err := os.Open(path) //nolint:gosec // path is built from a validated digest under the store root
	if err != nil {
		return nil, fmt.Errorf("open local blob: %w", err)
	}
	return f, nil
}

// blobPath maps digest to its raw chunk file, rejecting anything
// that is not a canonical lowercase sha256 digest so crafted
// references cannot escape the store root. Canonical form is sha256
// plus 64 lowercase hex, matching blob.Store and
// transport.ValidBlobDigest; uppercase input is rejected instead of
// normalized. The filename is re-encoded from decoded bytes, never
// from the raw string, so separators and case tricks cannot smuggle
// in a path.
func (s rawBlobDir) blobPath(digest string) (string, error) {
	hexPart, ok := strings.CutPrefix(digest, "sha256:")
	if !ok {
		return "", fmt.Errorf("unsupported digest algorithm in %q", digest)
	}
	if len(hexPart) != 64 {
		return "", fmt.Errorf("digest length mismatch for %q", digest)
	}
	for i := range len(hexPart) {
		c := hexPart[i]
		ok := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
		if !ok {
			return "", fmt.Errorf("invalid digest %q", digest)
		}
	}
	raw, err := hex.DecodeString(hexPart)
	if err != nil {
		return "", fmt.Errorf("invalid digest %q: %w", digest, err)
	}
	if len(raw) != sha256.Size {
		return "", fmt.Errorf("digest length mismatch for %q", digest)
	}
	name := hex.EncodeToString(raw)
	return filepath.Join(s.root, "chunks", name[:2], name[2:]), nil
}

// tryLocalStore returns a store reading this node's registry data
// dir, or false when the dir is absent. Raw blobs are addressed by
// digest alone, so one store covers every repository. A miss is
// normal (blob never pulled here) and the caller falls back to HTTP,
// so the wire protocol is unchanged. The canonical path uses
// blob.DefaultDirName; the literal legacy path is probed second so a
// store rename drifts to a miss, never to bad bytes.
func tryLocalStore() (localBlobStore, bool) {
	configDir, err := os.UserConfigDir()
	if err != nil {
		return nil, false
	}
	newDir := filepath.Join(configDir, localConcordSubdir, localRegistrySubdir, blob.DefaultDirName, "chunks")
	info, err := os.Stat(newDir)
	if err == nil && info.IsDir() {
		return rawBlobDir{root: filepath.Dir(newDir)}, true
	}
	oldDir := filepath.Join(configDir, "concord", "zot", "blobs", "chunks")
	if oldDir != newDir {
		info, err = os.Stat(oldDir)
		if err == nil && info.IsDir() {
			return rawBlobDir{root: filepath.Dir(oldDir)}, true
		}
	}
	return nil, false
}

// readLocalConfig serves an image config from the local store,
// enforcing the same size cap and digest check as the HTTP path.
func readLocalConfig(ctx context.Context, store localBlobStore, digest string) (*ConfigFile, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled: %w", err)
	}
	rc, err := store.Open(digest)
	if err != nil {
		return nil, fmt.Errorf("open local config %q: %w", digest, err)
	}
	defer func() {
		_ = rc.Close() //nolint:errcheck // best-effort read-only close
	}()

	body, err := io.ReadAll(io.LimitReader(rc, maxConfigBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read local config: %w", err)
	}
	if int64(len(body)) > maxConfigBytes {
		return nil, fmt.Errorf("read local config: exceeds %d bytes", maxConfigBytes)
	}
	if err := verifyDigest(body, digest); err != nil {
		return nil, err
	}
	var cfg ConfigFile
	if err := json.Unmarshal(body, &cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	return &cfg, nil
}

// extractLocalLayer streams one layer blob from the local store and
// extracts it, with the same digest verification as the HTTP path.
func extractLocalLayer(ctx context.Context, store localBlobStore, digest, rootfs string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}
	rc, err := store.Open(digest)
	if err != nil {
		return fmt.Errorf("open local blob %q: %w", digest, err)
	}
	defer func() {
		_ = rc.Close() //nolint:errcheck // best-effort read-only close
	}()

	if err := extractBlobStream(ctx, rc, digest, rootfs); err != nil {
		return err
	}
	return nil
}
