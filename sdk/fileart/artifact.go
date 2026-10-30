// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package fileart pushes and pulls file artifacts through any OCI
// distribution registry, including Concord's embedded registry. Every file
// a workload needs persisted becomes one artifact: the exact bytes as
// a blob plus a manifest referencing it, under the concord-files repo.
// The registry syncs tagged repos peer to peer, so artifacts replicate
// like images; the journal carries only digests and stays small.
//
// This package is stdlib-only on purpose: SDK users, the CLI, and the
// daemon share this one implementation instead of three copies.
//
// Wire stays monolithic OCI for compat: HEAD blob exists, POST upload
// with digest, PUT manifest, GET blob. Fileart does no chunking; the
// store and sync layer chunk transparently so every peer sees identical
// requests. Fileart never reads the registry data dir directly (the sdk
// module cannot import internal stores). Pulls stream with flat memory
// into a temp file beside dest and rename only after the digest
// verifies, so an interrupted pull leaves nothing behind and a retry
// converges on identical bytes.
package fileart

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// Repo is the registry repository holding file artifacts.
const Repo = "concord-files"

// LayerMediaType marks a file-content blob. The bytes are stored and
// returned exactly as pushed: no compression, no transformation.
const LayerMediaType = "application/vnd.concord.file.v1+binary"

// ConfigMediaType marks the empty config every file manifest carries.
// Manifests need a config descriptor; files have no config, so it is
// always two bytes: {}.
const ConfigMediaType = "application/vnd.oci.image.config.v1+json"

// ManifestMediaType is the standard OCI image manifest type. File
// manifests are schema-valid image manifests with one custom layer;
// registries store them without understanding them.
const ManifestMediaType = "application/vnd.oci.image.manifest.v1+json"

// DefaultMode is the file mode for materialized files without an
// explicit mode: owner read-write, nothing else.
const DefaultMode = 0o600

// File is one materialized file: container path, blob digest, mode.
// Mode 0 means DefaultMode.
type File struct {
	Path   string
	Digest string
	Mode   uint32
}

// Client speaks the OCI distribution subset file artifacts need:
// blob existence, monolithic blob upload, manifest put, blob get.
// No request timeout: large blobs over slow links take as long as
// they take, and the caller's context bounds the wait instead.
// Every operation goes over HTTP with no local shortcut, so pulls
// and pushes replicate through the registry like any other peer.
type Client struct {
	base string
	http *http.Client
}

// NewClient creates a client for a registry at hostport, e.g.
// "127.0.0.1:8444". Plain HTTP: registries here are loopback-local.
func NewClient(hostport string) *Client {
	return &Client{
		base: "http://" + hostport + "/v2/" + Repo,
		http: &http.Client{},
	}
}

// descriptor is one content descriptor inside a manifest.
type descriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
}

// manifest is the OCI image manifest pushed per file.
type manifest struct {
	SchemaVersion int          `json:"schemaVersion"`
	MediaType     string       `json:"mediaType"`
	Config        descriptor   `json:"config"`
	Layers        []descriptor `json:"layers"`
}

// digestOf returns the sha256 digest string for bytes.
func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Push stores data as a file artifact and returns its blob digest. Blobs
// and manifests already present are skipped by existence check, so
// re-pushing shared content costs two HEADs and no bytes: dedup falls
// out of content addressing. The manifest is tagged with the blob hex
// so registry sync (which replicates tagged repos) picks it up.
func (c *Client) Push(ctx context.Context, data []byte) (string, error) {
	blobDigest := digestOf(data)
	exists, err := c.blobExists(ctx, blobDigest)
	if err != nil {
		return "", err
	}
	if !exists {
		if err := c.uploadBlob(ctx, blobDigest, data); err != nil {
			return "", err
		}
	}

	config := []byte("{}")
	configDigest := digestOf(config)
	exists, err = c.blobExists(ctx, configDigest)
	if err != nil {
		return "", err
	}
	if !exists {
		if err := c.uploadBlob(ctx, configDigest, config); err != nil {
			return "", err
		}
	}

	doc := manifest{
		SchemaVersion: 2,
		MediaType:     ManifestMediaType,
		Config:        descriptor{MediaType: ConfigMediaType, Digest: configDigest, Size: int64(len(config))},
		Layers:        []descriptor{{MediaType: LayerMediaType, Digest: blobDigest, Size: int64(len(data))}},
	}
	raw, err := json.Marshal(doc) //nolint:errchkjson // struct encoding never fails, checked for symmetry
	if err != nil {
		return "", fmt.Errorf("marshal manifest: %w", err)
	}
	tag := strings.TrimPrefix(blobDigest, "sha256:")
	if err := c.put(ctx, "/manifests/"+tag, ManifestMediaType, raw); err != nil {
		return "", err
	}
	return blobDigest, nil
}

// PullTo streams a blob to dest, verifying the hash before the rename.
// Memory stays flat no matter the blob size: bytes flow from the socket
// through the hasher into a temp file, and only a verified complete file
// ever appears at dest. A mismatch deletes the temp file and fails;
// the temp file lives beside dest so the rename never crosses devices.
// Cancelling the context aborts the copy and removes the temp file,
// so a later retry starts clean and converges on identical bytes.
func (c *Client) PullTo(ctx context.Context, digest, dest string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/blobs/"+digest, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("get blob: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck // best-effort body drain
		_ = resp.Body.Close()                 //nolint:errcheck // best-effort response close
	}()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if err != nil {
			return fmt.Errorf("read error body: %w", err)
		}
		return fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
	}

	tmp, err := os.CreateTemp(filepath.Dir(dest), ".pull-*.part")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpName := tmp.Name()
	succeeded := false
	defer func() {
		if !succeeded {
			_ = os.Remove(tmpName) //nolint:errcheck // best-effort temp cleanup
		}
	}()

	hasher := sha256.New()
	if _, err := io.Copy(tmp, io.TeeReader(resp.Body, hasher)); err != nil {
		_ = tmp.Close() //nolint:errcheck // close error folded into copy error
		return fmt.Errorf("stream blob: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close() //nolint:errcheck // close error folded into sync error
		return fmt.Errorf("sync blob: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close blob: %w", err)
	}
	if got := "sha256:" + hex.EncodeToString(hasher.Sum(nil)); got != digest {
		return fmt.Errorf("blob digest mismatch for %s", digest)
	}
	if err := os.Rename(tmpName, dest); err != nil {
		return fmt.Errorf("rename blob: %w", err)
	}
	succeeded = true
	return nil
}

// Materialize writes files into rootfs, creating parents as needed.
// Paths and modes validate up front, before any fetch, so a bad third
// file never pays for pulling the first two. A failed fetch aborts the
// start and the next tick retries idempotently: same digests rewrite
// identical bytes, so a partial rootfs converges instead of corrupting.
// Modes of 0 become DefaultMode; modes above 0777 fail.
func Materialize(ctx context.Context, rootfs string, files []File, fetch func(ctx context.Context, digest, dest string) error) error {
	dests, err := resolveDests(rootfs, files)
	if err != nil {
		return err
	}
	err = fetchAll(ctx, files, dests, fetch)
	if err != nil {
		return err
	}
	return nil
}

// resolveDests validates every path and mode, then creates parent dirs.
// It returns the dest per file in input order.
func resolveDests(rootfs string, files []File) ([]string, error) {
	dests := make([]string, len(files))
	for i, file := range files {
		err := checkPath(file.Path)
		if err != nil {
			return nil, err
		}
		_, err = validatedMode(file)
		if err != nil {
			return nil, err
		}
		dests[i] = filepath.Join(rootfs, filepath.Clean("/"+file.Path))
		err = os.MkdirAll(filepath.Dir(dests[i]), 0o750)
		if err != nil {
			return nil, fmt.Errorf("create parent dirs: %w", err)
		}
	}
	return dests, nil
}

// fetchAll pulls each digest into its dest and applies the file mode.
func fetchAll(ctx context.Context, files []File, dests []string, fetch func(ctx context.Context, digest, dest string) error) error {
	for i, file := range files {
		err := fetch(ctx, file.Digest, dests[i])
		if err != nil {
			return fmt.Errorf("fetch %s: %w", file.Digest, err)
		}
		mode, err := validatedMode(file)
		if err != nil {
			return err
		}
		err = os.Chmod(dests[i], os.FileMode(mode))
		if err != nil {
			return fmt.Errorf("chmod file: %w", err)
		}
	}
	return nil
}

// validatedMode returns the effective mode, defaulting zero to DefaultMode.
func validatedMode(file File) (uint32, error) {
	if file.Mode == 0 {
		return DefaultMode, nil
	}
	if file.Mode > 0o777 {
		return 0, fmt.Errorf("file mode %o exceeds 0777: %s", file.Mode, file.Path)
	}
	return file.Mode, nil
}

// checkPath rejects anything but a clean absolute path without parent
// escapes. A relative path could land anywhere under rootfs; a ".."
// could climb out of it.
func checkPath(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("file path must be absolute: %q", path)
	}
	clean := filepath.Clean(path)
	if clean != path {
		return fmt.Errorf("file path must be clean: %q", path)
	}
	if slices.Contains(strings.Split(clean, string(filepath.Separator)), "..") {
		return fmt.Errorf("file path must not escape: %q", path)
	}
	return nil
}

// blobExists reports whether a blob is already stored.
func (c *Client) blobExists(ctx context.Context, digest string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.base+"/blobs/"+digest, nil)
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return false, fmt.Errorf("check blob: %w", err)
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck // best-effort body drain
		_ = resp.Body.Close()                 //nolint:errcheck // best-effort response close
	}()

	if resp.StatusCode == http.StatusOK {
		return true, nil
	}
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	return false, fmt.Errorf("check blob (%d)", resp.StatusCode)
}

// uploadBlob stores one blob with a monolithic POST carrying the digest.
func (c *Client) uploadBlob(ctx context.Context, digest string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/blobs/uploads/?digest="+digest, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("upload blob: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upload blob (%d): %s", resp.StatusCode, string(body))
	}
	return nil
}

// put stores a manifest under a tag.
func (c *Client) put(ctx context.Context, path, contentType string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.base+path, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("put manifest: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("put manifest (%d): %s", resp.StatusCode, string(body))
	}
	return nil
}
