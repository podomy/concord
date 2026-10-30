// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"

	"github.com/podomy/concord/internal/blob"
)

// BlobRepo is the registry repository under which chunked blob
// manifests are published as tags. Chunks and manifest JSON travel as
// plain blobs under the same repo path, which the registry serves from
// its repo-independent blob namespace: GET /v2/concord-blobs/blobs/D
// serves D no matter which repository originally pushed it. The syncer
// relies on that property so one canonical repo prefix reaches every
// blob on the peer.
//
// Contract notes, docs only. File artifacts live under the separate
// fileart repo concord-files with a monolithic OCI wire for compat;
// this sync repo holds chunked manifests. peersync blobSyncRepo
// aliases this constant so the sync prefix has one definition.
// Canonical sync forms are strict: digests are lowercase sha256 plus
// 64 hex (see ValidBlobDigest, no case folding), repos match the
// registry.validRepo shape (lowercase segments, slash separated), sync tags
// are 64 lowercase hex (blob root or content hex, which fileart hex
// tags satisfy). Image repos keep OCI case rules and bypass sync
// validation; transport repo and tag checks apply to sync only.
const BlobRepo = "concord-blobs"

// BlobManifest is one content-addressed blob as an ordered chunk list.
// It aliases the blob package contract so the chunked wire format has
// exactly one definition: version, chunkSize, totalSize, chunks, root,
// where root is the sha256 over the joined chunk hex digests. OCI
// image manifests (fileart concord-files artifacts) are a separate
// compat shape, not this type.
type BlobManifest = blob.Manifest

// MaxManifestJSON bounds one chunked sync manifest JSON reply. The or
// registry keeps a 10MB wire cap for OCI image manifest compat while
// sync enforces 1MB here for chunked manifests, and peersync keeps a
// matching 1MB store readback cap. The wire stays monolithic OCI for
// compat; chunking lives in the store and sync layer. Raw blob fetches
// use a separate 32GB disk cap and stream, so the cap below bounds
// disk, never RAM. Raw reads held fully in memory use MaxRawGet
// (32MB, matching blob.Store GetRaw) instead.
const MaxManifestJSON = 1 << 20

// MaxRawGet bounds one raw blob held fully in memory (32MB, matching
// blob.Store GetRaw and maxRawBytes). Larger payloads must use
// chunked PutBlob plus streaming OpenBlob, not GetRaw, so memory
// stays flat. Wire fetches stream under maxBlobFetchBytes instead.
const MaxRawGet = 32 << 20

// Caps bound one malicious or corrupt peer. Manifests stay small by
// construction; raw fetches ride streams so the cap bounds disk, never
// RAM, and chunk fetches additionally enforce the blob chunk size at
// the sync layer. maxBlobManifestJSON aliases MaxManifestJSON so the
// sync manifest cap has one name; maxBlobFetchBytes matches the
// peersync raw fetch cap and the or registry blob cap (32GB disk
// streaming); MaxRawGet above matches the blob store memory cap
// (32MB) for callers that must hold bytes.
const (
	maxBlobManifestJSON = MaxManifestJSON
	maxBlobFetchBytes   = 32 << 30
)

// ValidBlobDigest reports whether s is a canonical blob digest:
// sha256 followed by 64 lowercase hex digits, strict, with no case
// folding. Every entry point validates before the digest reaches a
// URL path or a store key, so traversal payloads never become file
// or key material. Callers use this directly: FetchBlob and
// ParseBlobManifest here, plus peersync cleanDigests and
// WantedFromSpecs before Ensure. FetchTags accepts either hex case
// in a listing but normalizes to lowercase after validation.
func ValidBlobDigest(s string) bool {
	hexPart, ok := strings.CutPrefix(s, "sha256:")
	if !ok {
		return false
	}
	if len(hexPart) != 64 {
		return false
	}
	for _, c := range hexPart {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

// PeerRegistryBase builds the base URL for a peer's blob server from its
// underlay IP and the registry port. It returns the bare v2 root
// (http://host:port/v2); callers append the repo path per call
// (base + "/" + repo + ...), e.g. bare http://192.168.1.5:8444/v2
// plus concord-blobs for sync. That differs from fileart NewClient,
// whose base already includes its repo
// (http://hostport/v2/concord-files), and from cr manifestBaseURL
// which returns /v2/<repo> for an image repo. The three bases must
// not mix. Callers must pass the member's Address.Addr with the
// registry port (registry.Port, 8444), never the member's gossip port:
// member.Address carries the memberlist port, and dialing it speaks
// the wrong protocol. This constructor is where the prior anomaly
// (sync pointed at the gossip port) stays fixed. Image repos keep
// OCI case rules and bypass sync repo validation by design;
// validateBlobRepo below guards sync fetches for concord-blobs and
// concord-files only.
func PeerRegistryBase(addr netip.Addr, port int) string {
	if !addr.IsValid() {
		return ""
	}
	if port <= 0 || port > 65535 {
		return ""
	}
	hostport := net.JoinHostPort(addr.String(), strconv.Itoa(port))
	return "http://" + hostport + "/v2"
}

// BlobClient fetches manifests and blobs from a peer registry over plain
// HTTP. The mesh is trusted like the prior sync, and every byte is hash
// verified against its digest, so a tampering peer fails the fetch
// instead of poisoning the store. No request timeout: the caller's
// context bounds the wait.
type BlobClient struct {
	http *http.Client
}

// NewBlobClient creates a blob client with a default HTTP client.
func NewBlobClient() *BlobClient {
	return &BlobClient{http: &http.Client{}}
}

// FetchManifestTag returns the raw manifest JSON stored under tag in
// repo, plus the sha256 digest of those exact bytes. Tags in the sync
// repo are blob hexes: hex(root) for chunked roots, hex(content) for
// converted raw blobs. The response is capped so an oversized peer
// reply fails instead of filling memory.
func (c *BlobClient) FetchManifestTag(ctx context.Context, base, repo, tag string) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", fmt.Errorf("context cancelled: %w", err)
	}
	if err := validateBlobRepo(repo); err != nil {
		return nil, "", err
	}
	if err := validateBlobTag(tag); err != nil {
		return nil, "", err
	}
	raw, err := c.getCapped(ctx, base+"/"+repo+"/manifests/"+tag, "application/json", maxBlobManifestJSON)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(raw)
	return raw, "sha256:" + hex.EncodeToString(sum[:]), nil
}

// FetchBlob streams the blob at digest from repo into w and verifies
// the sha256 matches digest. maxBytes caps the transfer; chunk fetches
// pass just over the blob chunk size, raw conversions pass the total
// size cap. A mismatch fails the fetch and the caller discards its
// partial output.
func (c *BlobClient) FetchBlob(ctx context.Context, base, repo, digest string, w io.Writer, maxBytes int64) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("context cancelled: %w", err)
	}
	if err := validateBlobRepo(repo); err != nil {
		return 0, err
	}
	if !ValidBlobDigest(digest) {
		return 0, fmt.Errorf("invalid blob digest %q", digest)
	}
	if maxBytes <= 0 {
		return 0, fmt.Errorf("invalid fetch cap %d", maxBytes)
	}
	url := base + "/" + repo + "/blobs/" + digest
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("get blob: %w", err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck // best-effort response close
	}()
	if resp.StatusCode != http.StatusOK {
		msg, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if err != nil {
			return 0, fmt.Errorf("blob %s: status %d", digest, resp.StatusCode)
		}
		return 0, fmt.Errorf("blob %s: status %d: %s", digest, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	hasher := sha256.New()
	n, err := io.Copy(w, io.TeeReader(io.LimitReader(resp.Body, maxBytes+1), hasher))
	if err != nil {
		return 0, fmt.Errorf("stream blob: %w", err)
	}
	if n > maxBytes {
		return 0, fmt.Errorf("blob %s exceeds max %d bytes", digest, maxBytes)
	}
	sum := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if sum != digest {
		return 0, fmt.Errorf("blob digest mismatch: want %s", digest)
	}
	return n, nil
}

// FetchTags lists the tags published under repo on the peer. The sync
// repo holds one tag per replicated root (chunked or converted), so the
// tag list is the peer's publish index for anti-entropy. Entries that
// are not blob hexes are skipped: the repo is syncer owned, but a
// foreign writer must never inject fetch work.
func (c *BlobClient) FetchTags(ctx context.Context, base, repo string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("context cancelled: %w", err)
	}
	if err := validateBlobRepo(repo); err != nil {
		return nil, err
	}
	raw, err := c.getCapped(ctx, base+"/"+repo+"/tags/list", "application/json", maxBlobManifestJSON)
	if err != nil {
		return nil, err
	}
	var listed tagsListResponse
	err = json.Unmarshal(raw, &listed)
	if err != nil {
		return nil, fmt.Errorf("decode tags list: %w", err)
	}
	var out []string
	for _, tag := range listed.Tags {
		if validateBlobTag(tag) != nil {
			continue
		}
		out = append(out, strings.ToLower(tag))
	}
	return out, nil
}

// ParseBlobManifest validates manifest JSON against the blob contract:
// version 1, the fixed chunk size, well formed chunk digests,
// empty/non-empty consistency, and root binding (root must equal the
// hash over the joined chunk hexes). It does not compare Root to any
// tag: the caller does that, since alias tags carry manifests whose
// root differs by construction.
func ParseBlobManifest(raw []byte) (BlobManifest, error) {
	var m BlobManifest
	err := json.Unmarshal(raw, &m)
	if err != nil {
		return BlobManifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	if err := checkManifestFixed(m); err != nil {
		return BlobManifest{}, err
	}
	if err := checkManifestSizes(m); err != nil {
		return BlobManifest{}, err
	}
	if err := checkManifestChunks(m.Chunks); err != nil {
		return BlobManifest{}, err
	}
	if err := checkManifestRoot(m); err != nil {
		return BlobManifest{}, err
	}
	return m, nil
}

// checkManifestFixed validates version, chunk size, and total size sign.
func checkManifestFixed(m BlobManifest) error {
	if m.Version != 1 {
		return fmt.Errorf("unsupported manifest version %d", m.Version)
	}
	if m.ChunkSize != blob.ChunkSize {
		return fmt.Errorf("bad manifest chunk size %d", m.ChunkSize)
	}
	if m.TotalSize < 0 {
		return fmt.Errorf("negative manifest size %d", m.TotalSize)
	}
	return nil
}

// checkManifestSizes validates empty versus non-empty shape and the chunk
// count cap.
func checkManifestSizes(m BlobManifest) error {
	if m.TotalSize == 0 && len(m.Chunks) != 0 {
		return fmt.Errorf("empty manifest must have no chunks") //nolint:perfsprint // plain error, no wrap target
	}
	if m.TotalSize > 0 && len(m.Chunks) == 0 {
		return fmt.Errorf("non-empty manifest must have chunks") //nolint:perfsprint // plain error, no wrap target
	}
	if int64(len(m.Chunks)) > maxBlobFetchBytes/int64(blob.ChunkSize)+1 {
		return fmt.Errorf("manifest lists %d chunks, over cap", len(m.Chunks))
	}
	return nil
}

// checkManifestChunks validates every chunk digest shape.
func checkManifestChunks(chunks []string) error {
	for _, chunk := range chunks {
		if !ValidBlobDigest(chunk) {
			return fmt.Errorf("manifest lists invalid chunk %q", chunk)
		}
	}
	return nil
}

// checkManifestRoot validates root binding and root shape.
func checkManifestRoot(m BlobManifest) error {
	root, err := BlobRoot(m.Chunks)
	if err != nil {
		return err
	}
	if root != m.Root {
		return fmt.Errorf("manifest root verification failed") //nolint:perfsprint // plain error, no wrap target
	}
	if !ValidBlobDigest(m.Root) {
		return fmt.Errorf("invalid manifest root %q", m.Root)
	}
	return nil
}

// ManifestBytes marshals m canonically and returns the bytes plus their
// sha256 digest. Canonical struct encoding keeps every publisher's
// bytes identical, so the digest dedups across nodes.
func ManifestBytes(m BlobManifest) ([]byte, string, error) {
	raw, err := json.Marshal(m) //nolint:errchkjson // struct encoding never fails, checked for symmetry
	if err != nil {
		return nil, "", fmt.Errorf("marshal manifest: %w", err)
	}
	sum := sha256.Sum256(raw)
	return raw, "sha256:" + hex.EncodeToString(sum[:]), nil
}

// BlobRoot recomputes the root digest over chunk digests: sha256 over
// the joined chunk hex strings, matching the blob package binding.
func BlobRoot(chunks []string) (string, error) {
	hexes := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		hexPart, ok := strings.CutPrefix(chunk, "sha256:")
		if !ok || len(hexPart) != 64 {
			return "", fmt.Errorf("invalid chunk digest %q", chunk)
		}
		hexes = append(hexes, hexPart)
	}
	sum := sha256.Sum256([]byte(strings.Join(hexes, "")))
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// getCapped performs one GET with an Accept header and returns at most
// maxBytes+1 bytes so the caller can tell truncation apart from a full body.
func (c *BlobClient) getCapped(ctx context.Context, url, accept string, maxBytes int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", accept)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", url, err)
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck // best-effort response close
	}()
	if resp.StatusCode != http.StatusOK {
		msg, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if err != nil {
			return nil, fmt.Errorf("get %s: status %d", url, resp.StatusCode)
		}
		return nil, fmt.Errorf("get %s: status %d: %s", url, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if int64(len(raw)) > maxBytes {
		return nil, fmt.Errorf("response from %s exceeds max %d bytes", url, maxBytes)
	}
	return raw, nil
}

// validateBlobRepo rejects empty, absolute, or traversing repo names.
// Repos are registry namespaces, never filesystem paths. The accepted
// shape matches the registry.validRepo rule for sync use: lowercase
// alphanumerics plus dot, underscore, hyphen per segment, slash
// separated. Image repos keep wider OCI rules outside sync; this
// check guards sync fetch paths only.
func validateBlobRepo(repo string) error {
	if repo == "" {
		return fmt.Errorf("empty blob repo") //nolint:perfsprint // plain error, no wrap target
	}
	if strings.Contains(repo, "..") {
		return fmt.Errorf("invalid blob repo %q", repo)
	}
	if strings.HasPrefix(repo, "/") || strings.HasSuffix(repo, "/") {
		return fmt.Errorf("invalid blob repo %q", repo)
	}
	for _, c := range repo {
		if !strings.ContainsRune("abcdefghijklmnopqrstuvwxyz0123456789._-/", c) {
			return fmt.Errorf("invalid blob repo %q", repo)
		}
	}
	return nil
}

// validateBlobTag accepts 64 hex characters, either case for
// robustness. The canonical sync tag is 64 lowercase hex: the blob
// root or content hex, which fileart manifest tags satisfy because
// they derive from lowercase blob hex. FetchTags normalizes accepted
// tags to lowercase after this check.
func validateBlobTag(tag string) error {
	if len(tag) != 64 {
		return fmt.Errorf("invalid manifest tag %q", tag)
	}
	for _, c := range tag {
		lower := c >= 'a' && c <= 'f'
		upper := c >= 'A' && c <= 'F'
		digit := c >= '0' && c <= '9'
		if !lower && !upper && !digit {
			return fmt.Errorf("invalid manifest tag %q", tag)
		}
	}
	return nil
}

// tagsListResponse decodes the registry tag listing envelope.
type tagsListResponse struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}
