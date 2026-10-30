// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
)

// ImageConfig holds the execution defaults from an OCI image config.
// Only the fields the reconciler consumes are modelled.
type ImageConfig struct {
	Entrypoint []string `json:"Entrypoint,omitempty"`
	Cmd        []string `json:"Cmd,omitempty"`
	Env        []string `json:"Env,omitempty"`
	WorkingDir string   `json:"WorkingDir,omitempty"`
	User       string   `json:"User,omitempty"`
}

// ConfigFile holds the image configuration for use by the bundle builder.
// It mirrors the OCI image config JSON layout for the fields we need.
type ConfigFile struct {
	Architecture string      `json:"architecture,omitempty"`
	OS           string      `json:"os,omitempty"`
	Config       ImageConfig `json:"config"`
}

// imageReference is a parsed OCI reference.
type imageReference struct {
	registry   string
	repository string
	reference  string
	isDigest   bool
}

// Manifest media types accepted from the registry.
const manifestAcceptHeader = "application/vnd.oci.image.index.v1+json, " +
	"application/vnd.oci.image.manifest.v1+json, " +
	"application/vnd.docker.distribution.manifest.v2+json, " +
	"application/vnd.docker.distribution.manifest.list.v2+json"

// Response size limits guard config and manifest parsing. The 10MB wire
// cap keeps OCI compat for image configs and manifests pulled from any
// registry. Chunked sync manifests use a separate 1MB cap in transport
// (MaxManifestJSON) and peersync. Raw blob fetches stream under a 32GB
// disk cap and never sit fully in RAM; blob.Store GetRaw holds at most
// 32MB (see transport MaxRawGet), so large raws must stream via
// PutBlob and OpenBlob instead of GetRaw.
const (
	maxManifestBytes = 10 << 20
	maxConfigBytes   = 10 << 20
)

// ociDescriptor is one content descriptor in a manifest.
type ociDescriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
}

// ociPlatform selects one entry out of an image index.
type ociPlatform struct {
	OS           string `json:"os,omitempty"`
	Architecture string `json:"architecture,omitempty"`
}

// ociIndexEntry is one child manifest inside an image index.
type ociIndexEntry struct {
	MediaType string       `json:"mediaType"`
	Digest    string       `json:"digest"`
	Platform  *ociPlatform `json:"platform,omitempty"`
}

// ociManifest covers both image manifests and image indexes. Only the
// fields this client needs are modelled.
type ociManifest struct {
	MediaType     string          `json:"mediaType"`
	SchemaVersion int             `json:"schemaVersion,omitempty"`
	Config        ociDescriptor   `json:"config"`
	Layers        []ociDescriptor `json:"layers,omitempty"`
	Manifests     []ociIndexEntry `json:"manifests,omitempty"`
}

// parseImageReference parses s as a tag or digest reference. It follows
// the prior registry client rules: a missing tag defaults to latest,
// a missing registry defaults to index.docker.io, docker.io rewrites to
// index.docker.io, and a bare repository on the default registry gains
// the library namespace.
func parseImageReference(s string) (imageReference, error) {
	if strings.TrimSpace(s) == "" {
		return imageReference{}, fmt.Errorf("empty image reference") //nolint:perfsprint // plain error, no wrap target
	}

	base, ref, isDigest, err := splitReference(s)
	if err != nil {
		return imageReference{}, err
	}
	if base == "" {
		return imageReference{}, fmt.Errorf("invalid reference %q: empty repository", s)
	}

	registry, repository := splitRegistry(base)
	if repository == "" {
		return imageReference{}, fmt.Errorf("invalid reference %q: empty repository", s)
	}

	return imageReference{registry: registry, repository: repository, reference: ref, isDigest: isDigest}, nil
}

// splitReference separates the tag or digest suffix from the base.
// A missing tag defaults to latest; a digest suffix must name an
// algorithm.
func splitReference(s string) (string, string, bool, error) {
	if at := strings.LastIndex(s, "@"); at >= 0 {
		ref := s[at+1:]
		if !strings.Contains(ref, ":") {
			return "", "", false, fmt.Errorf("invalid digest reference %q", s)
		}
		return s[:at], ref, true, nil
	}

	base := s
	ref := ""
	lastColon := strings.LastIndex(s, ":")
	if lastColon >= 0 && !strings.Contains(s[lastColon+1:], "/") {
		base = s[:lastColon]
		ref = s[lastColon+1:]
		if ref == "" {
			return "", "", false, fmt.Errorf("invalid reference %q: empty tag", s)
		}
	}
	if ref == "" {
		ref = "latest"
	}
	return base, ref, false, nil
}

// splitRegistry separates the registry host from the repository. A
// missing registry defaults to index.docker.io, docker.io rewrites to
// index.docker.io, and a bare repository on the default registry gains
// the library namespace.
func splitRegistry(base string) (string, string) {
	registry := ""
	repository := base
	if parts := strings.SplitN(base, "/", 2); len(parts) == 2 {
		first := parts[0]
		if first == "localhost" || strings.Contains(first, ".") || strings.Contains(first, ":") {
			registry = first
			repository = parts[1]
		}
	}
	if registry == "" {
		registry = "index.docker.io"
	}
	if registry == "docker.io" {
		registry = "index.docker.io"
	}
	if registry == "index.docker.io" && !strings.Contains(repository, "/") {
		repository = "library/" + repository
	}
	return registry, repository
}

// registryScheme returns http for loopback and private registries and
// https otherwise. It mirrors the previous client's scheme selection so
// localhost registry uses plain http while Docker Hub uses https.
func registryScheme(registry string) string {
	host := registry
	if i := strings.LastIndex(host, ":"); i >= 0 {
		maybePort := host[i+1:]
		isPort := true
		for _, c := range maybePort {
			if c < '0' || c > '9' {
				isPort = false
				break
			}
		}
		if isPort {
			host = host[:i]
		}
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return "http"
	}
	if strings.HasPrefix(host, "127.") {
		return "http"
	}
	if host == "::1" || host == "[::1]" {
		return "http"
	}
	if isPrivateIPv4(host) {
		return "http"
	}
	return "https"
}

// isPrivateIPv4 reports whether host is an RFC1918 IPv4 address.
func isPrivateIPv4(host string) bool {
	var a, b, c, d int
	n, err := fmt.Sscanf(host, "%d.%d.%d.%d", &a, &b, &c, &d)
	if err != nil {
		return false
	}
	if n != 4 {
		return false
	}
	if a == 10 {
		return true
	}
	if a == 172 && b >= 16 && b <= 31 {
		return true
	}
	if a == 192 && b == 168 {
		return true
	}
	return false
}

// manifestBaseURL returns the /v2/<repo> prefix for a reference, e.g.
// http://127.0.0.1:8444/v2/library/ubuntu for repo library/ubuntu.
// The base already includes the repo, unlike transport
// PeerRegistryBase which returns the bare v2 root
// (http://host:8444/v2) with the repo added per call, and unlike
// fileart NewClient whose base pins /v2/concord-files. Image repos
// keep OCI case rules and bypass the strict sync repo validation in
// transport by design: this path serves any registry repo, while sync
// validation guards concord-blobs and concord-files only.
func manifestBaseURL(ref imageReference) string {
	scheme := registryScheme(ref.registry)
	return scheme + "://" + ref.registry + "/v2/" + ref.repository
}

// doGet issues a GET with ctx and returns the response. The caller closes it.
func doGet(ctx context.Context, client *http.Client, url, accept string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("registry get: %w", err)
	}

	return resp, nil
}

// readLimitedBody reads at most limit bytes and reports truncation.
func readLimitedBody(resp *http.Response, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("response exceeds %d bytes", limit)
	}
	return body, nil
}

// checkStatus returns nil for 200 and a wrapped error otherwise.
func checkStatus(resp *http.Response, what string) error {
	if resp.StatusCode == http.StatusOK {
		return nil
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("%s: registry requires authentication (status 401)", what) // status is context
	}
	return fmt.Errorf("%s: unexpected status %s", what, resp.Status) // status is context
}

// fetchImageManifest resolves ref to an image manifest, following an index
// to the entry matching the local platform. It returns the config digest
// and the ordered layer digests.
func fetchImageManifest(ctx context.Context, client *http.Client, ref imageReference) (string, []string, error) {
	base := manifestBaseURL(ref)
	url := base + "/manifests/" + ref.reference

	resp, err := doGet(ctx, client, url, manifestAcceptHeader)
	if err != nil {
		return "", nil, err
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck // read-only body close
	}()

	err = checkStatus(resp, "fetch manifest")
	if err != nil {
		return "", nil, err
	}

	body, err := readLimitedBody(resp, maxManifestBytes)
	if err != nil {
		return "", nil, fmt.Errorf("read manifest: %w", err)
	}

	var parsed ociManifest
	err = json.Unmarshal(body, &parsed)
	if err != nil {
		return "", nil, fmt.Errorf("decode manifest: %w", err)
	}

	return resolveManifest(ctx, client, ref, parsed, manifestMediaType(parsed, resp))
}

// manifestMediaType prefers the document field, falling back to the
// Content-Type header without parameters.
func manifestMediaType(parsed ociManifest, resp *http.Response) string {
	if parsed.MediaType != "" {
		return parsed.MediaType
	}
	mediaType := resp.Header.Get("Content-Type")
	if i := strings.Index(mediaType, ";"); i >= 0 {
		mediaType = strings.TrimSpace(mediaType[:i])
	}
	return mediaType
}

// resolveManifest follows an index to the local platform entry or
// returns the image config and layer digests of a manifest.
func resolveManifest(ctx context.Context, client *http.Client, ref imageReference, parsed ociManifest, mediaType string) (string, []string, error) {
	switch mediaType {
	case "application/vnd.oci.image.index.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json":
		return fetchChildManifest(ctx, client, ref, parsed)
	case "application/vnd.oci.image.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.v2+json":
		return imageLayers(parsed)
	default:
		if len(parsed.Manifests) > 0 {
			return fetchChildManifest(ctx, client, ref, parsed)
		}
		if parsed.Config.Digest != "" {
			layers := make([]string, 0, len(parsed.Layers))
			for _, layer := range parsed.Layers {
				layers = append(layers, layer.Digest)
			}
			return parsed.Config.Digest, layers, nil
		}
		return "", nil, fmt.Errorf("unsupported manifest media type %q", mediaType)
	}
}

// fetchChildManifest follows an index to the entry matching the local
// platform and resolves it.
func fetchChildManifest(ctx context.Context, client *http.Client, ref imageReference, parsed ociManifest) (string, []string, error) {
	child, err := pickIndexEntry(parsed.Manifests)
	if err != nil {
		return "", nil, err
	}
	childRef := imageReference{registry: ref.registry, repository: ref.repository, reference: child, isDigest: true}
	return fetchImageManifest(ctx, client, childRef)
}

// imageLayers returns the config digest and the ordered layer digests,
// requiring every descriptor to name a digest.
func imageLayers(parsed ociManifest) (string, []string, error) {
	if parsed.Config.Digest == "" {
		return "", nil, fmt.Errorf("manifest missing config digest") //nolint:perfsprint // plain error, no wrap target
	}
	layers := make([]string, 0, len(parsed.Layers))
	for _, layer := range parsed.Layers {
		if layer.Digest == "" {
			return "", nil, fmt.Errorf("manifest entry missing digest") //nolint:perfsprint // plain error, no wrap target
		}
		layers = append(layers, layer.Digest)
	}
	return parsed.Config.Digest, layers, nil
}

// pickIndexEntry selects the child digest matching the local platform,
// falling back to linux/amd64 and then to the first entry.
func pickIndexEntry(entries []ociIndexEntry) (string, error) {
	if len(entries) == 0 {
		return "", fmt.Errorf("image index has no manifests") //nolint:perfsprint // plain error, no wrap target
	}

	for _, entry := range entries {
		if entry.Platform != nil && entry.Platform.OS == runtime.GOOS && entry.Platform.Architecture == runtime.GOARCH {
			return entry.Digest, nil
		}
	}
	for _, entry := range entries {
		if entry.Platform != nil && entry.Platform.OS == "linux" && entry.Platform.Architecture == "amd64" {
			return entry.Digest, nil
		}
	}
	for _, entry := range entries {
		if entry.Platform == nil {
			return entry.Digest, nil
		}
	}
	return entries[0].Digest, nil
}

// fetchImageConfig downloads and parses the image config blob, verifying
// its sha256 digest. A blob already present in this node's blob store dir
// is read from disk instead: the digest addresses content, so a local
// hit is identical to the registry copy and skips the HTTP round trip.
// A local miss or corrupt file falls through to HTTP by design with no
// log, keeping cr logger-free; readLocalConfig re-hashes every byte so
// corruption reads as an error here and the HTTP path re-verifies,
// never bad bytes. Misses fall back to HTTP, leaving the wire protocol
// unchanged.
func fetchImageConfig(ctx context.Context, client *http.Client, ref imageReference, digest string) (*ConfigFile, error) {
	if store, ok := tryLocalStore(); ok {
		// Intentional miss-plus-HTTP fallback: any local error,
		// including digest mismatch, retries over HTTP where the
		// bytes are hashed again.
		cfg, err := readLocalConfig(ctx, store, digest)
		if err == nil {
			return cfg, nil
		}
	}

	url := manifestBaseURL(ref) + "/blobs/" + digest

	resp, err := doGet(ctx, client, url, "application/vnd.oci.image.config.v1+json, application/vnd.docker.container.image.v1+json")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck // read-only body close
	}()

	err = checkStatus(resp, "fetch config")
	if err != nil {
		return nil, err
	}

	body, err := readLimitedBody(resp, maxConfigBytes)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	err = verifyDigest(body, digest)
	if err != nil {
		return nil, err
	}

	var cfg ConfigFile
	err = json.Unmarshal(body, &cfg)
	if err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}

	return &cfg, nil
}

// isHexDigit reports whether c is a lowercase hex digit.
func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

// canonicalDigestHex validates digest as canonical lowercase sha256
// and returns the lowercase hex part. Canonical form matches
// blob.Store and transport.ValidBlobDigest; uppercase is rejected.
func canonicalDigestHex(digest string) (string, error) {
	hexPart, ok := strings.CutPrefix(digest, "sha256:")
	if !ok {
		return "", fmt.Errorf("unsupported digest algorithm in %q", digest)
	}
	lower := strings.ToLower(hexPart)
	if len(lower) != 64 {
		return "", fmt.Errorf("digest length mismatch for %q", digest)
	}
	for i := range len(lower) {
		if !isHexDigit(lower[i]) {
			return "", fmt.Errorf("invalid digest %q", digest)
		}
	}
	if hexPart != lower {
		return "", fmt.Errorf("non-canonical digest %q: want lowercase", digest)
	}
	return lower, nil
}

// verifyDigest checks body against a canonical lowercase sha256 digest.
// Canonical form is sha256 plus 64 lowercase hex, matching blob.Store
// and transport.ValidBlobDigest. Uppercase input is rejected instead of
// normalized so every layer agrees on one key.
func verifyDigest(body []byte, digest string) error {
	lower, err := canonicalDigestHex(digest)
	if err != nil {
		return err
	}
	sum := sha256.Sum256(body)
	want, err := hex.DecodeString(lower)
	if err != nil {
		return fmt.Errorf("invalid digest %q: %w", digest, err)
	}
	if len(want) != len(sum) {
		return fmt.Errorf("digest length mismatch for %q", digest)
	}
	for i := range sum {
		if sum[i] != want[i] {
			return fmt.Errorf("digest mismatch for %q", digest)
		}
	}
	return nil
}

// extractLayerBlob downloads one layer blob, verifies its digest while
// streaming, gunzips gzip layers, and extracts the tar into rootfs.
// Non-gzip layers pass through as plain tar. Zstd layers are not
// decompressed because the standard library has no zstd reader.
// A blob already on local disk streams from the registry data dir instead;
// a local miss or corrupt file falls through to HTTP by design with no
// log, keeping cr logger-free. Either way memory stays flat: bytes flow
// from the source through the hasher into the extractor, and both paths
// re-hash the stream so corruption fails the pull instead of poisoning
// the rootfs. The wire protocol is unchanged.
func extractLayerBlob(ctx context.Context, client *http.Client, ref imageReference, digest, rootfs string) error {
	if store, ok := tryLocalStore(); ok {
		// Intentional miss-plus-HTTP fallback: any local error,
		// including digest mismatch, retries over HTTP where the
		// bytes are hashed again.
		err := extractLocalLayer(ctx, store, digest, rootfs)
		if err == nil {
			return nil
		}
	}

	url := manifestBaseURL(ref) + "/blobs/" + digest

	resp, err := doGet(ctx, client, url, "")
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close() //nolint:errcheck // read-only body close
	}()

	err = checkStatus(resp, "fetch layer")
	if err != nil {
		return err
	}

	if err := extractBlobStream(ctx, resp.Body, digest, rootfs); err != nil {
		return err
	}

	return nil
}

// extractBlobStream verifies body against digest while streaming it
// through gunzip (when gzip) into the tar extractor at rootfs. The
// whole blob is consumed so the digest check covers every byte, and
// nothing larger than the tar window is ever buffered.
func extractBlobStream(ctx context.Context, body io.Reader, digest, rootfs string) error {
	sum := sha256.New()
	tee := io.TeeReader(body, sum)
	buf := bufio.NewReader(tee)

	if err := untarReader(ctx, buf, rootfs); err != nil {
		return err
	}

	_, err := io.Copy(io.Discard, buf)
	if err != nil {
		return fmt.Errorf("drain layer body: %w", err)
	}

	if err := checkLayerDigest(sum.Sum(nil), digest); err != nil {
		return err
	}

	return nil
}

// untarReader detects gzip by magic bytes and extracts the tar stream
// into rootfs. A short or empty body extracts as plain tar; only real
// read failures abort the pull.
func untarReader(ctx context.Context, buf *bufio.Reader, rootfs string) error {
	peek, err := buf.Peek(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("peek layer: %w", err)
	}

	if len(peek) == 2 && peek[0] == 0x1f && peek[1] == 0x8b {
		if err := extractGzipTar(ctx, buf, rootfs); err != nil {
			return err
		}
		return nil
	}

	if err := extractTar(ctx, buf, rootfs); err != nil {
		return fmt.Errorf("extract layer: %w", err)
	}
	return nil
}

// extractGzipTar gunzips buf and extracts the tar into rootfs.
func extractGzipTar(ctx context.Context, buf *bufio.Reader, rootfs string) error {
	gz, err := gzip.NewReader(buf)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	extractErr := extractTar(ctx, gz, rootfs)
	closeErr := gz.Close()
	if extractErr != nil {
		return fmt.Errorf("extract layer: %w", extractErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close gzip reader: %w", closeErr)
	}
	return nil
}

// checkLayerDigest compares the streamed hash against digest, which
// must be a canonical lowercase sha256 digest naming the exact blob
// bytes. Uppercase input is rejected to match blob.Store and
// transport.ValidBlobDigest.
func checkLayerDigest(sum []byte, digest string) error {
	lower, err := canonicalDigestHex(digest)
	if err != nil {
		return err
	}
	if hex.EncodeToString(sum) != lower {
		return fmt.Errorf("layer digest mismatch for %q", digest) // digest is context
	}
	return nil
}
