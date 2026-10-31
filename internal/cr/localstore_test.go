// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/blob"
	"github.com/podomy/concord/internal/registry"
	"github.com/podomy/concord/sdk/fileart"
)

// testDigest returns the sha256 digest string for data.
func testDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// putRawBlob stores data in a real blob store under root, mirroring
// what the embedded registry writes on upload, and returns its digest.
func putRawBlob(t *testing.T, root string, data []byte) string {
	t.Helper()

	store, err := blob.Open(filepath.Join(root, "concord", "zot", "blobs"))
	if err != nil {
		t.Fatal(err)
	}
	digest := testDigest(data)
	if err := store.PutRaw(t.Context(), digest, data); err != nil {
		t.Fatal(err)
	}
	return digest
}

// makeTarFile packs one file into a plain tar archive.
func makeTarFile(t *testing.T, name string, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// gzipBytes compresses data with gzip.
func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// mustMarshal encodes v as JSON, failing the test on error.
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()

	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// A config already in this node's store is served from disk: the
// registry port is dead, so success proves no HTTP happened.
func TestLocalConfigFastPath(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	want := ConfigFile{Architecture: "amd64", OS: "linux"}
	want.Config.Cmd = []string{"/bin/run"}
	digest := putRawBlob(t, dir, mustMarshal(t, want))

	ref := imageReference{registry: "127.0.0.1:1", repository: "library/localcfg", reference: "latest"}
	got, err := fetchImageConfig(t.Context(), &http.Client{}, ref, digest)
	if err != nil {
		t.Fatalf("fetch config from local store: %v", err)
	}
	if got.Architecture != "amd64" || len(got.Config.Cmd) != 1 || got.Config.Cmd[0] != "/bin/run" {
		t.Fatalf("config = %+v", got)
	}
}

// A layer already on disk extracts without HTTP and verifies the
// digest the same way the network path does.
func TestLocalLayerFastPath(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	payload := []byte("local layer bytes")
	digest := putRawBlob(t, dir, makeTarFile(t, "payload.txt", payload))

	rootfs := filepath.Join(t.TempDir(), "rootfs")
	if err := os.MkdirAll(rootfs, 0o750); err != nil {
		t.Fatal(err)
	}
	ref := imageReference{registry: "127.0.0.1:1", repository: "library/locallayer", reference: "latest"}
	if err := extractLayerBlob(t.Context(), &http.Client{}, ref, digest, rootfs); err != nil {
		t.Fatalf("extract layer from local store: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(rootfs, "payload.txt")) //nolint:gosec // test reads a file it just wrote
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("extracted bytes differ from blob bytes")
	}
}

// A blob missing from disk falls back to HTTP with identical results.
func TestLocalStoreMissFallsBackToHTTP(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	repo := "library/fallback"
	want := ConfigFile{Architecture: "arm64", OS: "linux"}
	raw := mustMarshal(t, want)
	digest := testDigest(raw)
	blobs := map[string][]byte{digest: raw}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, ok := blobs[strings.TrimPrefix(r.URL.Path, "/v2/"+repo+"/blobs/")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data) //nolint:errcheck // test stub
	}))
	defer server.Close()

	ref := imageReference{
		registry:   strings.TrimPrefix(server.URL, "http://"),
		repository: repo,
		reference:  "latest",
	}
	got, err := fetchImageConfig(t.Context(), server.Client(), ref, digest)
	if err != nil {
		t.Fatalf("fetch config via HTTP fallback: %v", err)
	}
	if got.Architecture != "arm64" {
		t.Fatalf("config = %+v", got)
	}
}

// A corrupt local blob never poisons the pull: the digest check fails
// and the error surfaces instead of bad bytes.
func TestLocalLayerDigestMismatchFails(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	putRawBlob(t, dir, makeTarFile(t, "payload.txt", []byte("tampered")))
	digest := testDigest([]byte("genuine"))

	rootfs := filepath.Join(t.TempDir(), "rootfs")
	if err := os.MkdirAll(rootfs, 0o750); err != nil {
		t.Fatal(err)
	}
	ref := imageReference{registry: "127.0.0.1:1", repository: "library/tampered", reference: digest}
	if err := extractLayerBlob(t.Context(), &http.Client{}, ref, digest, rootfs); err == nil {
		t.Fatal("expected digest mismatch error")
	}
}

// Crafted digests cannot escape the store root: every lookup fails
// closed and falls back to HTTP.
func TestLocalStoreRejectsBadDigests(t *testing.T) {
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	store := rawBlobDir{root: t.TempDir()}
	bad := []string{
		"",
		"md5:d41d8cd98f00b204e9800998ecf8427e",
		"sha256:zzzz",
		"sha256:abc",
		"sha256:../../etc/passwd",
		"sha256:" + strings.Repeat("0", 63),
		"sha256:" + strings.Repeat("0", 64) + "/../x",
	}
	for _, digest := range bad {
		if store.Has(digest) {
			t.Errorf("Has(%q) = true", digest)
		}
		if _, err := store.Open(digest); err == nil {
			t.Errorf("Open(%q) succeeded", digest)
		}
	}

	// A well-formed digest with no file behind it is a miss, not an
	// error shaped like a hit: the caller falls back to HTTP.
	missing := "sha256:" + strings.Repeat("0", 64)
	if store.Has(missing) {
		t.Errorf("Has(%q) = true", missing)
	}
	if _, err := store.Open(missing); err == nil {
		t.Errorf("Open(%q) succeeded", missing)
	}

	if _, ok := tryLocalStore(); ok {
		t.Error("tryLocalStore() succeeded without a store dir")
	}
}

// startTestRegistry creates an embedded registry on a test port and
// returns it with a client. The caller stops the registry when done
// pushing; reads after that prove the disk fast path.
func startTestRegistry(t *testing.T, port int) (*registry.Registry, *fileart.Client) {
	t.Helper()

	oldPort := registry.Port
	registry.Port = port
	t.Cleanup(func() { registry.Port = oldPort })

	reg, err := registry.New(uuid.New())
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Start(t.Context(), nil, logs.NewNop()); err != nil {
		t.Fatal(err)
	}
	return reg, fileart.NewClient("127.0.0.1:" + strconv.Itoa(port))
}

// Blobs pushed through the real registry are served from disk once
// the server is down: the fast path reads the same store the
// registry wrote, with no HTTP involved.
func TestLocalFastPathAgainstRealRegistry(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	reg, client := startTestRegistry(t, 18501)
	ctx := t.Context()

	cfg := ConfigFile{Architecture: "amd64", OS: "linux"}
	cfg.Config.Cmd = []string{"/bin/real"}
	configDigest, err := client.Push(ctx, mustMarshal(t, cfg))
	if err != nil {
		t.Fatalf("push config: %v", err)
	}
	payload := []byte("real registry layer")
	layerDigest, err := client.Push(ctx, makeTarFile(t, "real.txt", payload))
	if err != nil {
		t.Fatalf("push layer: %v", err)
	}
	reg.Stop()

	ref := imageReference{registry: "127.0.0.1:18501", repository: "concord-files", reference: "latest"}
	got, err := fetchImageConfig(ctx, &http.Client{}, ref, configDigest)
	if err != nil {
		t.Fatalf("fetch config from stopped registry: %v", err)
	}
	if got.Architecture != "amd64" || len(got.Config.Cmd) != 1 || got.Config.Cmd[0] != "/bin/real" {
		t.Fatalf("config = %+v", got)
	}

	rootfs := filepath.Join(t.TempDir(), "rootfs")
	if err := os.MkdirAll(rootfs, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := extractLayerBlob(ctx, &http.Client{}, ref, layerDigest, rootfs); err != nil {
		t.Fatalf("extract layer from stopped registry: %v", err)
	}
	content, err := os.ReadFile(filepath.Join(rootfs, "real.txt")) //nolint:gosec // test reads a file it just wrote
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(content, payload) {
		t.Fatal("extracted bytes differ from pushed bytes")
	}
}

// testDescriptor mirrors the manifest descriptor fields the fake
// registry needs to serve, including size.
type testDescriptor struct {
	MediaType string `json:"mediaType"`
	Digest    string `json:"digest"`
	Size      int64  `json:"size"`
}

// testManifest is the image manifest the fake registry serves.
type testManifest struct {
	SchemaVersion int              `json:"schemaVersion"`
	MediaType     string           `json:"mediaType"`
	Config        testDescriptor   `json:"config"`
	Layers        []testDescriptor `json:"layers"`
}

// A full Pull serves a 5MB gzip layer over HTTP with flat memory on
// the client: bytes stream from the socket through the hasher into
// the extractor and land byte-identical on disk.
func TestPullLargeLayerViaHTTP(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", dir)

	const size = 5 << 20
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i * 31) % 251)
	}
	layer := gzipBytes(t, makeTarFile(t, "big.bin", data))
	layerDigest := testDigest(layer)

	cfg := ConfigFile{Architecture: "amd64", OS: "linux"}
	cfg.Config.Cmd = []string{"/bin/big"}
	configRaw := mustMarshal(t, cfg)
	configDigest := testDigest(configRaw)

	repo := "testowner/biglayer"
	manifest := testManifest{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.manifest.v1+json",
		Config:        testDescriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: configDigest, Size: int64(len(configRaw))},
		Layers:        []testDescriptor{{MediaType: "application/vnd.oci.image.layer.v1.tar+gzip", Digest: layerDigest, Size: int64(len(layer))}},
	}
	manifestRaw := mustMarshal(t, manifest)
	blobs := map[string][]byte{configDigest: configRaw, layerDigest: layer}

	mux := http.NewServeMux()
	mux.HandleFunc("/v2/"+repo+"/manifests/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(manifestRaw) //nolint:errcheck // test stub
	})
	mux.HandleFunc("/v2/"+repo+"/blobs/", func(w http.ResponseWriter, r *http.Request) {
		data, ok := blobs[strings.TrimPrefix(r.URL.Path, "/v2/"+repo+"/blobs/")]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data) //nolint:errcheck // test stub
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	host := strings.TrimPrefix(server.URL, "http://")
	puller := &ImagePuller{}
	res, err := puller.Pull(t.Context(), host+"/"+repo+":latest", t.TempDir())
	if err != nil {
		t.Fatalf("pull: %v", err)
	}
	if res.Config == nil || res.Config.Architecture != "amd64" {
		t.Fatalf("config = %+v", res.Config)
	}
	got, err := os.ReadFile(filepath.Join(res.RootFS, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("pulled bytes differ from served bytes")
	}
}
