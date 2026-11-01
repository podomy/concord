// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package registry

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/blob"
	"github.com/podomy/concord/internal/logs"
)

// newTestRegistry creates a registry backed by an isolated store dir.
// Requests route through serveV2 directly, so no listener port is used.
func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "blobs")
	store, err := blob.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	return &Registry{store: store, storeDir: dir, logger: logs.NewNop(), nodeID: uuid.New()}
}

// digestOf returns the sha256 digest string for data.
func digestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// upperDigest returns the digest with an uppercase hex part.
func upperDigest(digest string) string {
	return "sha256:" + strings.ToUpper(strings.TrimPrefix(digest, "sha256:"))
}

// serve routes one request through the handler without a listener.
func serve(t *testing.T, reg *Registry, method, target string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, target, bytes.NewReader(body))
	rec := httptest.NewRecorder()
	reg.serveV2(rec, req)
	return rec
}

// requireCode fails the test unless rec holds the want status.
func requireCode(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status = %d, want %d, body = %s", rec.Code, want, rec.Body.String())
	}
}

// TestPing covers the version check without a listener port.
func TestPing(t *testing.T) {
	reg := newTestRegistry(t)
	rec := serve(t, reg, http.MethodGet, "/v2/", nil)
	requireCode(t, rec, http.StatusOK)
	if rec.Header().Get("Docker-Distribution-API-Version") != "registry/2.0" {
		t.Fatal("missing distribution version header")
	}
	if rec.Body.String() != "{}" {
		t.Fatalf("ping body = %q, want {}", rec.Body.String())
	}
	rec = serve(t, reg, http.MethodHead, "/v2/", nil)
	requireCode(t, rec, http.StatusOK)
	rec = serve(t, reg, http.MethodPost, "/v2/", nil)
	requireCode(t, rec, http.StatusMethodNotAllowed)
}

// TestBlobManifestTagsRoundTrip covers POST blob, HEAD and GET blob,
// PUT and GET manifest, and tags list through the handler.
func TestBlobManifestTagsRoundTrip(t *testing.T) {
	reg := newTestRegistry(t)
	repo := "testrepo/app"
	data := []byte("hello layer")
	digest := digestOf(data)
	rec := serve(t, reg, http.MethodPost, "/v2/"+repo+"/blobs/uploads?digest="+digest, data)
	requireCode(t, rec, http.StatusCreated)
	if rec.Header().Get("Docker-Content-Digest") != digest {
		t.Fatal("upload response missing digest header")
	}
	rec = serve(t, reg, http.MethodHead, "/v2/"+repo+"/blobs/"+digest, nil)
	requireCode(t, rec, http.StatusOK)
	if rec.Header().Get("Docker-Content-Digest") != digest {
		t.Fatal("head response missing digest header")
	}
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/blobs/"+digest, nil)
	requireCode(t, rec, http.StatusOK)
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("blob bytes differ from upload")
	}
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)
	rec = serve(t, reg, http.MethodPut, "/v2/"+repo+"/manifests/latest", manifest)
	requireCode(t, rec, http.StatusCreated)
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/manifests/latest", nil)
	requireCode(t, rec, http.StatusOK)
	if !bytes.Equal(rec.Body.Bytes(), manifest) {
		t.Fatal("manifest bytes differ from put")
	}
	if rec.Header().Get("Docker-Content-Digest") != digestOf(manifest) {
		t.Fatal("manifest response missing digest header")
	}
	rec = serve(t, reg, http.MethodHead, "/v2/"+repo+"/manifests/latest", nil)
	requireCode(t, rec, http.StatusOK)
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/tags/list", nil)
	requireCode(t, rec, http.StatusOK)
	var listing struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	err := json.Unmarshal(rec.Body.Bytes(), &listing)
	if err != nil {
		t.Fatal(err)
	}
	if listing.Name != repo {
		t.Fatalf("tags name = %q, want %q", listing.Name, repo)
	}
	if len(listing.Tags) != 1 || listing.Tags[0] != "latest" {
		t.Fatalf("tags = %v, want [latest]", listing.Tags)
	}
}

// TestTagOverwriteChangesServedManifest covers the mutable tag path:
// a second PUT to the same tag replaces the served manifest.
func TestTagOverwriteChangesServedManifest(t *testing.T) {
	reg := newTestRegistry(t)
	repo := "testrepo/overwrite"
	first := []byte(`{"schemaVersion":2,"hello":"one"}`)
	second := []byte(`{"schemaVersion":2,"hello":"two"}`)
	if digestOf(first) == digestOf(second) {
		t.Fatal("test manifests must hash differently")
	}
	rec := serve(t, reg, http.MethodPut, "/v2/"+repo+"/manifests/stable", first)
	requireCode(t, rec, http.StatusCreated)
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/manifests/stable", nil)
	requireCode(t, rec, http.StatusOK)
	if !bytes.Equal(rec.Body.Bytes(), first) {
		t.Fatal("first manifest not served")
	}
	rec = serve(t, reg, http.MethodPut, "/v2/"+repo+"/manifests/stable", second)
	requireCode(t, rec, http.StatusCreated)
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/manifests/stable", nil)
	requireCode(t, rec, http.StatusOK)
	if !bytes.Equal(rec.Body.Bytes(), second) {
		t.Fatal("tag overwrite did not change served manifest")
	}
}

// TestManifestDigestMismatch covers PUT by digest with a wrong body.
func TestManifestDigestMismatch(t *testing.T) {
	reg := newTestRegistry(t)
	body := []byte(`{"schemaVersion":2}`)
	wrong := digestOf([]byte("something else"))
	rec := serve(t, reg, http.MethodPut, "/v2/testrepo/mismatch/manifests/"+wrong, body)
	requireCode(t, rec, http.StatusBadRequest)
	if !strings.Contains(rec.Body.String(), "DIGEST_INVALID") {
		t.Fatalf("body = %s, want DIGEST_INVALID", rec.Body.String())
	}
}

// TestBlobDigestMismatch covers POST upload with a wrong digest query.
func TestBlobDigestMismatch(t *testing.T) {
	reg := newTestRegistry(t)
	data := []byte("layer bytes")
	wrong := digestOf([]byte("other bytes"))
	rec := serve(t, reg, http.MethodPost, "/v2/testrepo/mismatch/blobs/uploads?digest="+wrong, data)
	requireCode(t, rec, http.StatusBadRequest)
	if !strings.Contains(rec.Body.String(), "DIGEST_INVALID") {
		t.Fatalf("body = %s, want DIGEST_INVALID", rec.Body.String())
	}
}

// TestOversizeManifest covers PUT bodies over the 10MB cap.
func TestOversizeManifest(t *testing.T) {
	reg := newTestRegistry(t)
	pad := strings.Repeat("a", (10<<20)+1)
	raw := []byte(`{"data":"` + pad + `"}`)
	rec := serve(t, reg, http.MethodPut, "/v2/testrepo/big/manifests/latest", raw)
	requireCode(t, rec, http.StatusRequestEntityTooLarge)
}

// TestMissingBlob covers GET and HEAD of an unknown digest.
func TestMissingBlob(t *testing.T) {
	reg := newTestRegistry(t)
	missing := digestOf([]byte("no such blob"))
	rec := serve(t, reg, http.MethodGet, "/v2/testrepo/app/blobs/"+missing, nil)
	requireCode(t, rec, http.StatusNotFound)
	if !strings.Contains(rec.Body.String(), "BLOB_UNKNOWN") {
		t.Fatalf("body = %s, want BLOB_UNKNOWN", rec.Body.String())
	}
	rec = serve(t, reg, http.MethodHead, "/v2/testrepo/app/blobs/"+missing, nil)
	requireCode(t, rec, http.StatusNotFound)
}

// TestUnknownRepoTags covers the tags list of an unknown repository.
func TestUnknownRepoTags(t *testing.T) {
	reg := newTestRegistry(t)
	rec := serve(t, reg, http.MethodGet, "/v2/nosuchrepo/tags/list", nil)
	requireCode(t, rec, http.StatusNotFound)
	if !strings.Contains(rec.Body.String(), "NAME_UNKNOWN") {
		t.Fatalf("body = %s, want NAME_UNKNOWN", rec.Body.String())
	}
}

// TestUppercaseDigestNormalization covers uppercase hex digests in
// upload queries and in blob and manifest references.
func TestUppercaseDigestNormalization(t *testing.T) {
	reg := newTestRegistry(t)
	repo := "testrepo/case"
	data := []byte("case bytes")
	digest := digestOf(data)
	rec := serve(t, reg, http.MethodPost, "/v2/"+repo+"/blobs/uploads?digest="+upperDigest(digest), data)
	requireCode(t, rec, http.StatusCreated)
	if rec.Header().Get("Docker-Content-Digest") != digest {
		t.Fatal("upload digest header not normalized to lowercase")
	}
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/blobs/"+upperDigest(digest), nil)
	requireCode(t, rec, http.StatusOK)
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("uppercase blob fetch returned wrong bytes")
	}
	if rec.Header().Get("Docker-Content-Digest") != digest {
		t.Fatal("blob digest header not normalized to lowercase")
	}
	manifest := []byte(`{"schemaVersion":2}`)
	manDigest := digestOf(manifest)
	rec = serve(t, reg, http.MethodPut, "/v2/"+repo+"/manifests/"+upperDigest(manDigest), manifest)
	requireCode(t, rec, http.StatusCreated)
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/manifests/"+upperDigest(manDigest), nil)
	requireCode(t, rec, http.StatusOK)
	if !bytes.Equal(rec.Body.Bytes(), manifest) {
		t.Fatal("uppercase manifest fetch returned wrong bytes")
	}
}

// TestLargeBlobRoundTrip covers the large upload install path and the
// single-pass large serve path with bytes just over 10MB.
func TestLargeBlobRoundTrip(t *testing.T) {
	reg := newTestRegistry(t)
	repo := "testrepo/large"
	const size = (10 << 20) + 1024
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i * 31) % 251)
	}
	digest := digestOf(data)
	rec := serve(t, reg, http.MethodPost, "/v2/"+repo+"/blobs/uploads?digest="+digest, data)
	requireCode(t, rec, http.StatusCreated)
	rec = serve(t, reg, http.MethodHead, "/v2/"+repo+"/blobs/"+digest, nil)
	requireCode(t, rec, http.StatusOK)
	rec = serve(t, reg, http.MethodGet, "/v2/"+repo+"/blobs/"+digest, nil)
	requireCode(t, rec, http.StatusOK)
	if !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("large blob bytes differ from upload")
	}
}
