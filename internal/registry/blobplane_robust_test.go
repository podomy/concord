// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package registry

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Manifest PUT shares the blob upload semaphore: a saturated bound fails
// fast 503, and a drained bound still answers 201.
func TestRobustManifestPutSaturated503(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)
	target := "/v2/testrepo/robust-bound/manifests/latest"
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)

	for range maxConcurrentUploads {
		uploadSlots <- struct{}{}
	}
	rec := serve(t, reg, http.MethodPut, target, manifest)
	if rec.Code != http.StatusServiceUnavailable {
		for range maxConcurrentUploads {
			<-uploadSlots
		}
		t.Fatalf("status = %d, want 503, body = %s", rec.Code, rec.Body.String())
	}
	for range maxConcurrentUploads {
		<-uploadSlots
	}
	rec = serve(t, reg, http.MethodPut, target, manifest)
	requireCode(t, rec, http.StatusCreated)
}

// A manifest PUT declaring over the manifest cap fails 413 before the
// body is read, matching readManifestBody for served bodies.
func TestRobustManifestPutOversizeDeclared413(t *testing.T) {
	// No t.Parallel: serveManifestPut takes an upload slot, which the
	// saturated-bound test holds while it runs.
	reg := newTestRegistry(t)
	big := make([]byte, maxManifestBytes+1)
	rec := serve(t, reg, http.MethodPut, "/v2/testrepo/robust-big/manifests/latest", big)
	requireCode(t, rec, http.StatusRequestEntityTooLarge)
}

// An unknown-length manifest PUT passes the small disk guard and stores,
// keeping 201 for chunked or proxied bodies.
func TestRobustManifestPutUnknownLength201(t *testing.T) {
	// No t.Parallel: serveManifestPut takes an upload slot, which the
	// saturated-bound test holds while it runs.
	reg := newTestRegistry(t)
	manifest := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json"}`)
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPut, "/v2/testrepo/robust-chunked/manifests/latest", io.NopCloser(bytes.NewReader(manifest)))
	req.ContentLength = -1
	rec := httptest.NewRecorder()
	reg.serveV2(rec, req)
	requireCode(t, rec, http.StatusCreated)
}

// Serve caps one tags reply at maxTagsServe: the store keeps every tag,
// but the HTTP body stays bounded under tag spam.
func TestRobustTagsServeCap(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)
	repo := "testrepo/robust-tags"
	dir := filepath.Join(reg.storeDirFor(), "tags", filepath.FromSlash(repo))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := range maxTagsServe + 10 {
		name := fmt.Sprintf("tag-%04d", i)
		if err := os.WriteFile(filepath.Join(dir, name), []byte("sha256:"+strings.Repeat("0", 64)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	rec := serve(t, reg, http.MethodGet, "/v2/"+repo+"/tags/list", nil)
	requireCode(t, rec, http.StatusOK)
	var listing struct {
		Name string   `json:"name"`
		Tags []string `json:"tags"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Tags) != maxTagsServe {
		t.Fatalf("tags = %d, want cap %d", len(listing.Tags), maxTagsServe)
	}
}

// installRaw verifies the temp before the rename: a corrupt temp fails
// with the mismatch sentinel, leaves no dest, and keeps the temp for the
// caller to clean. A good temp installs and the temp path is consumed.
func TestRobustInstallRawVerifiesBeforeRename(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)
	dir := reg.storeDirFor()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	good := []byte("robust raw good")
	digest := digestOf(good)

	badName := auditWriteTemp(t, dir, []byte("robust raw corrupt"))
	if err := reg.installRaw(badName, digest); !errors.Is(err, errRawHashMismatch) {
		t.Fatalf("installRaw corrupt = %v, want hash mismatch", err)
	}
	dest, ok := rawBlobPath(dir, digest)
	if !ok {
		t.Fatal("raw blob path rejected valid digest")
	}
	if _, err := os.Stat(dest); !os.IsNotExist(err) {
		t.Fatalf("corrupt install left %s behind", dest)
	}
	if _, err := os.Stat(badName); err != nil {
		t.Fatalf("corrupt temp removed by installRaw, want caller-owned cleanup: %v", err)
	}
	_ = os.Remove(badName) //nolint:errcheck // test cleanup

	goodName := auditWriteTemp(t, dir, good)
	if err := reg.installRaw(goodName, digest); err != nil {
		t.Fatalf("installRaw good bytes: %v", err)
	}
	if _, err := os.Stat(dest); err != nil {
		t.Fatalf("installed file missing: %v", err)
	}
	if _, err := os.Stat(goodName); !os.IsNotExist(err) {
		t.Fatalf("installed temp %s still present, want rename", goodName)
	}
}

// The chunked serve path aborts like the raw path: headers commit 200 and
// a mid-stream verification failure ends the body short of Content-Length.
func TestRobustWriteChunkedBlobAborts(t *testing.T) {
	t.Parallel()

	reg := newTestRegistry(t)
	rec := httptest.NewRecorder()
	reg.writeChunkedBlob(rec, failReadCloser{err: errors.New("chunk corrupt")}, digestOf([]byte("x")), 100)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 committed before abort", rec.Code)
	}
	if rec.Body.Len() >= 100 {
		t.Fatalf("body = %d bytes, want short of Content-Length 100", rec.Body.Len())
	}
}

// The periodic disk guard propagates exhaustion: a failing space check
// aborts the stream with the no-space sentinel, which the caller maps to
// 413 instead of 400.
func TestRobustProgressWriterFullDisk(t *testing.T) {
	t.Parallel()

	tmp, err := os.CreateTemp(t.TempDir(), ".tmp-progress-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Remove(tmp.Name()) //nolint:errcheck // test cleanup
	})
	pw := &progressWriter{
		dst:       tmp,
		nextCheck: 1,
		check: func(string) error {
			return errUploadNoSpace
		},
	}
	_, werr := pw.Write([]byte("hello"))
	if !errors.Is(werr, errUploadNoSpace) {
		t.Fatalf("write with full disk = %v, want no-space", werr)
	}
	if err := checkTempSpace(t.TempDir()); err != nil {
		t.Fatalf("checkTempSpace on roomy dir = %v, want nil", err)
	}
}

// failReadCloser fails every read with a fixed error.
type failReadCloser struct {
	err error
}

func (f failReadCloser) Read([]byte) (int, error) { return 0, f.err }
func (f failReadCloser) Close() error             { return nil }
