// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// blobDigestForTest returns the canonical digest for data.
func blobDigestForTest(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Forged bytes with 200 fail FetchBlob with a digest mismatch.
func TestFetchBlobDigestMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	genuine := []byte("genuine-blob-bytes")
	forged := []byte("forged-blob-bytes-padded")
	digest := "sha256:" + strings.Repeat("a", 64)
	_ = digest
	sumDigest := blobDigestForTest(genuine)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(forged) //nolint:errcheck // test stub
	}))
	t.Cleanup(srv.Close)
	client := NewBlobClient()
	var buf bytes.Buffer
	_, err := client.FetchBlob(ctx, srv.URL+"/v2", BlobRepo, sumDigest, &buf, 1<<20)
	if err == nil {
		t.Fatalf("FetchBlob accepted forged bytes")
	}
	if !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("error = %v want mismatch", err)
	}
}

// Missing blobs report status errors so the caller can fail over.
func TestFetchBlob404(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	data := []byte("good-bytes")
	digest := blobDigestForTest(data)
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(bad.Close)
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data) //nolint:errcheck // test stub
	}))
	t.Cleanup(good.Close)
	client := NewBlobClient()
	var buf bytes.Buffer
	_, err := client.FetchBlob(ctx, bad.URL+"/v2", BlobRepo, digest, &buf, 1<<20)
	if err == nil {
		t.Fatalf("FetchBlob accepted 404")
	}
	if !strings.Contains(err.Error(), "404") {
		t.Fatalf("error = %v want 404", err)
	}
	buf.Reset()
	n, err := client.FetchBlob(ctx, good.URL+"/v2", BlobRepo, digest, &buf, 1<<20)
	if err != nil {
		t.Fatalf("failover FetchBlob: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("n = %d want %d", n, len(data))
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("bytes differ after failover")
	}
}
