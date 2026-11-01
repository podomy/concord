// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
)

// rawDigestOf returns the sha256 digest for data.
func rawDigestOf(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

// TestGetRawSizeReportsStoredSize checks the size gate used before load.
func TestGetRawSizeReportsStoredSize(t *testing.T) {
	t.Parallel()
	s, _ := mustOpen(t)
	ctx := context.Background()
	data := makeData(1234)
	digest := rawDigestOf(data)
	mustPutRaw(t, s, ctx, digest, data)
	size, ok := s.GetRawSize(digest)
	if !ok {
		t.Fatalf("GetRawSize false after put")
	}
	if size != int64(len(data)) {
		t.Fatalf("GetRawSize = %d want %d", size, len(data))
	}
	missing := "sha256:" + strings.Repeat("0", 64)
	if _, ok := s.GetRawSize(missing); ok {
		t.Fatalf("GetRawSize true for missing")
	}
	if _, ok := s.GetRawSize("bogus"); ok {
		t.Fatalf("GetRawSize true for malformed")
	}
}

// TestPutRawCapCoherent proves PutRaw and GetRaw share the 32MB cap so
// no large raw can exist for GetRaw to later reject. Large bytes must
// use PutBlob chunked plus OpenBlob streaming.
func TestPutRawCapCoherent(t *testing.T) {
	t.Parallel()
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
	if s.HasRaw(digest) {
		t.Fatalf("HasRaw true after rejected put")
	}
	if _, ok := s.GetRawSize(digest); ok {
		t.Fatalf("GetRawSize true after rejected put")
	}
}
