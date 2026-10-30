// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package blob

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
)

// checkLargeBlobMeta validates size after put.
func checkLargeBlobMeta(t *testing.T, s *Store, digest string, data []byte) {
	t.Helper()
	if !s.HasBlob(digest) {
		t.Fatalf("HasBlob false after put")
	}
	size, ok := s.GetBlobSize(digest)
	if !ok {
		t.Fatalf("GetBlobSize missing")
	}
	if size != int64(len(data)) {
		t.Fatalf("size = %d want %d", size, len(data))
	}
}

// streamBlob reads rc in chunks and returns the bytes.
func streamBlob(t *testing.T, rc io.ReadCloser, total int64, wantLen int) []byte {
	t.Helper()
	if total != int64(wantLen) {
		t.Fatalf("total = %d want %d", total, wantLen)
	}
	buf := make([]byte, 32*1024)
	var got []byte
	for {
		n, rerr := rc.Read(buf)
		if n > 0 {
			got = append(got, buf[:n]...)
		}
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			_ = rc.Close() //nolint:errcheck // close after failed read, error already captured
			t.Fatalf("read: %v", rerr)
		}
	}
	err := rc.Close()
	if err != nil {
		t.Fatalf("close: %v", err)
	}
	return got
}

// Large blobs round trip through streaming reads with flat memory.
func TestPutGetLargeBlobStreaming(t *testing.T) {
	t.Parallel()
	s, _ := mustOpen(t)
	ctx := context.Background()
	data := makeData(5 << 20)
	digest, err := s.PutBlob(ctx, data)
	if err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	checkLargeBlobMeta(t, s, digest, data)
	rc, total, err := s.OpenBlob(digest)
	if err != nil {
		t.Fatalf("OpenBlob: %v", err)
	}
	got := streamBlob(t, rc, total, len(data))
	if !bytes.Equal(got, data) {
		t.Fatalf("bytes differ size %d want %d", len(got), len(data))
	}
}

// checkOverwriteFirst validates the first tag binding.
func checkOverwriteFirst(t *testing.T, s *Store, d1 string) {
	t.Helper()
	err := s.PutTag("app", "latest", d1)
	if err != nil {
		t.Fatalf("PutTag d1: %v", err)
	}
	got, ok := s.ResolveTag("app", "latest")
	if !ok {
		t.Fatalf("ResolveTag missing after first put")
	}
	if got != d1 {
		t.Fatalf("resolve = %s want %s", got, d1)
	}
}

// checkOverwriteSecond validates the overwrite binding.
func checkOverwriteSecond(t *testing.T, s *Store, d1, d2 string) {
	t.Helper()
	if d1 == d2 {
		t.Fatalf("test digests collide")
	}
	err := s.PutTag("app", "latest", d2)
	if err != nil {
		t.Fatalf("PutTag d2: %v", err)
	}
	got, ok := s.ResolveTag("app", "latest")
	if !ok {
		t.Fatalf("ResolveTag missing after overwrite")
	}
	if got != d2 {
		t.Fatalf("resolve = %s want %s", got, d2)
	}
	tags := s.ListTags("app")
	if len(tags) != 1 {
		t.Fatalf("tags = %v want single entry", tags)
	}
	if tags[0] != "latest" {
		t.Fatalf("tag = %q want latest", tags[0])
	}
}

// Same tag bound twice resolves to the second digest.
func TestTagOverwriteSameTagDifferentDigest(t *testing.T) {
	t.Parallel()
	s, _ := mustOpen(t)
	ctx := context.Background()
	d1, err := s.PutBlob(ctx, makeData(100))
	if err != nil {
		t.Fatalf("PutBlob d1: %v", err)
	}
	d2, err := s.PutBlob(ctx, makeData(200))
	if err != nil {
		t.Fatalf("PutBlob d2: %v", err)
	}
	checkOverwriteFirst(t, s, d1)
	checkOverwriteSecond(t, s, d1, d2)
}
