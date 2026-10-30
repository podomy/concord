// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"encoding/json"
	"net/netip"
	"strings"
	"testing"

	"github.com/podomy/concord/internal/blob"
)

// Digests outside strict sha256 hex form never reach a URL or store.
func TestValidBlobDigest(t *testing.T) {
	t.Parallel()

	good := "sha256:" + strings.Repeat("a", 64)
	bad := []string{
		"",
		"sha256:short",
		"sha256:" + strings.Repeat("z", 64),
		"sha256:" + strings.Repeat("A", 64),
		"md5:" + strings.Repeat("a", 32),
		"sha256:" + strings.Repeat("a", 63) + " ",
		"../sha256:" + strings.Repeat("a", 64),
	}
	if !ValidBlobDigest(good) {
		t.Fatalf("ValidBlobDigest(%q) false", good)
	}
	for _, digest := range bad {
		if ValidBlobDigest(digest) {
			t.Fatalf("ValidBlobDigest(%q) true", digest)
		}
	}
}

// The peer base URL pins the registry port to the underlay IP: the
// gossip port must never appear here.
func TestPeerRegistryBase(t *testing.T) {
	t.Parallel()

	addr := netip.MustParseAddr("192.168.1.20")
	if got := PeerRegistryBase(addr, 8444); got != "http://192.168.1.20:8444/v2" {
		t.Fatalf("base = %q", got)
	}
	if got := PeerRegistryBase(netip.Addr{}, 8444); got != "" {
		t.Fatalf("invalid addr base = %q", got)
	}
	for _, port := range []int{0, -1, 65536} {
		if got := PeerRegistryBase(addr, port); got != "" {
			t.Fatalf("port %d base = %q", port, got)
		}
	}
}

// Manifests validate version, chunk size, digest shape, and root
// binding against the blob contract.
func TestParseBlobManifest(t *testing.T) {
	t.Parallel()

	chunks, m := blob.Split([]byte("manifest-test-payload"))
	raw, dm, err := ManifestBytes(m)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseBlobManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Root != m.Root || len(parsed.Chunks) != len(chunks) {
		t.Fatalf("parsed = %+v", parsed)
	}
	if dm == "" {
		t.Fatalf("empty manifest digest")
	}

	mutants := []func(*BlobManifest){
		func(m *BlobManifest) { m.Version = 2 },
		func(m *BlobManifest) { m.ChunkSize++ },
		func(m *BlobManifest) { m.Root = "sha256:" + strings.Repeat("0", 64) },
		func(m *BlobManifest) { m.Chunks = append(m.Chunks, "sha256:"+strings.Repeat("0", 64)) },
		func(m *BlobManifest) { m.TotalSize = -1 },
	}
	for i, mutate := range mutants {
		dup := m
		mutate(&dup)
		mutRaw, err := json.Marshal(dup) //nolint:errchkjson // struct encoding never fails, fallible path covered elsewhere
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ParseBlobManifest(mutRaw); err == nil {
			t.Fatalf("mutant %d accepted", i)
		}
	}
	if _, err := ParseBlobManifest([]byte("{invalid")); err == nil {
		t.Fatalf("garbage manifest accepted")
	}
}

// BlobRoot recomputes the same root the blob package binds.
func TestBlobRootMatchesSplit(t *testing.T) {
	t.Parallel()

	chunks, m := blob.Split(make([]byte, 3*blob.ChunkSize+10))
	if len(chunks) != 4 {
		t.Fatalf("chunks = %d, want 4", len(chunks))
	}
	root, err := BlobRoot(m.Chunks)
	if err != nil {
		t.Fatal(err)
	}
	if root != m.Root {
		t.Fatalf("root = %s, want %s", root, m.Root)
	}
	if _, err := BlobRoot([]string{"bogus"}); err == nil {
		t.Fatalf("bogus chunk accepted")
	}
}
