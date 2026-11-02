// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"strings"
	"testing"

	"github.com/podomy/concord/internal/transport"
	"github.com/podomy/concord/internal/workload"
)

// normalizeBlobDigest accepts any hex case and folds to canonical
// lowercase, rejecting anything not shaped like a digest.
func TestRobustNormalizeBlobDigest(t *testing.T) {
	t.Parallel()

	lower := "sha256:" + strings.Repeat("a", 64)
	upper := "sha256:" + strings.Repeat("A", 64)
	norm, ok := normalizeBlobDigest(upper)
	if !ok {
		t.Fatalf("normalizeBlobDigest(%q) rejected", upper)
	}
	if norm != lower {
		t.Fatalf("normalizeBlobDigest(%q) = %q, want %q", upper, norm, lower)
	}
	for _, bad := range []string{"", "bogus", "md5:" + strings.Repeat("a", 64), "sha256:" + strings.Repeat("z", 64), "sha256:" + strings.Repeat("a", 63)} {
		if _, ok := normalizeBlobDigest(bad); ok {
			t.Fatalf("normalizeBlobDigest(%q) accepted", bad)
		}
	}
}

// cleanDigests folds hex case to canonical form while deduping, and still
// fails the whole call on a malformed entry.
func TestRobustCleanDigestsNormalizes(t *testing.T) {
	t.Parallel()

	lower := "sha256:" + strings.Repeat("b", 64)
	upper := "sha256:" + strings.Repeat("B", 64)
	other := "sha256:" + strings.Repeat("c", 64)
	clean, err := cleanDigests([]string{upper, lower, other})
	if err != nil {
		t.Fatalf("cleanDigests: %v", err)
	}
	if len(clean) != 2 || clean[0] != lower || clean[1] != other {
		t.Fatalf("cleanDigests = %v, want [%s %s]", clean, lower, other)
	}
	if _, err := cleanDigests([]string{lower, "bogus"}); err == nil {
		t.Fatal("cleanDigests accepted bogus entry")
	}
}

// filterValidDigests folds peer hex case to canonical form and drops the
// rest: peer listings are untrusted input, not caller bugs.
func TestRobustFilterValidDigestsNormalizes(t *testing.T) {
	t.Parallel()

	lower := "sha256:" + strings.Repeat("d", 64)
	upper := "sha256:" + strings.Repeat("D", 64)
	clean := filterValidDigests([]string{upper, "bogus", lower})
	if len(clean) != 2 {
		t.Fatalf("filterValidDigests = %v, want two entries", clean)
	}
	for _, digest := range clean {
		if !transport.ValidBlobDigest(digest) {
			t.Fatalf("filterValidDigests entry %q not canonical", digest)
		}
	}
}

// WantedFromSpecs and WantedWithImages fold spec hex case so uppercase
// journal digests fetch instead of dropping silently.
func TestRobustWantedNormalizes(t *testing.T) {
	t.Parallel()

	upper := "sha256:" + strings.Repeat("E", 64)
	lower := "sha256:" + strings.Repeat("e", 64)
	specs := []workload.Spec{{Files: []workload.FileRef{{Path: "/a", Digest: upper}}}}
	got := WantedFromSpecs(specs)
	if len(got) != 1 || got[0] != lower {
		t.Fatalf("WantedFromSpecs = %v, want [%s]", got, lower)
	}
	merged := WantedWithImages(nil, []string{upper})
	if len(merged) != 1 || merged[0] != lower {
		t.Fatalf("WantedWithImages = %v, want [%s]", merged, lower)
	}
}
