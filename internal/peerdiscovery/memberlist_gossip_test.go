// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"reflect"
	"strings"
	"testing"
)

// Oversize metadata fails Publish's pre-flight instead of panicking inside
// memberlist.
func TestCheckMetaSizeRejectsOversize(t *testing.T) {
	t.Parallel()

	big := NodeMetadata{WireGuardPublicKey: strings.Repeat("k", nodeMetaLimit+1)}
	if err := checkMetaSize(big); err == nil {
		t.Fatal("expected error for oversize metadata")
	}
}

// Normal metadata passes the pre-flight.
func TestCheckMetaSizeAcceptsNormal(t *testing.T) {
	t.Parallel()

	meta := NodeMetadata{
		WireGuardPublicKey: "key",
		CPUMHz:             2400,
		MemoryMB:           8192,
		CPUPercent:         30,
	}
	if err := checkMetaSize(meta); err != nil {
		t.Fatalf("check meta size: %v", err)
	}
}

// Remote gossip sanitizes on read: percents clamp, negative workloads
// floor, invalid positions reset. The input copy is sanitized, nothing
// stored mutates because sanitize takes and returns a value.
func TestSanitizeNodeMeta(t *testing.T) {
	t.Parallel()

	meta := NodeMetadata{
		CPUPercent:  200,
		MemPercent:  101,
		DiskPercent: 50,
		Workloads:   -3,
		Lat:         91,
		Lon:         0,
	}
	got := sanitizeNodeMeta(meta)
	if got.CPUPercent != 100 || got.MemPercent != 100 {
		t.Fatalf("percents = %d/%d, want 100/100", got.CPUPercent, got.MemPercent)
	}
	if got.DiskPercent != 50 {
		t.Fatalf("disk = %d, want 50", got.DiskPercent)
	}
	if got.Workloads != 0 {
		t.Fatalf("workloads = %d, want 0", got.Workloads)
	}
	if got.Lat != 0 || got.Lon != 0 {
		t.Fatalf("position = %v/%v, want 0/0", got.Lat, got.Lon)
	}
}

// In-range remote values pass through untouched.
func TestSanitizeNodeMetaKeepsValid(t *testing.T) {
	t.Parallel()

	meta := NodeMetadata{
		CPUPercent:  30,
		MemPercent:  40,
		DiskPercent: 50,
		Workloads:   2,
		Lat:         47.6,
		Lon:         8.9,
	}
	if got := sanitizeNodeMeta(meta); !reflect.DeepEqual(got, meta) {
		t.Fatalf("got %+v, want %+v", got, meta)
	}
}

// NodeMeta enforces the caller limit instead of panicking downstream:
// over-limit metadata advertises nothing (nil) until Publish's
// pre-flight rejects it with an error. Normal metadata within the limit
// is unchanged.
func TestNodeMetaEnforcesLimit(t *testing.T) {
	t.Parallel()

	delegate := &nodeMetadataDelegate{}
	delegate.meta = func() NodeMetadata {
		return NodeMetadata{CPUMHz: 2400.0, MemoryMB: 8192}
	}

	full := delegate.NodeMeta(512)
	if len(full) == 0 || len(full) > 512 {
		t.Fatalf("full meta = %d bytes, want within cap", len(full))
	}
	if got := delegate.NodeMeta(len(full) - 1); got != nil {
		t.Fatalf("over-limit meta = %q, want nil", got)
	}
}
