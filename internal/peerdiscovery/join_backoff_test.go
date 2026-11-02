// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"
)

// The first failure waits about the base: 5s plus up to 25 percent
// jitter, never less than the base.
func TestJoinDelayBase(t *testing.T) {
	t.Parallel()

	for range 50 {
		if got := joinDelay(1); got < joinBackoffBase || got > joinBackoffBase+joinBackoffBase/4+time.Second {
			t.Fatalf("delay(1) = %v, want within 5s..~6.25s", got)
		}
	}
}

// Failures double the wait and the ceiling holds at 5m plus jitter.
func TestJoinDelayCaps(t *testing.T) {
	t.Parallel()

	base := joinBackoffBase
	for f := 1; f <= 20; f++ {
		if base > joinBackoffMax {
			base = joinBackoffMax
		}
		got := joinDelay(f)
		if got < base || got > base+base/4+time.Second {
			t.Fatalf("delay(%d) = %v, want within %v..%v", f, got, base, base+base/4)
		}
		base *= 2
	}
}

// A failed round gates the address until its window expires; success
// clears it. Skipped addresses keep their window.
func TestJoinFilterAndRecord(t *testing.T) {
	t.Parallel()

	m := &MemberService{}
	dead := netip.MustParseAddrPort("10.0.0.9:7946")
	other := netip.MustParseAddrPort("10.0.0.10:7946")

	sentinel := errors.New("dial refused")
	m.joinRecord([]netip.AddrPort{dead}, sentinel)
	if got := m.joinFilter([]netip.AddrPort{dead, other}); len(got) != 1 || got[0] != other {
		t.Fatalf("filter = %v, want only %v", got, other)
	}

	m.joinRecord([]netip.AddrPort{other}, nil)
	if got := m.joinFilter([]netip.AddrPort{dead, other}); len(got) != 1 || got[0] != other {
		t.Fatalf("filter after unrelated success = %v, want only %v", got, other)
	}

	m.joinRecord([]netip.AddrPort{dead}, nil)
	if got := m.joinFilter([]netip.AddrPort{dead}); len(got) != 1 {
		t.Fatalf("filter after success = %v, want attemptable", got)
	}
}

// The join error carries the local fingerprint and the mismatch hint;
// without a fingerprint the legacy prefix stays byte-identical.
func TestJoinErrFingerprint(t *testing.T) {
	t.Parallel()

	base := errors.New("no route to host")
	withFP := joinErr(base, "abc123")
	if !errors.Is(withFP, base) {
		t.Fatal("join error must wrap the memberlist error")
	}
	msg := withFP.Error()
	if !strings.Contains(msg, "abc123") || !strings.Contains(msg, "secret.key") {
		t.Fatalf("join error = %q, want fingerprint and hint", msg)
	}
	if !strings.HasPrefix(msg, "list join") {
		t.Fatalf("join error = %q, want stable prefix", msg)
	}

	plain := joinErr(base, "")
	if plain.Error() != "list join: no route to host" {
		t.Fatalf("join error = %q, want legacy text", plain.Error())
	}
}
