// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"strings"
	"testing"
)

// Released IPs reuse before the cursor advances: destroy and recreate
// cycles converge instead of marching toward exhaustion.
func TestAllocateReleaseReallocatesSameIP(t *testing.T) {
	SetNodeSubnet(5)
	t.Cleanup(func() { SetNodeSubnet(0) })

	first, err := AllocateIP()
	if err != nil {
		t.Fatalf("allocate ip: %v", err)
	}
	if first != "10.0.5.2/24" {
		t.Fatalf("first = %s, want 10.0.5.2/24", first)
	}
	second, err := AllocateIP()
	if err != nil {
		t.Fatalf("allocate ip: %v", err)
	}
	if second != "10.0.5.3/24" {
		t.Fatalf("second = %s, want 10.0.5.3/24", second)
	}

	ReleaseIP(first)
	got, err := AllocateIP()
	if err != nil {
		t.Fatalf("allocate after release: %v", err)
	}
	if got != first {
		t.Fatalf("reallocated = %s, want freed %s", got, first)
	}
}

// An adopted container's IP is skipped by the cursor, never handed out
// twice. Unknown and out-of-subnet CIDRs are ignored.
func TestAdoptedIPNotReallocated(t *testing.T) {
	SetNodeSubnet(6)
	t.Cleanup(func() { SetNodeSubnet(0) })

	AdoptIP("10.0.6.2/24")
	got, err := AllocateIP()
	if err != nil {
		t.Fatalf("allocate ip: %v", err)
	}
	if got != "10.0.6.3/24" {
		t.Fatalf("allocated = %s, want 10.0.6.3/24 (adopted .2 skipped)", got)
	}

	AdoptIP("not-a-cidr")
	AdoptIP("10.0.99.2/24")
	if _, err := AllocateIP(); err != nil {
		t.Fatalf("allocate after bad adopts: %v", err)
	}
}

// The exhaustion error names the subnet and tells the operator how to
// recover: remove workloads or restart to reclaim addresses.
func TestExhaustionMessageMentionsRemediation(t *testing.T) {
	SetNodeSubnet(9)
	t.Cleanup(func() { SetNodeSubnet(0) })
	for range 254 {
		if _, err := AllocateIP(); err != nil {
			t.Fatalf("allocation: %v", err)
		}
	}
	_, err := AllocateIP()
	if err == nil {
		t.Fatal("expected exhaustion error, got nil")
	}
	if !strings.Contains(err.Error(), "10.0.9.0/24") {
		t.Fatalf("exhaustion error names no subnet: %v", err)
	}
	if !strings.Contains(err.Error(), "reclaim") {
		t.Fatalf("exhaustion error names no remediation: %v", err)
	}
}
