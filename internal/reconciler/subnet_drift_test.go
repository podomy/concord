// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/cn"
	"github.com/podomy/concord/internal/peerdiscovery"
)

// A membership order that selects a different index than the boot index
// is drift; the check reports both indexes and stays sticky.
func TestCheckSubnetDrift(t *testing.T) {
	low := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	high := uuid.MustParse("99999999-9999-9999-9999-999999999999")
	members := []peerdiscovery.Node{{ID: high}, {ID: low}}

	cn.SetNodeSubnet(0)
	t.Cleanup(func() { cn.SetNodeSubnet(0) })

	drifted, current, active, err := checkSubnetDrift(members, low)
	if err != nil {
		t.Fatalf("drift check: %v", err)
	}
	if drifted {
		t.Fatal("low ID at index 0 with active 0 must not drift")
	}
	if current != 0 || active != 0 {
		t.Fatalf("indexes = %d/%d, want 0/0", current, active)
	}

	cn.SetNodeSubnet(1)
	drifted, current, active, err = checkSubnetDrift(members, low)
	if err != nil {
		t.Fatalf("drift check: %v", err)
	}
	if !drifted {
		t.Fatal("membership index 0 against active 1 must drift")
	}
	if current != 0 || active != 1 {
		t.Fatalf("indexes = %d/%d, want 0/1", current, active)
	}
}

// Self absent from the list is an error, not drift: uncertainty never
// warns.
func TestCheckSubnetDriftAbsentSelf(t *testing.T) {
	cn.SetNodeSubnet(0)
	t.Cleanup(func() { cn.SetNodeSubnet(0) })

	drifted, _, _, err := checkSubnetDrift([]peerdiscovery.Node{{ID: uuid.New()}}, uuid.New())
	if err == nil {
		t.Fatal("expected error for absent self, got nil")
	}
	if drifted {
		t.Fatal("absent self must not report drift")
	}
}
