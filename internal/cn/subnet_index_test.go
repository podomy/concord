// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/peerdiscovery"
)

// SubnetIndexFor sorts members by ID string and returns the position:
// the shared order behind boot picks and drift checks.
func TestSubnetIndexFor(t *testing.T) {
	t.Parallel()

	low := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	mid := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	high := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	members := []peerdiscovery.Node{{ID: high}, {ID: low}, {ID: mid}}

	for id, want := range map[uuid.UUID]int{low: 0, mid: 1, high: 2} {
		got, err := SubnetIndexFor(members, id)
		if err != nil {
			t.Fatalf("index for %s: %v", id, err)
		}
		if got != want {
			t.Fatalf("index for %s = %d, want %d", id, got, want)
		}
	}
}

// Absence is an error, never a silent 0: a silent 0 would overlap
// another node's /24.
func TestSubnetIndexForAbsentErrors(t *testing.T) {
	t.Parallel()

	members := []peerdiscovery.Node{{ID: uuid.New()}}
	if _, err := SubnetIndexFor(members, uuid.New()); err == nil {
		t.Fatal("expected error for absent node, got nil")
	}
}
