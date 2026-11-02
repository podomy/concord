// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/workload"
)

// The final ID tiebreak keeps the pick identical for every observer even
// when memberlists arrive in different orders: equal pressure and equal
// workload counts decide by lowest node ID string, not input position.
func TestPickNodeTieBreakIsOrderIndependent(t *testing.T) {
	t.Parallel()

	mkNode := func(id uuid.UUID) peerdiscovery.Node {
		return peerdiscovery.Node{
			ID:    id,
			State: peerdiscovery.NodeStateAlive,
			Metadata: peerdiscovery.NodeMetadata{
				Workloads:   3,
				CPUPercent:  20,
				MemPercent:  20,
				DiskPercent: 20,
			},
		}
	}
	low := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	high := uuid.MustParse("99999999-9999-9999-9999-999999999999")

	first, ok := pickNode([]peerdiscovery.Node{mkNode(high), mkNode(low)}, workload.Spec{}, nil)
	if !ok {
		t.Fatal("expected a fit")
	}
	second, ok := pickNode([]peerdiscovery.Node{mkNode(low), mkNode(high)}, workload.Spec{}, nil)
	if !ok {
		t.Fatal("expected a fit")
	}
	if first.ID != second.ID {
		t.Fatalf("order changed pick: %v vs %v", first.ID, second.ID)
	}
	if first.ID != low {
		t.Fatalf("tiebreak picked %v, want lowest ID %v", first.ID, low)
	}
}
