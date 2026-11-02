// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/workload"
)

// Fresh-boot nodes gossip all-zero pressure until the first sample.
// Unknown deprioritizes below any measured node but never excludes.
func TestPickNodeDeprioritizesUnknownPressure(t *testing.T) {
	t.Parallel()

	unknown := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: 0,
		},
	}
	measured := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads:   10,
			CPUPercent:  80,
			MemPercent:  70,
			DiskPercent: 60,
		},
	}

	chosen, ok := pickNode([]peerdiscovery.Node{unknown, measured}, workload.Spec{}, nil)
	if !ok {
		t.Fatal("expected a fit")
	}
	if chosen.ID != measured.ID {
		t.Fatalf("expected measured node to beat fresh-boot unknown, got %v", chosen.ID)
	}
}

// All-unknown candidates pick normally among themselves: pressure ties at
// zero, so fewest workloads wins instead of stranding.
func TestPickNodeAllUnknownPicksNormally(t *testing.T) {
	t.Parallel()

	light := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: 1,
		},
	}
	heavy := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: 9,
		},
	}

	chosen, ok := pickNode([]peerdiscovery.Node{heavy, light}, workload.Spec{}, nil)
	if !ok {
		t.Fatal("expected unknown fleet to still place")
	}
	if chosen.ID != light.ID {
		t.Fatalf("expected fewest workloads among unknown, got %v", chosen.ID)
	}
}
