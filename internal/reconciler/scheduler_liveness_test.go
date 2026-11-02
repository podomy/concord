// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/workload"
)

// Liveness gates every placement, not just orphans: an initial
// assignment never lands on a dead or suspect node however empty it
// looks, and waits for an alive node instead.
func TestPickNodeSkipsNonAliveForInitialPlacement(t *testing.T) {
	t.Parallel()

	dead := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateDead,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: 0,
		},
	}
	suspect := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateSuspect,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: 0,
		},
	}
	loaded := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads:   9,
			CPUPercent:  90,
			MemPercent:  80,
			DiskPercent: 70,
		},
	}

	chosen, ok := pickNode([]peerdiscovery.Node{dead, suspect, loaded}, workload.Spec{}, nil)
	if !ok {
		t.Fatal("expected the alive node to place")
	}
	if chosen.ID != loaded.ID {
		t.Fatalf("placed on non-alive node %v, want alive %v", chosen.ID, loaded.ID)
	}
}

// No alive candidate means no placement at all: the spec waits for the
// next tick instead of stranding where it cannot run.
func TestPickNodeNoFitWithoutAlive(t *testing.T) {
	t.Parallel()

	dead := peerdiscovery.Node{ID: uuid.New(), State: peerdiscovery.NodeStateDead}
	if _, ok := pickNode([]peerdiscovery.Node{dead}, workload.Spec{}, nil); ok {
		t.Fatal("expected no fit with only dead nodes")
	}
	if _, ok := pickNode(nil, workload.Spec{}, nil); ok {
		t.Fatal("expected no fit with no members")
	}
}
