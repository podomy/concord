// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/peerdiscovery"
)

func TestPickNode(t *testing.T) {
	t.Parallel()

	node1 := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: 5,
		},
	}
	node2 := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: 2,
		},
	}
	deadNode := peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateDead,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: 0,
		},
	}

	members := []peerdiscovery.Node{node1, deadNode, node2}
	chosen := pickNode(members)

	if chosen.ID != node2.ID {
		t.Fatalf("expected alive node2 to be picked, got %v", chosen.ID)
	}
}

func TestLeaderElectionNilService(t *testing.T) {
	t.Parallel()

	leader, err := getLeader(nil)
	if err == nil || leader != uuid.Nil {
		t.Fatalf("expected error and nil UUID for nil MemberService, got %v, %v", leader, err)
	}

	if isLeader(uuid.New(), nil) {
		t.Fatal("expected isLeader to return false for nil MemberService")
	}
}
