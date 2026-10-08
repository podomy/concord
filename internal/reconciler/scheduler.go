// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/workload"
)

// MemberSource lists current membership (memberlist).
// *peerdiscovery.MemberService implements it.
type MemberSource interface {
	Members() ([]peerdiscovery.Node, error)
}

// ErrNoMembers indicates that the cluster members list is empty.
var ErrNoMembers = errors.New("not enough members in the members list")

// getLeader determines the cluster leader by finding the member with the
// lowest lexicographical UUID string.
func getLeader(peerService MemberSource) (uuid.UUID, error) {
	if peerService == nil {
		return uuid.Nil, ErrNoMembers //nolint:wrapcheck // sentinel error
	}
	members, err := peerService.Members()
	if err != nil {
		return uuid.Nil, fmt.Errorf("member service: %w", err)
	}
	if len(members) == 0 {
		return uuid.Nil, ErrNoMembers //nolint:wrapcheck // sentinel error
	}

	minID := members[0].ID.String()
	for _, member := range members[1:] {
		if member.ID.String() < minID {
			minID = member.ID.String()
		}
	}

	leader, err := uuid.Parse(minID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse leader uuid: %w", err)
	}

	return leader, nil
}

// isLeader checks whether the given nodeID is currently the leader of the cluster.
func isLeader(myID uuid.UUID, peerService MemberSource) bool {
	leader, err := getLeader(peerService)
	if err != nil {
		return false
	}

	return leader == myID
}

// scheduleWorkloads assigns unassigned workload specs (AssignedNodeID == uuid.Nil)
// and reassigns orphaned specs whose owner is no longer alive to an alive node.
func scheduleWorkloads(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	workloads *journalview.Workloads,
	peerService MemberSource,
	nodeID uuid.UUID,
	views []journalview.View,
) {
	if peerService == nil {
		return
	}
	members, err := peerService.Members()
	if err != nil {
		logger.Error("peerservice members", zap.Error(err))
		return
	}
	if len(members) == 0 {
		return
	}

	specs, err := workloads.List(ctx)
	if err != nil {
		logger.Error("workloads list", zap.Error(err))
		return
	}

	alive := aliveNodeIDs(members)
	if len(alive) == 0 {
		return
	}

	for _, spec := range specs {
		if spec.Removed {
			continue
		}

		if spec.AssignedNodeID == uuid.Nil {
			chosenMember := pickNode(members)
			spec.AssignedNodeID = chosenMember.ID
			recordAssignment(ctx, logger, j, views, nodeID, spec)
			continue
		}

		if _, ok := alive[spec.AssignedNodeID]; ok {
			continue
		}

		chosenMember := pickNode(members)
		if chosenMember.State != peerdiscovery.NodeStateAlive {
			continue
		}
		spec.AssignedNodeID = chosenMember.ID
		spec.AssignmentEpoch++
		recordAssignment(ctx, logger, j, views, nodeID, spec)
	}
}

// aliveNodeIDs returns the set of member IDs currently observed as alive.
func aliveNodeIDs(members []peerdiscovery.Node) map[uuid.UUID]struct{} {
	alive := make(map[uuid.UUID]struct{}, len(members))
	for _, member := range members {
		if member.State == peerdiscovery.NodeStateAlive {
			alive[member.ID] = struct{}{}
		}
	}

	return alive
}

// recordAssignment writes one workload.spec copy carrying its current assignment.
func recordAssignment(ctx context.Context, logger *zap.Logger, j journal.Journal, views []journalview.View, nodeID uuid.UUID, spec workload.Spec) {
	payload, err := json.Marshal(spec)
	if err != nil {
		logger.Error("json marshal", zap.Error(err))
		return
	}

	event := journal.NewEvent(nodeID, "workload.spec", payload)
	err = journalview.RecordEventAndLog(ctx, logger, j, views, event, "workload.spec")
	if err != nil {
		logger.Error("record workload.spec event", zap.Error(err))
	}
}

// pickNode selects the peer discovery node with the lowest pressure,
// prioritizing healthy (NodeStateAlive) peers over inactive/dead nodes.
// Equal pressure falls back to fewest running workloads.
func pickNode(members []peerdiscovery.Node) peerdiscovery.Node {
	best := members[0]

	for _, member := range members[1:] {
		// Prefer alive nodes over non-alive nodes.
		if member.State == peerdiscovery.NodeStateAlive && best.State != peerdiscovery.NodeStateAlive {
			best = member
			continue
		}
		if member.State != best.State {
			continue
		}
		// Lowest pressure wins; fewest workloads breaks ties.
		memberPressure, bestPressure := nodePressure(member), nodePressure(best)
		if memberPressure != bestPressure {
			if memberPressure < bestPressure {
				best = member
			}
			continue
		}
		if member.Metadata.Workloads < best.Metadata.Workloads {
			best = member
		}
	}

	return best
}

// nodePressure reduces a member's utilization trio to one number: the max,
// so a node is as loaded as its most constrained resource.
func nodePressure(member peerdiscovery.Node) uint8 {
	return max(member.Metadata.CPUPercent, member.Metadata.MemPercent, member.Metadata.DiskPercent)
}
