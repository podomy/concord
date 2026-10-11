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
		scheduleOne(ctx, logger, j, views, nodeID, members, alive, spec)
	}
}

// scheduleOne assigns one unassigned spec or reassigns one orphan. Specs
// with healthy owners, tombstones, or no fitting node are left alone:
// placement that cannot fit waits for the next tick instead of landing
// where it cannot run.
func scheduleOne(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	views []journalview.View,
	nodeID uuid.UUID,
	members []peerdiscovery.Node,
	alive map[uuid.UUID]struct{},
	spec workload.Spec,
) {
	if spec.Removed {
		return
	}

	if spec.AssignedNodeID == uuid.Nil {
		chosenMember, ok := pickNode(members, spec)
		if !ok {
			logger.Debug("no node fits workload memory request, leaving unassigned",
				zap.String("workload_id", spec.ID.String()))
			return
		}
		spec.AssignedNodeID = chosenMember.ID
		recordAssignment(ctx, logger, j, views, nodeID, spec, "workload.spec assigned")
		return
	}

	if _, ok := alive[spec.AssignedNodeID]; ok {
		return
	}

	chosenMember, ok := pickNode(members, spec)
	if !ok {
		logger.Debug("no node fits orphan memory request, leaving orphaned",
			zap.String("workload_id", spec.ID.String()))
		return
	}
	if chosenMember.State != peerdiscovery.NodeStateAlive {
		return
	}
	spec.AssignedNodeID = chosenMember.ID
	spec.AssignmentEpoch++
	recordAssignment(ctx, logger, j, views, nodeID, spec, "workload.spec reassigned")
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

// recordAssignment writes one workload.spec copy carrying its current
// assignment. The message names the action so initial assignments and
// orphan reassignments read apart in logs.
func recordAssignment(ctx context.Context, logger *zap.Logger, j journal.Journal, views []journalview.View, nodeID uuid.UUID, spec workload.Spec, message string) {
	payload, err := json.Marshal(spec)
	if err != nil {
		logger.Error("json marshal", zap.Error(err))
		return
	}

	event := journal.NewEvent(nodeID, journalview.EventTypeWorkloadSpec, payload)
	err = journalview.RecordEventAndLog(ctx, logger, j, views, event, message,
		zap.String("workload_id", spec.ID.String()),
		zap.String("assigned_node", spec.AssignedNodeID.String()),
	)
	if err != nil {
		logger.Error("record workload.spec event", zap.Error(err))
	}
}

// pickNode selects the alive peer with the lowest pressure that fits the
// workload's memory request, preferring healthy peers; fewest running
// workloads breaks pressure ties. It reports false when no member fits,
// and the caller leaves the spec for the next tick instead of placing
// where it cannot run.
func pickNode(members []peerdiscovery.Node, spec workload.Spec) (peerdiscovery.Node, bool) {
	var best peerdiscovery.Node
	found := false

	for _, member := range members {
		if !fitsMemory(member, spec.Resources.MemoryMB) {
			continue
		}
		if !found {
			best, found = member, true
			continue
		}
		if prefersNode(member, best) {
			best = member
		}
	}

	return best, found
}

// prefersNode reports whether member beats best under the placement
// order: alive over non-alive, lowest pressure, fewest workloads.
func prefersNode(member, best peerdiscovery.Node) bool {
	if member.State == peerdiscovery.NodeStateAlive && best.State != peerdiscovery.NodeStateAlive {
		return true
	}
	if member.State != best.State {
		return false
	}
	memberPressure, bestPressure := nodePressure(member), nodePressure(best)
	if memberPressure != bestPressure {
		return memberPressure < bestPressure
	}
	return member.Metadata.Workloads < best.Metadata.Workloads
}

// fitsMemory reports whether the member has the requested megabytes free.
// No request (unlimited) always fits, and unknown node capacity never
// blocks: missing data must not strand workloads. CPU shares are relative
// weights and per-workload disk is unreported, so neither admits a fit
// check; pressure already steers off hot nodes.
func fitsMemory(member peerdiscovery.Node, requestedMB int64) bool {
	if requestedMB <= 0 {
		return true
	}
	total := member.Metadata.MemoryMB
	if total == 0 {
		return true
	}
	used := total * uint64(member.Metadata.MemPercent) / 100
	if used >= total {
		return false
	}
	return uint64(requestedMB) <= total-used
}

// nodePressure reduces a member's utilization trio to one number: the max,
// so a node is as loaded as its most constrained resource.
func nodePressure(member peerdiscovery.Node) uint8 {
	return max(member.Metadata.CPUPercent, member.Metadata.MemPercent, member.Metadata.DiskPercent)
}
