// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

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

	// Orphans were running somewhere and lost their owner; they go
	// first. The unassigned follow highest class first so important
	// arrivals claim room before the best-effort crowd. Both orders are
	// stable: view order decides within a class.
	pending := pendingIndexes(specs, alive)
	reserved := make(map[uuid.UUID]uint64)
	for _, i := range pending {
		schedulePending(ctx, logger, j, views, nodeID, members, specs, reserved, i)
	}
}

// pendingIndexes returns spec indexes needing placement: orphans first,
// then unassigned highest class first. Healthy, removed, and unknown
// specs never enter.
func pendingIndexes(specs []workload.Spec, alive map[uuid.UUID]struct{}) []int {
	var orphans, unassigned []int
	for i := range specs {
		spec := &specs[i]
		if spec.Removed {
			continue
		}
		if spec.AssignedNodeID == uuid.Nil {
			unassigned = append(unassigned, i)
			continue
		}
		if _, ok := alive[spec.AssignedNodeID]; !ok {
			orphans = append(orphans, i)
		}
	}
	sort.SliceStable(unassigned, func(a, b int) bool {
		return specs[unassigned[a]].QoS.Rank() > specs[unassigned[b]].QoS.Rank()
	})
	return append(orphans, unassigned...)
}

// schedulePending assigns one unassigned spec or reassigns one orphan at
// specs[i]. A fitting node takes it; a higher class with nowhere to fit
// displaces one lower victim; anything else waits for the next tick.
// Writes update specs in place so later candidates in the same tick see
// earlier placements.
func schedulePending(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	views []journalview.View,
	nodeID uuid.UUID,
	members []peerdiscovery.Node,
	specs []workload.Spec,
	reserved map[uuid.UUID]uint64,
	i int,
) {
	spec := &specs[i]
	initial := spec.AssignedNodeID == uuid.Nil

	chosenMember, ok := pickNode(members, *spec, reserved)
	if !ok {
		if tryPreempt(ctx, logger, j, views, nodeID, members, specs, reserved, i) {
			return
		}
		logger.Debug("no node fits workload, leaving for next tick",
			zap.String("workload_id", spec.ID.String()))
		return
	}
	if !initial && chosenMember.State != peerdiscovery.NodeStateAlive {
		return
	}
	spec.AssignedNodeID = chosenMember.ID
	if spec.Resources.MemoryMB > 0 {
		reserved[chosenMember.ID] += uint64(spec.Resources.MemoryMB)
	}
	if initial {
		recordAssignment(ctx, logger, j, views, nodeID, *spec, "workload.spec assigned")
		return
	}
	spec.AssignmentEpoch++
	recordAssignment(ctx, logger, j, views, nodeID, *spec, "workload.spec reassigned")
}

// tryPreempt displaces one lower-class running workload to make room for
// the unplaceable spec at specs[i]. It scans live assigned specs for the
// lowest class whose freed request covers the deficit on its node, writes
// that victim's unassignment (empty node, epoch plus one), and leaves the
// arrival for the next tick, when the freed request counts. Single
// victim, strictly downward, so displacement terminates; equal classes
// keep first-come-first-served. Memory math is approximate: node free is
// real usage, requests are reservations, and the overlap drains as the
// victim stops. It reports whether it evicted.
func tryPreempt(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	views []journalview.View,
	nodeID uuid.UUID,
	members []peerdiscovery.Node,
	specs []workload.Spec,
	reserved map[uuid.UUID]uint64,
	i int,
) bool {
	arrival := &specs[i]
	if arrival.Resources.MemoryMB <= 0 {
		return false
	}

	victim := -1
	for j := range specs {
		if !victimCovers(members, &specs[j], arrival, reserved) {
			continue
		}
		candidate := &specs[j]
		if victim < 0 || candidate.QoS.Rank() < specs[victim].QoS.Rank() ||
			(candidate.QoS.Rank() == specs[victim].QoS.Rank() && candidate.Resources.MemoryMB > specs[victim].Resources.MemoryMB) {
			victim = j
		}
	}
	if victim < 0 {
		return false
	}

	evicted := &specs[victim]
	evicted.AssignedNodeID = uuid.Nil
	evicted.AssignmentEpoch++
	recordAssignment(ctx, logger, j, views, nodeID, *evicted, "workload.spec preempted")
	return true
}

// victimCovers reports whether a live spec assigned to an alive node may
// give way to the arrival: strictly lower class, a provable request, and
// freed memory covering the deficit.
func victimCovers(members []peerdiscovery.Node, candidate, arrival *workload.Spec, reserved map[uuid.UUID]uint64) bool {
	if candidate.Removed || candidate.AssignedNodeID == uuid.Nil {
		return false
	}
	if candidate.QoS.Rank() >= arrival.QoS.Rank() {
		return false
	}
	candidateMB := candidate.Resources.MemoryMB
	arrivalMB := arrival.Resources.MemoryMB
	if candidateMB <= 0 || arrivalMB <= 0 {
		return false
	}
	node, ok := memberByID(members, candidate.AssignedNodeID)
	if !ok || node.State != peerdiscovery.NodeStateAlive {
		return false
	}
	free := effectiveFreeMemory(node, reserved[node.ID])
	return free+uint64(candidateMB) >= uint64(arrivalMB)
}

// memberByID finds a member by ID.
func memberByID(members []peerdiscovery.Node, id uuid.UUID) (peerdiscovery.Node, bool) {
	for _, member := range members {
		if member.ID == id {
			return member, true
		}
	}
	return peerdiscovery.Node{}, false
}

// nodeFreeMemory derives free megabytes from gossip totals and percents.
// Unknown capacity reports zero free; callers treat that as unmeasurable,
// never as room.
func nodeFreeMemory(member peerdiscovery.Node) uint64 {
	total := member.Metadata.MemoryMB
	if total == 0 {
		return 0
	}
	used := total * uint64(member.Metadata.MemPercent) / 100
	if used >= total {
		return 0
	}
	return total - used
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
// workload's memory request minus this tick's reservations, preferring
// healthy peers; fewest running workloads breaks pressure ties. It
// reports false when no member fits, and the caller leaves the spec for
// the next tick instead of placing where it cannot run.
func pickNode(members []peerdiscovery.Node, spec workload.Spec, reserved map[uuid.UUID]uint64) (peerdiscovery.Node, bool) {
	var best peerdiscovery.Node
	found := false

	for _, member := range members {
		if !fitsMemory(member, spec.Resources.MemoryMB, reserved[member.ID]) {
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

// fitsMemory reports whether the member has the requested megabytes free
// after this tick's reservations. No request (unlimited) always fits,
// and unknown node capacity never blocks: missing data must not strand
// workloads. CPU shares are relative weights and per-workload disk is
// unreported, so neither admits a fit check; pressure already steers off
// hot nodes.
func fitsMemory(member peerdiscovery.Node, requestedMB int64, reservedMB uint64) bool {
	if requestedMB <= 0 {
		return true
	}
	if member.Metadata.MemoryMB == 0 {
		return true
	}
	free := nodeFreeMemory(member)
	if reservedMB >= free {
		return false
	}
	return uint64(requestedMB) <= free-reservedMB
}

// effectiveFreeMemory subtracts this tick's reservations from gossip
// free, floored at zero.
func effectiveFreeMemory(member peerdiscovery.Node, reservedMB uint64) uint64 {
	free := nodeFreeMemory(member)
	if reservedMB >= free {
		return 0
	}
	return free - reservedMB
}

// nodePressure reduces a member's utilization trio to one number: the max,
// so a node is as loaded as its most constrained resource.
func nodePressure(member peerdiscovery.Node) uint8 {
	return max(member.Metadata.CPUPercent, member.Metadata.MemPercent, member.Metadata.DiskPercent)
}
