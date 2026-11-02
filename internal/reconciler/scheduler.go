// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

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
// At most maxEvictionsPerTick preemptions happen per call; the rest wait
// for the next tick so one arrival wave cannot evict the whole node.
func scheduleWorkloads(
	ctx context.Context,
	logger *logs.Logger,
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
		logger.Error("peerservice members", logs.Error(err))
		return
	}
	if len(members) == 0 {
		return
	}

	specs, err := workloads.List(ctx)
	if err != nil {
		logger.Error("workloads list", logs.Error(err))
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
	advanceEvictTick()
	pending := pendingIndexes(specs, alive)
	reserved := make(map[uuid.UUID]uint64)
	evictions := 0
	for _, i := range pending {
		schedulePending(ctx, logger, j, views, nodeID, members, specs, reserved, i, &evictions)
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
// displaces one lower victim while the per-tick eviction budget lasts;
// anything else waits for the next tick.
// Writes update specs in place so later candidates in the same tick see
// earlier placements.
func schedulePending(
	ctx context.Context,
	logger *logs.Logger,
	j journal.Journal,
	views []journalview.View,
	nodeID uuid.UUID,
	members []peerdiscovery.Node,
	specs []workload.Spec,
	reserved map[uuid.UUID]uint64,
	i int,
	evictions *int,
) {
	spec := &specs[i]
	initial := spec.AssignedNodeID == uuid.Nil

	chosenMember, ok := pickNode(members, *spec, reserved)
	if !ok {
		if *evictions >= maxEvictionsPerTick {
			logger.Debug("preemption budget spent, leaving for next tick",
				logs.String("workload_id", spec.ID.String()))
			return
		}
		if tryPreempt(ctx, logger, j, views, nodeID, members, specs, reserved, i) {
			*evictions++
			return
		}
		logger.Debug("no node fits workload, leaving for next tick",
			logs.String("workload_id", spec.ID.String()))
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

// maxEvictionsPerTick caps preemption evictions per scheduler tick. One
// eviction frees one victim's request, which the arrival claims on the
// next tick; allowing more would let one arrival wave evict a whole node
// before any freed request counts. The rest wait for later ticks.
const maxEvictionsPerTick = 1

// evictCooldownTicks bars an evicted workload from re-eviction for this
// many scheduler ticks. An evicted workload is unassigned, so it cannot
// be a victim again until it is placed and running; the cooldown covers
// the flapping window where it oscillates between nodes.
const evictCooldownTicks = 3

// evictTracker clocks scheduler ticks and remembers recent evictions so
// the same workload is not re-evicted while it flaps. Keyed by workload
// ID, so parallel unit tests with fresh IDs never interfere. All access
// holds the mutex; scheduleWorkloads runs single-threaded.
var evictTracker = struct {
	sync.Mutex
	tick uint64
	last map[uuid.UUID]uint64
}{last: make(map[uuid.UUID]uint64)}

// advanceEvictTick moves the scheduler clock one tick and prunes expired
// cooldowns. Call once per scheduleWorkloads.
func advanceEvictTick() {
	evictTracker.Lock()
	defer evictTracker.Unlock()
	evictTracker.tick++
	for id, at := range evictTracker.last {
		if evictTracker.tick-at >= evictCooldownTicks {
			delete(evictTracker.last, id)
		}
	}
}

// onEvictCooldown reports whether id was evicted within the cooldown.
func onEvictCooldown(id uuid.UUID) bool {
	evictTracker.Lock()
	defer evictTracker.Unlock()
	_, ok := evictTracker.last[id]
	return ok
}

// recordEviction stamps id with the current tick.
func recordEviction(id uuid.UUID) {
	evictTracker.Lock()
	defer evictTracker.Unlock()
	evictTracker.last[id] = evictTracker.tick
}

// tryPreempt displaces one lower-class running workload to make room for
// the unplaceable spec at specs[i]. It scans live assigned specs for the
// lowest class whose freed request covers the deficit on its node, writes
// that victim's unassignment (empty node, epoch plus one), and leaves the
// arrival for the next tick, when the freed request counts. Single
// victim, strictly downward, so displacement terminates; equal classes
// keep first-come-first-served. Victims on eviction cooldown are skipped.
// Memory math is approximate: node free is real usage, requests are
// reservations, and the overlap drains as the victim stops. It reports
// whether it evicted.
func tryPreempt(
	ctx context.Context,
	logger *logs.Logger,
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
	var victimSpec *workload.Spec
	for j := range specs {
		if onEvictCooldown(specs[j].ID) {
			continue
		}
		if !victimCovers(members, &specs[j], arrival, reserved) {
			continue
		}
		if victim < 0 || isBetterVictim(&specs[j], victimSpec) {
			victim = j
			victimSpec = &specs[j]
		}
	}
	if victim < 0 {
		return false
	}

	evicted := &specs[victim]
	recordEviction(evicted.ID)
	evicted.AssignedNodeID = uuid.Nil
	evicted.AssignmentEpoch++
	recordAssignment(ctx, logger, j, views, nodeID, *evicted, "workload.spec preempted")
	return true
}

// isBetterVictim reports whether candidate displaces current as the
// eviction victim: strictly lower class wins; within a class the larger
// request frees more room.
func isBetterVictim(candidate, current *workload.Spec) bool {
	if candidate.QoS.Rank() != current.QoS.Rank() {
		return candidate.QoS.Rank() < current.QoS.Rank()
	}
	return candidate.Resources.MemoryMB > current.Resources.MemoryMB
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
func recordAssignment(ctx context.Context, logger *logs.Logger, j journal.Journal, views []journalview.View, nodeID uuid.UUID, spec workload.Spec, message string) {
	payload, err := json.Marshal(spec)
	if err != nil {
		logger.Error("json marshal", logs.Error(err))
		return
	}

	event := journal.NewEvent(nodeID, journalview.EventTypeWorkloadSpec, payload)
	err = journalview.RecordEventAndLog(ctx, logger, j, views, event, message,
		logs.String("workload_id", spec.ID.String()),
		logs.String("assigned_node", spec.AssignedNodeID.String()),
	)
	if err != nil {
		logger.Error("record workload.spec event", logs.Error(err))
	}
}

// pickNode selects the alive peer with the lowest pressure that fits the
// workload's memory request minus this tick's reservations, preferring
// healthy peers; fewest running workloads breaks pressure ties, and the
// lowest node ID string breaks the rest so the choice is deterministic
// across observers regardless of member order. Non-alive members never
// place: initial assignments and orphan reassignments alike wait for an
// alive node instead of stranding a workload where it cannot run. It
// reports false when no member fits, and the caller leaves the spec for
// the next tick instead of placing where it cannot run.
//
// Placement policy, kept stable: a measured node is preferred at any
// load, even 95 percent, over an unknown-pressure (fresh-boot gossip
// trio all zero) node; unknown nodes deprioritize but never exclude, so
// a fleet of only unknowns still places by workload count. Unknown
// capacity (MemoryMB 0) never blocks: missing data must not strand
// workloads. There are no taints or tolerations yet: pressure steers,
// fit gates, nothing else filters.
func pickNode(members []peerdiscovery.Node, spec workload.Spec, reserved map[uuid.UUID]uint64) (peerdiscovery.Node, bool) {
	var best peerdiscovery.Node
	found := false

	for _, member := range members {
		// Liveness gates every placement, not just orphans: a dead or
		// suspect node cannot run the workload however empty it looks.
		if member.State != peerdiscovery.NodeStateAlive {
			continue
		}
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
// order: alive over non-alive, known pressure over unknown, lowest
// pressure, fewest workloads, lowest node ID string. An all-zero CPU/Mem/Disk triple means
// fresh-boot gossip with no samples yet: it deprioritizes below any
// measured node but never excludes, so unknown nodes still place when
// they are the only candidates. If all candidates are unknown, pressure
// ties and the workload count decides among them. The final ID tiebreak
// keeps the pick identical for every observer even when memberlists
// arrive in different orders.
func prefersNode(member, best peerdiscovery.Node) bool {
	if member.State == peerdiscovery.NodeStateAlive && best.State != peerdiscovery.NodeStateAlive {
		return true
	}
	if member.State != best.State {
		return false
	}
	memberUnknown := isUnknownPressure(member.Metadata)
	bestUnknown := isUnknownPressure(best.Metadata)
	if memberUnknown != bestUnknown {
		return !memberUnknown
	}
	memberPressure, bestPressure := nodePressure(member), nodePressure(best)
	if memberPressure != bestPressure {
		return memberPressure < bestPressure
	}
	if member.Metadata.Workloads != best.Metadata.Workloads {
		return member.Metadata.Workloads < best.Metadata.Workloads
	}
	return member.ID.String() < best.ID.String()
}

// isUnknownPressure reports whether the gossip trio carries no samples
// yet. Fresh-boot nodes gossip zeros until the fast beat publishes its
// first reading; zeros then mean unknown, not idle.
func isUnknownPressure(m peerdiscovery.NodeMetadata) bool {
	return m.CPUPercent == 0 && m.MemPercent == 0 && m.DiskPercent == 0
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
