// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/workload"
)

// seedEvictionWave writes two assigned best-effort occupants plus two
// unassigned guaranteed arrivals on a node too full for any of them.
func seedEvictionWave(t *testing.T, view *journalview.Workloads, node peerdiscovery.Node) (victims, arrivals [2]workload.Spec) {
	t.Helper()
	victims = [2]workload.Spec{
		{ID: uuid.New(), Image: "x", AssignedNodeID: node.ID, QoS: workload.QoSBestEffort, Resources: workload.Resources{MemoryMB: 800}},
		{ID: uuid.New(), Image: "x", AssignedNodeID: node.ID, QoS: workload.QoSBestEffort, Resources: workload.Resources{MemoryMB: 800}},
	}
	arrivals = [2]workload.Spec{
		{ID: uuid.New(), Image: "x", QoS: workload.QoSGuaranteed, Resources: workload.Resources{MemoryMB: 800}},
		{ID: uuid.New(), Image: "x", QoS: workload.QoSGuaranteed, Resources: workload.Resources{MemoryMB: 800}},
	}
	for _, spec := range victims {
		seedSchedulerSpec(t, view, spec)
	}
	for _, spec := range arrivals {
		seedSchedulerSpec(t, view, spec)
	}
	return victims, arrivals
}

// fullNode returns one alive node with ~100MB free of 1024MB: room for
// nothing arriving at 800MB, but one 800MB victim covers any arrival.
func fullNode() peerdiscovery.Node {
	return peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			MemoryMB:   1024,
			MemPercent: 90,
		},
	}
}

// One arrival wave evicts at most one victim per tick: the second arrival
// waits even though a second victim could cover it.
func TestScheduleCapsEvictionsPerTick(t *testing.T) {
	t.Parallel()

	node := fullNode()
	view := newSchedulerWorkloads(t)
	victims, arrivals := seedEvictionWave(t, view, node)

	rec := &recordingJournal{}
	runScheduler(t, view, rec, []peerdiscovery.Node{node})

	if len(rec.events) != 1 {
		t.Fatalf("events = %d, want exactly 1 eviction per tick", len(rec.events))
	}
	for _, a := range arrivals {
		got, err := view.Get(t.Context(), a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.AssignedNodeID != uuid.Nil {
			t.Fatalf("arrival placed despite spent budget: %+v", got)
		}
	}
	evicted := 0
	for _, v := range victims {
		got, err := view.Get(t.Context(), v.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil && got.AssignedNodeID == uuid.Nil {
			evicted++
		}
	}
	if evicted != 1 {
		t.Fatalf("evicted victims = %d, want 1", evicted)
	}
}

// The budget resets every tick: a second tick evicts the second victim
// for the still-waiting arrival.
func TestScheduleEvictionBudgetResetsPerTick(t *testing.T) {
	t.Parallel()

	node := fullNode()
	view := newSchedulerWorkloads(t)
	_, arrivals := seedEvictionWave(t, view, node)

	rec := &recordingJournal{}
	runScheduler(t, view, rec, []peerdiscovery.Node{node})
	runScheduler(t, view, rec, []peerdiscovery.Node{node})

	if len(rec.events) != 2 {
		t.Fatalf("events = %d, want 1 eviction per tick over 2 ticks", len(rec.events))
	}
	for _, a := range arrivals {
		got, err := view.Get(t.Context(), a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got == nil || got.AssignedNodeID != uuid.Nil {
			t.Fatalf("arrival placed too early: %+v", got)
		}
	}
}

// A victim on eviction cooldown is skipped even when it is the only
// cover; past the cooldown it evicts normally again.
func TestTryPreemptSkipsCooldownVictim(t *testing.T) {
	t.Parallel()

	node := fullNode()
	view := newSchedulerWorkloads(t)
	victim := workload.Spec{ID: uuid.New(), Image: "x", AssignedNodeID: node.ID, QoS: workload.QoSBestEffort, Resources: workload.Resources{MemoryMB: 800}}
	arrival := workload.Spec{ID: uuid.New(), Image: "x", QoS: workload.QoSGuaranteed, Resources: workload.Resources{MemoryMB: 800}}
	specs := []workload.Spec{victim, arrival}
	views := []journalview.View{view}
	logger := logs.NewTest(t)
	nodeID := uuid.New()

	recordEviction(victim.ID)
	if !onEvictCooldown(victim.ID) {
		t.Fatal("expected victim on cooldown right after eviction")
	}
	rec := &recordingJournal{}
	if tryPreempt(t.Context(), logger, rec, views, nodeID, []peerdiscovery.Node{node}, specs, map[uuid.UUID]uint64{}, 1) {
		t.Fatal("preempted a victim on cooldown")
	}
	if len(rec.events) != 0 {
		t.Fatalf("events = %d, want none while victim cools down", len(rec.events))
	}

	for range evictCooldownTicks {
		runScheduler(t, view, rec, []peerdiscovery.Node{aliveSchedulerNode(0)})
	}
	if onEvictCooldown(victim.ID) {
		t.Fatal("cooldown did not expire after cooldown ticks")
	}
	if !tryPreempt(t.Context(), logger, rec, views, nodeID, []peerdiscovery.Node{node}, specs, map[uuid.UUID]uint64{}, 1) {
		t.Fatal("expected preemption past the cooldown")
	}
}
