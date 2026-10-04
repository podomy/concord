// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap/zaptest"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/workload"
)

// fakeMemberSource is a test MemberSource returning fixed members or an error.
type fakeMemberSource struct {
	members []peerdiscovery.Node
	err     error
}

// Members returns the fixed members or the fixed error.
func (f fakeMemberSource) Members() ([]peerdiscovery.Node, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.members, nil
}

// recordingJournal captures appended journal events for assertions.
type recordingJournal struct {
	events []journal.Event
}

// Append records the event in memory.
func (m *recordingJournal) Append(_ context.Context, event journal.Event) error {
	m.events = append(m.events, event)
	return nil
}

// newSchedulerWorkloads opens an isolated Workloads view for scheduler tests.
func newSchedulerWorkloads(t *testing.T) *journalview.Workloads {
	t.Helper()
	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "bbolt.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("close kv: %v", err)
		}
	})
	return journalview.NewWorkloads(kv)
}

// seedSchedulerSpec writes one workload.spec event into the view.
func seedSchedulerSpec(t *testing.T, view *journalview.Workloads, spec workload.Spec) {
	t.Helper()
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	err = view.Apply(t.Context(), journal.NewEvent(uuid.New(), "workload.spec", payload))
	if err != nil {
		t.Fatal(err)
	}
}

// runScheduler runs scheduleWorkloads once and returns the recorded events.
func runScheduler(t *testing.T, view *journalview.Workloads, rec *recordingJournal, members []peerdiscovery.Node) {
	t.Helper()
	scheduleWorkloads(t.Context(), zaptest.NewLogger(t), rec, view, fakeMemberSource{members: members}, uuid.New(), []journalview.View{view})
}

// aliveSchedulerNode builds one alive member with the given workload count.
func aliveSchedulerNode(workloads int) peerdiscovery.Node {
	return peerdiscovery.Node{
		ID:    uuid.New(),
		State: peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			Workloads: workloads,
		},
	}
}

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

func TestScheduleAssignsUnassigned(t *testing.T) {
	t.Parallel()

	nodeA := aliveSchedulerNode(0)
	nodeB := aliveSchedulerNode(5)
	view := newSchedulerWorkloads(t)
	spec := workload.Spec{ID: uuid.New(), Image: "nginx:latest"}
	seedSchedulerSpec(t, view, spec)

	rec := &recordingJournal{}
	runScheduler(t, view, rec, []peerdiscovery.Node{nodeA, nodeB})

	if len(rec.events) != 1 {
		t.Fatalf("expected 1 assignment event, got %d", len(rec.events))
	}
	got, err := view.Get(t.Context(), spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected spec, got nil")
	}
	if got.AssignedNodeID != nodeA.ID {
		t.Fatalf("expected assignment to least-loaded node %v, got %v", nodeA.ID, got.AssignedNodeID)
	}
}

func TestScheduleReassignsOrphan(t *testing.T) {
	t.Parallel()

	nodeA := aliveSchedulerNode(1)
	nodeB := aliveSchedulerNode(1)
	view := newSchedulerWorkloads(t)
	deadOwner := uuid.New()
	spec := workload.Spec{ID: uuid.New(), Image: "nginx:latest", AssignedNodeID: deadOwner}
	seedSchedulerSpec(t, view, spec)

	rec := &recordingJournal{}
	runScheduler(t, view, rec, []peerdiscovery.Node{nodeA, nodeB})

	if len(rec.events) != 1 {
		t.Fatalf("expected 1 reassignment event, got %d", len(rec.events))
	}
	got, err := view.Get(t.Context(), spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected spec, got nil")
	}
	if got.AssignedNodeID == deadOwner {
		t.Fatal("expected orphan to leave dead owner")
	}
	if got.AssignedNodeID != nodeA.ID && got.AssignedNodeID != nodeB.ID {
		t.Fatalf("expected assignment to an alive node, got %v", got.AssignedNodeID)
	}
	if got.AssignmentEpoch != spec.AssignmentEpoch+1 {
		t.Fatalf("expected epoch %d, got %d", spec.AssignmentEpoch+1, got.AssignmentEpoch)
	}
}

func TestScheduleSkipsTombstone(t *testing.T) {
	t.Parallel()

	nodeA := aliveSchedulerNode(0)
	view := newSchedulerWorkloads(t)
	spec := workload.Spec{ID: uuid.New(), Image: "nginx:latest", AssignedNodeID: uuid.New(), Removed: true}
	seedSchedulerSpec(t, view, spec)

	rec := &recordingJournal{}
	runScheduler(t, view, rec, []peerdiscovery.Node{nodeA})

	if len(rec.events) != 0 {
		t.Fatalf("expected no events for tombstone, got %d", len(rec.events))
	}
}

func TestScheduleSkipsAliveOwner(t *testing.T) {
	t.Parallel()

	nodeA := aliveSchedulerNode(0)
	nodeB := aliveSchedulerNode(0)
	view := newSchedulerWorkloads(t)
	spec := workload.Spec{ID: uuid.New(), Image: "nginx:latest", AssignedNodeID: nodeA.ID}
	seedSchedulerSpec(t, view, spec)

	rec := &recordingJournal{}
	runScheduler(t, view, rec, []peerdiscovery.Node{nodeA, nodeB})

	if len(rec.events) != 0 {
		t.Fatalf("expected no events for alive owner, got %d", len(rec.events))
	}
	got, err := view.Get(t.Context(), spec.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.AssignedNodeID != nodeA.ID {
		t.Fatalf("expected assignment to stay %v, got %+v", nodeA.ID, got)
	}
}

func TestScheduleSkipsWhenNoAlive(t *testing.T) {
	t.Parallel()

	dead := peerdiscovery.Node{ID: uuid.New(), State: peerdiscovery.NodeStateDead}
	view := newSchedulerWorkloads(t)
	spec := workload.Spec{ID: uuid.New(), Image: "nginx:latest"}
	seedSchedulerSpec(t, view, spec)

	rec := &recordingJournal{}
	runScheduler(t, view, rec, []peerdiscovery.Node{dead})

	if len(rec.events) != 0 {
		t.Fatalf("expected no events without alive nodes, got %d", len(rec.events))
	}
}
