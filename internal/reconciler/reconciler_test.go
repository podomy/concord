// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/clock"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/proc"
	"github.com/podomy/concord/internal/workload"
)

// Unit tests for reconciler error paths: stage failure, start failure,
// wrong-node skip, and already-running skip.
// Happy-path process lifecycle tests use the fake launcher, no host
// binaries, no root.

// testFileDigest is the canned blob digest staged by stubStageFetch.
const testFileDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// testFileDigestV2 is a second canned digest for drift tests.
const testFileDigestV2 = "sha256:fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"

// binSpec builds a runnable spec: one staged binary plus command. Image
// stays populated as an opaque label until validation drops the
// requirement; the raw path never pulls it.
func binSpec(id uuid.UUID) workload.Spec {
	return workload.Spec{
		ID:      id,
		Image:   "raw:local",
		Command: []string{"/app/bin"},
		Files: []workload.FileRef{
			{Path: "/app/bin", Digest: testFileDigest, Mode: 0o755},
		},
	}
}

// stubStageFetch replaces blob fetch with canned bytes for the test.
func stubStageFetch(t *testing.T) {
	t.Helper()
	old := stageFetch
	stageFetch = func(_ context.Context, _, dest string) error {
		return os.WriteFile(dest, []byte("#!/bin/sh\n"), 0o755) //nolint:gosec // test stages an executable fixture
	}
	t.Cleanup(func() { stageFetch = old })
}

type mockJournal struct{}

func (mockJournal) Append(ctx context.Context, event journal.Event) error { return nil }
func (mockJournal) Close() error                                          { return nil }

// mockHandle implements proc.Handle with a scripted pid, recorded
// signals, and a controllable exit channel.
type mockHandle struct {
	pid     int
	signals []os.Signal
	exited  chan proc.ExitStatus
}

func (m *mockHandle) PID() int {
	if m.pid != 0 {
		return m.pid
	}
	return 1234
}

func (m *mockHandle) Signal(sig os.Signal) error {
	m.signals = append(m.signals, sig)
	return nil
}

func (m *mockHandle) Exited() <-chan proc.ExitStatus { return m.exited }

func TestReconcilerStageError(t *testing.T) {
	_, workloads, running := setupReconcilerTest(t)
	id := uuid.New()
	writeSpecEvent(t, workloads, binSpec(id), uuid.Nil)
	old := stageFetch
	stageFetch = func(_ context.Context, _, _ string) error {
		return errors.New("blob missing")
	}
	t.Cleanup(func() { stageFetch = old })
	launcher := &proc.FakeLauncher{}

	exitEvents := make(chan ExitEvent, 100)
	reconcileTick(t.Context(), logs.NewTest(t), uuid.Nil, launcher, &mockJournal{}, workloads, running, map[uuid.UUID]int{}, exitEvents, nil, nil, nil)

	if launcher.Calls() != 0 {
		t.Error("launcher started despite stage failure")
	}
	if _, ok := running[id]; ok {
		t.Error("entry recorded despite stage failure")
	}
}

func TestReconcilerStartError(t *testing.T) {
	_, workloads, running := setupReconcilerTest(t)
	id := uuid.New()
	writeSpecEvent(t, workloads, binSpec(id), uuid.Nil)
	launcher := &proc.FakeLauncher{StartErr: errors.New("spawn failed")}

	runTickWith(t, launcher, workloads, running)

	if _, ok := running[id]; ok {
		t.Error("entry recorded despite start failure")
	}
}

func TestReconcilerSkipsWrongNode(t *testing.T) {
	_, workloads, running := setupReconcilerTest(t)
	writeSpecEvent(t, workloads, workload.Spec{Image: "nginx:latest", ID: uuid.New(), AssignedNodeID: uuid.New()}, uuid.New())
	launcher := &proc.FakeLauncher{}

	runTickWith(t, launcher, workloads, running)

	if launcher.Calls() != 0 {
		t.Error("launcher started a workload belonging to another node")
	}
}

func TestReconcilerSkipsAlreadyRunning(t *testing.T) {
	_, workloads, running := setupReconcilerTest(t)
	spec := binSpec(uuid.New())
	writeSpecEvent(t, workloads, spec, uuid.Nil)
	running[spec.ID] = nil
	launcher := &proc.FakeLauncher{}

	runTickWith(t, launcher, workloads, running)

	if launcher.Calls() != 0 {
		t.Error("launcher started an already-running workload")
	}
}

func setupReconcilerTest(t *testing.T) (*proc.FakeLauncher, *journalview.Workloads, map[uuid.UUID]*WorkloadEntry) {
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
	return &proc.FakeLauncher{},
		journalview.NewWorkloads(kv),
		map[uuid.UUID]*WorkloadEntry{}
}

// runTickWith runs one reconcileTick with staged-blob stubbing, so starts
// proceed without a registry.
func runTickWith(t *testing.T, launcher *proc.FakeLauncher, workloads *journalview.Workloads, running map[uuid.UUID]*WorkloadEntry) {
	t.Helper()
	stubStageFetch(t)
	exitEvents := make(chan ExitEvent, 100)
	reconcileTick(t.Context(), logs.NewTest(t), uuid.Nil, launcher, &mockJournal{}, workloads, running, map[uuid.UUID]int{}, exitEvents, nil, nil, nil)
}

func writeSpecEvent(t *testing.T, workloads *journalview.Workloads, spec workload.Spec, nodeID uuid.UUID) {
	t.Helper()
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := workloads.Apply(t.Context(), journal.NewEvent(nodeID, journalview.EventTypeWorkloadSpec, payload)); err != nil {
		t.Fatal(err)
	}
}

// TestReconcilerDoesNotStartTombstonedWorkload verifies removed specs are not started.
func TestReconcilerDoesNotStartTombstonedWorkload(t *testing.T) {
	_, workloads, running := setupReconcilerTest(t)

	spec := binSpec(uuid.New())
	writeSpecEvent(t, workloads, spec, uuid.Nil)

	spec.Removed = true
	writeSpecEvent(t, workloads, spec, uuid.Nil)
	launcher := &proc.FakeLauncher{}

	runTickWith(t, launcher, workloads, running)

	if launcher.Calls() != 0 {
		t.Fatal("launcher started a tombstoned workload")
	}
}

func TestReconcilerDestroyProcessAsync(t *testing.T) {
	spec := workload.Spec{ID: uuid.New()}
	exitedChan := make(chan proc.ExitStatus, 1)
	handle := &mockHandle{exited: exitedChan}
	entry := &WorkloadEntry{
		Spec:   spec,
		Handle: handle,
	}
	running := map[uuid.UUID]*WorkloadEntry{spec.ID: entry}
	exitEvents := make(chan ExitEvent, 1)

	go monitorProcessExit(t.Context(), spec.ID, handle, exitEvents)

	destroyContainer(logs.NewTest(t), spec, running)

	if !entry.Stopping {
		t.Error("expected entry.Stopping to be true")
	}
	if len(handle.signals) != 1 || handle.signals[0] != syscall.SIGTERM {
		t.Errorf("expected SIGTERM signal, got %v", handle.signals)
	}

	// Send exit notification.
	exitedChan <- proc.ExitStatus{Code: 0}

	select {
	case ev := <-exitEvents:
		if ev.WorkloadID != spec.ID {
			t.Errorf("expected event for spec %v, got %v", spec.ID, ev.WorkloadID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for exit event")
	}
}

func TestReconcilerHandleExitEvent(t *testing.T) {
	spec := workload.Spec{ID: uuid.New()}
	entry := &WorkloadEntry{
		Spec:     spec,
		Stopping: true,
	}
	running := map[uuid.UUID]*WorkloadEntry{spec.ID: entry}

	handleExitEvent(t.Context(), logs.NewTest(t), &mockJournal{}, uuid.Nil, spec.ID, proc.ExitStatus{Code: 0}, running, nil, nil)

	if entry.ExitStatus == nil {
		t.Error("expected entry.ExitStatus to be non-nil after handleExitEvent")
	}
}

// Instance events reach live views, not just journal bytes: previously
// they appended without applying, so generic indexes missed them until
// the next rebuild.
// Instance events reach live views, not just journal bytes: previously
// they appended without applying, so generic indexes missed them until
// the next rebuild.
func TestRecordInstanceEventAppliesViews(t *testing.T) {
	ctx := context.Background()
	j, err := journal.OpenJSONLPath(filepath.Join(t.TempDir(), "journal.jsonl"))
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	t.Cleanup(func() {
		if err := j.Close(); err != nil {
			t.Errorf("close journal: %v", err)
		}
	})
	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Errorf("close kv: %v", err)
		}
	})

	eventsByID := journalview.NewEventsByID(kv)
	views := []journalview.View{eventsByID}
	spec := workload.Spec{ID: uuid.New()}

	recordInstanceEvent(ctx, logs.NewTest(t), j, views, spec, uuid.New(), workload.StateRunning, 123)

	list, err := eventsByID.List(ctx)
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	found := false
	for _, ev := range list {
		if ev.Type == journalview.EventTypeWorkloadInstancePrefix+"running" {
			found = true
		}
	}
	if !found {
		t.Fatal("instance event missing from live view")
	}
}

// Start config carries the spec command wholesale: no image fallback
// exists on the host path.
func TestBuildStartConfigUsesSpecCommand(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), Command: []string{"/bin/sh", "-c", "x"}}

	cfg := buildStartConfig(spec, "/stage/app/bin", node.NewSampler())
	if len(cfg.Args) != 3 || cfg.Args[0] != "/bin/sh" {
		t.Fatalf("args = %v, want spec command", cfg.Args)
	}
	if cfg.BinPath != "/stage/app/bin" {
		t.Fatalf("bin = %q", cfg.BinPath)
	}
	if cfg.Stdout == nil || cfg.Stderr == nil {
		t.Fatal("expected log writers attached")
	}
}

// A nil sampler leaves writers unset and output discards as before. The
// scheduled memory request rides along as an env var.
func TestBuildStartConfigWithoutSampler(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), Resources: workload.Resources{MemoryMB: 128}}

	cfg := buildStartConfig(spec, "/stage/app/bin", nil)
	if cfg.Stdout != nil || cfg.Stderr != nil {
		t.Fatal("expected unset writers without sampler")
	}
	found := false
	for _, kv := range cfg.Env {
		if kv == "CONCORD_MEM_LIMIT_MB=128" {
			found = true
		}
	}
	if !found {
		t.Fatalf("env = %v, want memory limit", cfg.Env)
	}
	if cfg.Dir != "/stage/app" {
		t.Fatalf("dir = %q, want stage dir", cfg.Dir)
	}
}

// A dependent waits while its same-node dependency is down, and starts
// once it runs. The exited dependency itself never restarts here
// (RestartNever), isolating the gate signal.
func TestReconcileWaitsOnDependencies(t *testing.T) {
	launcher, workloads, running := setupReconcilerTest(t)

	dep := binSpec(uuid.New())
	dep.Restart = workload.RestartNever
	sub := binSpec(uuid.New())
	sub.DependsOn = []uuid.UUID{dep.ID}
	writeSpecEvent(t, workloads, dep, uuid.Nil)
	writeSpecEvent(t, workloads, sub, uuid.Nil)

	running[dep.ID] = &WorkloadEntry{Spec: dep, ExitStatus: &proc.ExitStatus{Code: 0}}
	runTickWith(t, launcher, workloads, running)
	if launcher.Calls() != 0 {
		t.Fatal("dependent started while dependency down")
	}

	running[dep.ID] = &WorkloadEntry{Spec: dep}
	runTickWith(t, launcher, workloads, running)
	if launcher.Calls() == 0 {
		t.Fatal("dependent did not start once dependency ran")
	}
}

// Tombstoned, unknown, and elsewhere-assigned dependencies never gate:
// only a live same-node dependency waits.
func TestReconcileIgnoresIrrelevantDependencies(t *testing.T) {
	for _, tc := range []struct {
		name string
		dep  workload.Spec
	}{
		{"tombstoned", workload.Spec{ID: uuid.New(), Removed: true}},
		{"unassigned-elsewhere", workload.Spec{ID: uuid.New(), AssignedNodeID: uuid.New()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			launcher, workloads, running := setupReconcilerTest(t)
			writeSpecEvent(t, workloads, tc.dep, uuid.Nil)
			sub := binSpec(uuid.New())
			sub.DependsOn = []uuid.UUID{tc.dep.ID}
			writeSpecEvent(t, workloads, sub, uuid.Nil)

			runTickWith(t, launcher, workloads, running)
			if launcher.Calls() == 0 {
				t.Fatal("dependent stalled on irrelevant dependency")
			}
		})
	}

	// Unknown IDs are ignored the same way.
	launcher, workloads, running := setupReconcilerTest(t)
	sub := binSpec(uuid.New())
	sub.DependsOn = []uuid.UUID{uuid.New()}
	writeSpecEvent(t, workloads, sub, uuid.Nil)
	runTickWith(t, launcher, workloads, running)
	if launcher.Calls() == 0 {
		t.Fatal("dependent stalled on unknown dependency")
	}
}

func TestReconcilerStopTimeoutFromSpec(t *testing.T) {
	specDefault := workload.Spec{ID: uuid.New()}
	specCustom := workload.Spec{ID: uuid.New(), StopTimeoutSeconds: 15}

	if timeout := stopTimeout(specDefault); timeout != 60*time.Second {
		t.Errorf("expected default timeout 60s, got %v", timeout)
	}
	if timeout := stopTimeout(specCustom); timeout != 15*time.Second {
		t.Errorf("expected custom timeout 15s, got %v", timeout)
	}
}

func TestReconcilerTickRestartPolicies(t *testing.T) {
	stubStageFetch(t)
	// Test RestartNever: exited workload should not be restarted.
	specNever := binSpec(uuid.New())
	specNever.Restart = workload.RestartNever
	entryNever := &WorkloadEntry{Spec: specNever, ExitStatus: &proc.ExitStatus{Code: 0}}
	runningNever := map[uuid.UUID]*WorkloadEntry{specNever.ID: entryNever}
	launcherNever := &proc.FakeLauncher{}

	reconcileWorkloadSpec(t.Context(), logs.NewTest(t), uuid.Nil, launcherNever, &mockJournal{}, specNever, runningNever, nil, nil, nil, nil, nil, nil)
	if _, exists := runningNever[specNever.ID]; !exists {
		t.Error("expected RestartNever workload entry to be retained in running map")
	}

	// Test RestartOnFailure with clean exit (0): should not restart.
	specOnFailClean := binSpec(uuid.New())
	specOnFailClean.Restart = workload.RestartOnFailure
	entryOnFailClean := &WorkloadEntry{Spec: specOnFailClean, ExitStatus: &proc.ExitStatus{Code: 0}}
	runningOnFailClean := map[uuid.UUID]*WorkloadEntry{specOnFailClean.ID: entryOnFailClean}
	launcherClean := &proc.FakeLauncher{}

	reconcileWorkloadSpec(t.Context(), logs.NewTest(t), uuid.Nil, launcherClean, &mockJournal{}, specOnFailClean, runningOnFailClean, nil, nil, nil, nil, nil, nil)
	if _, exists := runningOnFailClean[specOnFailClean.ID]; !exists {
		t.Error("expected RestartOnFailure with clean exit to be retained in running map")
	}

	// Test RestartAlways: exited workload entry should be cleared and restarted.
	specAlways := binSpec(uuid.New())
	specAlways.Restart = workload.RestartAlways
	entryAlways := &WorkloadEntry{Spec: specAlways, ExitStatus: &proc.ExitStatus{Code: 0}}
	runningAlways := map[uuid.UUID]*WorkloadEntry{specAlways.ID: entryAlways}
	launcherAlways := &proc.FakeLauncher{}

	reconcileWorkloadSpec(t.Context(), logs.NewTest(t), uuid.Nil, launcherAlways, &mockJournal{}, specAlways, runningAlways, nil, nil, nil, nil, nil, nil)
	if launcherAlways.Calls() == 0 {
		t.Error("expected RestartAlways workload to trigger start on tick")
	}
}

// Restart policy pins beyond the base table: OnFailure restarts non-zero
// codes and transport errors, unknown policies never restart, and a future
// restartAfter cools down even RestartAlways. Semantics unchanged.
func TestReconcilerTickRestartPoliciesExtended(t *testing.T) {
	stubStageFetch(t)
	// OnFailure with a non-zero exit code restarts.
	specFail := binSpec(uuid.New())
	specFail.Restart = workload.RestartOnFailure
	runningFail := map[uuid.UUID]*WorkloadEntry{specFail.ID: {Spec: specFail, ExitStatus: &proc.ExitStatus{Code: 1}}}
	launcherFail := &proc.FakeLauncher{}
	reconcileWorkloadSpec(t.Context(), logs.NewTest(t), uuid.Nil, launcherFail, &mockJournal{}, specFail, runningFail, nil, nil, nil, nil, nil, nil)
	if launcherFail.Calls() == 0 {
		t.Error("expected RestartOnFailure with code 1 to restart")
	}

	// OnFailure with a clean code but a transport error restarts.
	specErr := binSpec(uuid.New())
	specErr.Restart = workload.RestartOnFailure
	runningErr := map[uuid.UUID]*WorkloadEntry{specErr.ID: {Spec: specErr, ExitStatus: &proc.ExitStatus{Code: 0, Err: errors.New("oom")}}}
	launcherErr := &proc.FakeLauncher{}
	reconcileWorkloadSpec(t.Context(), logs.NewTest(t), uuid.Nil, launcherErr, &mockJournal{}, specErr, runningErr, nil, nil, nil, nil, nil, nil)
	if launcherErr.Calls() == 0 {
		t.Error("expected RestartOnFailure with Err to restart")
	}

	// An unknown policy string never restarts, even on failure.
	specUnknown := binSpec(uuid.New())
	specUnknown.Restart = workload.RestartPolicy("sometimes")
	runningUnknown := map[uuid.UUID]*WorkloadEntry{specUnknown.ID: {Spec: specUnknown, ExitStatus: &proc.ExitStatus{Code: 1}}}
	launcherUnknown := &proc.FakeLauncher{}
	reconcileWorkloadSpec(t.Context(), logs.NewTest(t), uuid.Nil, launcherUnknown, &mockJournal{}, specUnknown, runningUnknown, nil, nil, nil, nil, nil, nil)
	if launcherUnknown.Calls() != 0 {
		t.Error("expected unknown restart policy to stay stopped")
	}
	if _, exists := runningUnknown[specUnknown.ID]; !exists {
		t.Error("expected unknown-policy entry to be retained in running map")
	}

	// A future restartAfter cools down even RestartAlways.
	specCool := binSpec(uuid.New())
	specCool.Restart = workload.RestartAlways
	runningCool := map[uuid.UUID]*WorkloadEntry{specCool.ID: {Spec: specCool, ExitStatus: &proc.ExitStatus{Code: 0}, restartAfter: clock.Now().Add(time.Hour)}}
	launcherCool := &proc.FakeLauncher{}
	reconcileWorkloadSpec(t.Context(), logs.NewTest(t), uuid.Nil, launcherCool, &mockJournal{}, specCool, runningCool, nil, nil, nil, nil, nil, nil)
	if launcherCool.Calls() != 0 {
		t.Error("expected cooling-down workload to wait, not restart")
	}
	if _, exists := runningCool[specCool.ID]; !exists {
		t.Error("expected cooling-down entry to be retained in running map")
	}

	// A past restartAfter releases the same workload.
	runningCool[specCool.ID].restartAfter = clock.Now().Add(-time.Second)
	launcherWarm := &proc.FakeLauncher{}
	reconcileWorkloadSpec(t.Context(), logs.NewTest(t), uuid.Nil, launcherWarm, &mockJournal{}, specCool, runningCool, nil, nil, nil, nil, nil, nil)
	if launcherWarm.Calls() == 0 {
		t.Error("expected cooled-down workload to restart")
	}
}

// A negative stop timeout is not a negative timer: it falls back to the
// 60s default like an unset one.
func TestReconcilerStopTimeoutNegativeDefaults(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), StopTimeoutSeconds: -5}
	if timeout := stopTimeout(spec); timeout != 60*time.Second {
		t.Errorf("expected default timeout 60s, got %v", timeout)
	}
}

// A dependency stall counts ticks and reports once at the threshold: one
// warn plus one journal edge, never an auto-break. Later ticks stay quiet
// until progress resets the count.
func TestNoteBlockedTickStallThreshold(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), DependsOn: []uuid.UUID{uuid.New()}}
	blocked := map[uuid.UUID]int{}
	rec := &recordingJournal{}
	logger := logs.NewTest(t)
	nodeID := uuid.New()

	for i := 1; i < blockedTickStallThreshold; i++ {
		noteBlockedTick(t.Context(), logger, rec, nil, nodeID, spec, blocked)
	}
	if blocked[spec.ID] != blockedTickStallThreshold-1 {
		t.Fatalf("blocked ticks = %d, want %d", blocked[spec.ID], blockedTickStallThreshold-1)
	}
	if len(rec.events) != 0 {
		t.Fatalf("reported %d events before threshold, want 0", len(rec.events))
	}

	noteBlockedTick(t.Context(), logger, rec, nil, nodeID, spec, blocked)
	if blocked[spec.ID] != blockedTickStallThreshold {
		t.Fatalf("blocked ticks = %d, want %d", blocked[spec.ID], blockedTickStallThreshold)
	}
	if len(rec.events) != 1 {
		t.Fatalf("events = %d, want 1 stall edge", len(rec.events))
	}

	noteBlockedTick(t.Context(), logger, rec, nil, nodeID, spec, blocked)
	if len(rec.events) != 1 {
		t.Fatalf("events = %d, want edge to stay at 1 past threshold", len(rec.events))
	}

	// A nil counter is a no-op for callers without dependency tracking.
	noteBlockedTick(t.Context(), logger, rec, nil, nodeID, spec, nil)
	if len(rec.events) != 1 {
		t.Fatalf("events = %d, want nil map to record nothing", len(rec.events))
	}
}

// Stall counters reset when dependencies resolve and prune with the view:
// a removed workload never carries a stale count into a future same-ID spec.
func TestBlockedCountersResetAndPrune(t *testing.T) {
	stubStageFetch(t)
	launcher, workloads, running := setupReconcilerTest(t)
	dep := binSpec(uuid.New())
	sub := binSpec(uuid.New())
	sub.DependsOn = []uuid.UUID{dep.ID}
	writeSpecEvent(t, workloads, dep, uuid.Nil)
	writeSpecEvent(t, workloads, sub, uuid.Nil)
	running[dep.ID] = &WorkloadEntry{Spec: dep}
	blocked := map[uuid.UUID]int{sub.ID: 5}

	exitEvents := make(chan ExitEvent, 100)
	reconcileTick(t.Context(), logs.NewTest(t), uuid.Nil, launcher, &mockJournal{}, workloads, running, blocked, exitEvents, nil, nil, nil)

	if launcher.Calls() == 0 {
		t.Fatal("dependent did not start once dependency ran")
	}
	if _, ok := blocked[sub.ID]; ok {
		t.Fatal("stall counter not reset on progress")
	}

	blocked[uuid.New()] = blockedTickStallThreshold
	pruneBlocked(blocked, []workload.Spec{dep, sub})
	if len(blocked) != 0 {
		t.Fatalf("stale counters not pruned: %v", blocked)
	}
	pruneBlocked(nil, []workload.Spec{dep, sub})
}

// A running process whose live spec points elsewhere gets SIGTERM;
// preemption on a live node must not leak it.
func TestReaperStopsDisplacedRunning(t *testing.T) {
	launcher, workloads, running := setupReconcilerTest(t)

	spec := binSpec(uuid.New())
	spec.AssignedNodeID = uuid.New()
	writeSpecEvent(t, workloads, spec, uuid.Nil)
	handle := &mockHandle{exited: make(chan proc.ExitStatus, 1)}
	running[spec.ID] = &WorkloadEntry{Spec: spec, Handle: handle}

	runTickWith(t, launcher, workloads, running)

	if len(handle.signals) != 1 || handle.signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v, want one SIGTERM", handle.signals)
	}
}

// A displaced entry that already exited is dropped from the map so it
// cannot linger or restart.
func TestReaperDropsDisplacedExited(t *testing.T) {
	_, workloads, running := setupReconcilerTest(t)

	spec := binSpec(uuid.New())
	spec.AssignedNodeID = uuid.New()
	writeSpecEvent(t, workloads, spec, uuid.Nil)
	running[spec.ID] = &WorkloadEntry{Spec: spec, ExitStatus: &proc.ExitStatus{Code: 0}}

	runTickWith(t, &proc.FakeLauncher{}, workloads, running)

	if _, ok := running[spec.ID]; ok {
		t.Fatal("displaced exited entry not dropped")
	}
}

// File drift on a running process stops the old one; the new binary
// starts after the normal exit cleanup on a later tick.
func TestReconcileStopsDriftedRunning(t *testing.T) {
	launcher, workloads, running := setupReconcilerTest(t)

	old := binSpec(uuid.New())
	writeSpecEvent(t, workloads, old, uuid.Nil)
	handle := &mockHandle{exited: make(chan proc.ExitStatus, 1)}
	running[old.ID] = &WorkloadEntry{Spec: old, Handle: handle}

	updated := old
	updated.Files = []workload.FileRef{{Path: "/app/bin", Digest: testFileDigestV2, Mode: 0o755}}
	writeSpecEvent(t, workloads, updated, uuid.Nil)

	runTickWith(t, launcher, workloads, running)

	if len(handle.signals) != 1 || handle.signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v, want one SIGTERM", handle.signals)
	}
	if launcher.Calls() != 0 {
		t.Fatal("new process started before old cleanup")
	}
}

// An explicit file bump overrides restart policy and backoff: the
// operator ordered it, so a Never policy still restarts on the new digest.
func TestReconcileDriftOverridesPolicy(t *testing.T) {
	launcher, workloads, running := setupReconcilerTest(t)

	old := binSpec(uuid.New())
	old.Restart = workload.RestartNever
	writeSpecEvent(t, workloads, old, uuid.Nil)
	running[old.ID] = &WorkloadEntry{Spec: old, ExitStatus: &proc.ExitStatus{Code: 0}}

	updated := old
	updated.Files = []workload.FileRef{{Path: "/app/bin", Digest: testFileDigestV2, Mode: 0o755}}
	writeSpecEvent(t, workloads, updated, uuid.Nil)

	runTickWith(t, launcher, workloads, running)

	if launcher.Calls() == 0 {
		t.Fatal("expected new process despite Never policy")
	}
}
