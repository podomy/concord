// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

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

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/uuid"
	"github.com/opencontainers/runc/libcontainer"
	"github.com/opencontainers/runc/libcontainer/configs"
	"go.uber.org/zap/zaptest"

	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/workload"
)

// Unit tests for reconciler error paths: pull failure, create failure,
// wrong-node skip, and already-running skip.
// Happy-path container lifecycle tests require root and live in integration tests.

type mockJournal struct{}

func (mockJournal) Append(ctx context.Context, event journal.Event) error { return nil }
func (mockJournal) Close() error                                          { return nil }

type mockPuller struct {
	pullCalled bool
	pullResult *cr.PullResult
	pullErr    error
}

func (m *mockPuller) Pull(ctx context.Context, image, bundleDir string) (*cr.PullResult, error) {
	m.pullCalled = true
	return m.pullResult, m.pullErr
}

type mockRunner struct {
	startErr     error
	createErr    error
	createCalled bool
	startCalled  bool
	startPID     int
}

func (m *mockRunner) Create(id string, config *configs.Config) (*libcontainer.Container, error) {
	m.createCalled = true
	if m.createErr != nil {
		return nil, m.createErr
	}
	return nil, errors.New("container creation not mockable in unit tests") //nolint:wrapcheck // plain error, no wrap target
}

func (m *mockRunner) Start(ctr *libcontainer.Container, proc *libcontainer.Process) (cr.ProcessHandle, error) {
	m.startCalled = true

	processHandle := cr.NewProcess(proc, m.startPID)

	return processHandle, m.startErr
}

func TestReconcilerPullError(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
	writeSpecEvent(t, workloads, workload.Spec{Image: "nginx:latest", ID: uuid.New()}, uuid.Nil)
	puller.pullErr = errors.New("pull failed")

	runTick(t, puller, runner, workloads, running, cidrs)

	if !puller.pullCalled {
		t.Error("Pull was not called")
	}
	if runner.createCalled {
		t.Error("Create was called despite pull failure")
	}
}

func TestReconcilerCreateError(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
	writeSpecEvent(t, workloads, workload.Spec{Image: "nginx:latest", ID: uuid.New()}, uuid.Nil)
	puller.pullResult = &cr.PullResult{RootFS: t.TempDir()}
	runner.createErr = errors.New("create failed")

	runTick(t, puller, runner, workloads, running, cidrs)

	if !runner.createCalled {
		t.Error("Create was not called")
	}
	if runner.startCalled {
		t.Error("Start was called despite create failure")
	}
}

func TestReconcilerSkipsWrongNode(t *testing.T) {
	_, _, workloads, running, cidrs := setupReconcilerTest(t)
	writeSpecEvent(t, workloads, workload.Spec{Image: "nginx:latest", ID: uuid.New(), AssignedNodeID: uuid.New()}, uuid.New())
	puller, runner := &mockPuller{}, &mockRunner{}

	runTick(t, puller, runner, workloads, running, cidrs)

	if puller.pullCalled {
		t.Error("Puller was called for event belonging to another node")
	}
}

func TestReconcilerSkipsAlreadyRunning(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
	spec := workload.Spec{Image: "nginx:latest", ID: uuid.New()}
	writeSpecEvent(t, workloads, spec, uuid.Nil)
	running[spec.ID] = nil

	runTick(t, puller, runner, workloads, running, cidrs)

	if puller.pullCalled {
		t.Error("Puller was called for already-running container")
	}
}

func setupReconcilerTest(t *testing.T) (*mockPuller, *mockRunner, *journalview.Workloads, map[uuid.UUID]*ContainerAndProcess, map[uuid.UUID]string) {
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
	return &mockPuller{pullResult: &cr.PullResult{}},
		&mockRunner{},
		journalview.NewWorkloads(kv),
		map[uuid.UUID]*ContainerAndProcess{},
		map[uuid.UUID]string{}
}

func runTick(t *testing.T, puller cr.Puller, runner cr.Runner, workloads *journalview.Workloads, running map[uuid.UUID]*ContainerAndProcess, cidrs map[uuid.UUID]string) {
	t.Helper()
	exitEvents := make(chan ExitEvent, 100)
	reconcileTick(t.Context(), zaptest.NewLogger(t), uuid.Nil, puller, runner, &mockJournal{}, workloads, running, cidrs, exitEvents, nil, nil, nil)
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
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)

	spec := workload.Spec{
		ID:             uuid.New(),
		Image:          "nginx:latest",
		AssignedNodeID: uuid.Nil,
	}
	writeSpecEvent(t, workloads, spec, uuid.Nil)

	spec.Removed = true
	writeSpecEvent(t, workloads, spec, uuid.Nil)

	runTick(t, puller, runner, workloads, running, cidrs)

	if puller.pullCalled {
		t.Fatal("puller was called for a tombstoned workload")
	}
}

type mockProcessHandle struct {
	signals    []os.Signal
	exitedChan chan cr.ExitStatus
}

func (m *mockProcessHandle) NamespacePID() int { return 1234 }
func (m *mockProcessHandle) Signal(sig os.Signal) error {
	m.signals = append(m.signals, sig)
	return nil
}
func (m *mockProcessHandle) Exited() <-chan cr.ExitStatus { return m.exitedChan }

func TestReconcilerDestroyContainerAsync(t *testing.T) {
	spec := workload.Spec{ID: uuid.New()}
	exitedChan := make(chan cr.ExitStatus, 1)
	proc := &mockProcessHandle{exitedChan: exitedChan}
	entry := &ContainerAndProcess{
		Spec:          spec,
		ProcessHandle: proc,
	}
	running := map[uuid.UUID]*ContainerAndProcess{spec.ID: entry}
	exitEvents := make(chan ExitEvent, 1)

	go monitorProcessExit(t.Context(), spec.ID, proc, exitEvents)

	destroyContainer(t.Context(), zaptest.NewLogger(t), spec, running)

	if !entry.Stopping {
		t.Error("expected entry.Stopping to be true")
	}
	if len(proc.signals) != 1 || proc.signals[0] != syscall.SIGTERM {
		t.Errorf("expected SIGTERM signal, got %v", proc.signals)
	}

	// Send exit notification.
	exitedChan <- cr.ExitStatus{Code: 0}

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
	entry := &ContainerAndProcess{
		Spec:     spec,
		Stopping: true,
	}
	running := map[uuid.UUID]*ContainerAndProcess{spec.ID: entry}
	cidrs := map[uuid.UUID]string{}

	handleExitEvent(t.Context(), zaptest.NewLogger(t), &mockJournal{}, uuid.Nil, spec.ID, cr.ExitStatus{Code: 0}, running, cidrs, nil, nil)

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

	recordInstanceEvent(ctx, zaptest.NewLogger(t), j, views, spec, uuid.New(), workload.StateRunning, 123)

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

// Container output flows into the sampler's log rings; a nil sampler
// leaves writers unset and output discards as before.
func TestBuildProcessAttachesLogWriters(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), Command: []string{"/bin/true"}}
	result := &cr.PullResult{Config: &v1.ConfigFile{}}

	proc, err := buildProcess(spec, result, node.NewSampler())
	if err != nil {
		t.Fatalf("build process: %v", err)
	}
	if proc.Stdout == nil || proc.Stderr == nil {
		t.Fatal("expected log writers attached")
	}
	if !proc.Init {
		t.Fatal("initial process must take the init path, not setns join")
	}

	plain, err := buildProcess(spec, result, nil)
	if err != nil {
		t.Fatalf("build process without sampler: %v", err)
	}
	if plain.Stdout != nil || plain.Stderr != nil {
		t.Fatal("expected unset writers without sampler")
	}
}

// A commandless submit runs the image default: entrypoint plus cmd.
// This is the standard container contract the probe image relies on.
func TestBuildProcessFallsBackToImageEntrypoint(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), Image: "probe:latest"}
	result := &cr.PullResult{Config: &v1.ConfigFile{}}
	result.Config.Config.Entrypoint = []string{"/probe"}
	result.Config.Config.Cmd = []string{"--serve"}

	proc, err := buildProcess(spec, result, nil)
	if err != nil {
		t.Fatalf("build process: %v", err)
	}
	want := []string{"/probe", "--serve"}
	if len(proc.Args) != len(want) || proc.Args[0] != want[0] || proc.Args[1] != want[1] {
		t.Fatalf("args = %v, want %v", proc.Args, want)
	}
}

// An explicit spec command overrides the image default wholesale.
func TestBuildProcessSpecCommandWins(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), Command: []string{"/bin/sh", "-c", "x"}}
	result := &cr.PullResult{Config: &v1.ConfigFile{}}
	result.Config.Config.Entrypoint = []string{"/probe"}

	proc, err := buildProcess(spec, result, nil)
	if err != nil {
		t.Fatalf("build process: %v", err)
	}
	if len(proc.Args) != 3 || proc.Args[0] != "/bin/sh" {
		t.Fatalf("args = %v, want spec command", proc.Args)
	}
}

// No spec command and no image default is a plain error at build time,
// never an empty argv reaching runc init.
func TestBuildProcessRejectsEmptyCommand(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), Image: "empty:latest"}
	result := &cr.PullResult{Config: &v1.ConfigFile{}}

	_, err := buildProcess(spec, result, nil)
	if err == nil {
		t.Fatal("expected error for empty command and empty image default")
	}

	_, err = buildProcess(spec, nil, nil)
	if err == nil {
		t.Fatal("expected error for empty command and nil pull result")
	}
}

// A dependent waits while its same-node dependency is down, and starts
// once it runs. The exited dependency itself never restarts here
// (RestartNever), isolating the gate signal.
func TestReconcileWaitsOnDependencies(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
	puller.pullResult = &cr.PullResult{RootFS: t.TempDir()}

	dep := workload.Spec{ID: uuid.New(), Image: "x", AssignedNodeID: uuid.Nil, Restart: workload.RestartNever}
	sub := workload.Spec{ID: uuid.New(), Image: "x", AssignedNodeID: uuid.Nil, DependsOn: []uuid.UUID{dep.ID}}
	writeSpecEvent(t, workloads, dep, uuid.Nil)
	writeSpecEvent(t, workloads, sub, uuid.Nil)

	running[dep.ID] = &ContainerAndProcess{Spec: dep, ExitStatus: &cr.ExitStatus{Code: 0}}
	runTick(t, puller, runner, workloads, running, cidrs)
	if runner.createCalled {
		t.Fatal("dependent started while dependency down")
	}

	running[dep.ID] = &ContainerAndProcess{Spec: dep}
	runner.createCalled = false
	runTick(t, puller, runner, workloads, running, cidrs)
	if !runner.createCalled {
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
			puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
			puller.pullResult = &cr.PullResult{RootFS: t.TempDir()}
			writeSpecEvent(t, workloads, tc.dep, uuid.Nil)
			sub := workload.Spec{ID: uuid.New(), Image: "x", AssignedNodeID: uuid.Nil, DependsOn: []uuid.UUID{tc.dep.ID}}
			writeSpecEvent(t, workloads, sub, uuid.Nil)

			runTick(t, puller, runner, workloads, running, cidrs)
			if !runner.createCalled {
				t.Fatal("dependent stalled on irrelevant dependency")
			}
		})
	}

	// Unknown IDs are ignored the same way.
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
	puller.pullResult = &cr.PullResult{RootFS: t.TempDir()}
	sub := workload.Spec{ID: uuid.New(), Image: "x", AssignedNodeID: uuid.Nil, DependsOn: []uuid.UUID{uuid.New()}}
	writeSpecEvent(t, workloads, sub, uuid.Nil)
	runTick(t, puller, runner, workloads, running, cidrs)
	if !runner.createCalled {
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
	// Test RestartNever: exited workload should not be restarted.
	specNever := workload.Spec{ID: uuid.New(), Restart: workload.RestartNever}
	entryNever := &ContainerAndProcess{Spec: specNever, ExitStatus: &cr.ExitStatus{Code: 0}}
	runningNever := map[uuid.UUID]*ContainerAndProcess{specNever.ID: entryNever}

	reconcileWorkloadSpec(t.Context(), zaptest.NewLogger(t), uuid.Nil, &mockPuller{}, &mockRunner{}, &mockJournal{}, specNever, runningNever, map[uuid.UUID]string{}, nil, nil, nil, nil, nil)
	if _, exists := runningNever[specNever.ID]; !exists {
		t.Error("expected RestartNever workload entry to be retained in running map")
	}

	// Test RestartOnFailure with clean exit (0): should not restart.
	specOnFailClean := workload.Spec{ID: uuid.New(), Restart: workload.RestartOnFailure}
	entryOnFailClean := &ContainerAndProcess{Spec: specOnFailClean, ExitStatus: &cr.ExitStatus{Code: 0}}
	runningOnFailClean := map[uuid.UUID]*ContainerAndProcess{specOnFailClean.ID: entryOnFailClean}

	reconcileWorkloadSpec(t.Context(), zaptest.NewLogger(t), uuid.Nil, &mockPuller{}, &mockRunner{}, &mockJournal{}, specOnFailClean, runningOnFailClean, map[uuid.UUID]string{}, nil, nil, nil, nil, nil)
	if _, exists := runningOnFailClean[specOnFailClean.ID]; !exists {
		t.Error("expected RestartOnFailure with clean exit to be retained in running map")
	}

	// Test RestartAlways: exited workload entry should be cleared and restarted.
	specAlways := workload.Spec{ID: uuid.New(), Restart: workload.RestartAlways}
	entryAlways := &ContainerAndProcess{Spec: specAlways, ExitStatus: &cr.ExitStatus{Code: 0}}
	runningAlways := map[uuid.UUID]*ContainerAndProcess{specAlways.ID: entryAlways}
	pullerAlways := &mockPuller{pullResult: &cr.PullResult{}}

	reconcileWorkloadSpec(t.Context(), zaptest.NewLogger(t), uuid.Nil, pullerAlways, &mockRunner{}, &mockJournal{}, specAlways, runningAlways, map[uuid.UUID]string{}, nil, nil, nil, nil, nil)
	if !pullerAlways.pullCalled {
		t.Error("expected RestartAlways workload to trigger startContainer on tick")
	}
}

// A running container whose live spec points elsewhere gets SIGTERM;
// preemption on a live node must not leak it.
func TestReaperStopsDisplacedRunning(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
	puller.pullResult = &cr.PullResult{RootFS: t.TempDir()}

	spec := workload.Spec{ID: uuid.New(), Image: "x", AssignedNodeID: uuid.New()}
	writeSpecEvent(t, workloads, spec, uuid.Nil)
	proc := &mockProcessHandle{exitedChan: make(chan cr.ExitStatus, 1)}
	running[spec.ID] = &ContainerAndProcess{Spec: spec, ProcessHandle: proc}

	runTick(t, puller, runner, workloads, running, cidrs)

	if len(proc.signals) != 1 || proc.signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v, want one SIGTERM", proc.signals)
	}
}

// A displaced entry that already exited is dropped from the map so it
// cannot linger or restart.
func TestReaperDropsDisplacedExited(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)

	spec := workload.Spec{ID: uuid.New(), Image: "x", AssignedNodeID: uuid.New()}
	writeSpecEvent(t, workloads, spec, uuid.Nil)
	running[spec.ID] = &ContainerAndProcess{Spec: spec, ExitStatus: &cr.ExitStatus{Code: 0}}

	runTick(t, puller, runner, workloads, running, cidrs)

	if _, ok := running[spec.ID]; ok {
		t.Fatal("displaced exited entry not dropped")
	}
}

// Image drift on a running container stops the old one; the new image
// starts after the normal exit cleanup on a later tick.
func TestReconcileStopsDriftedRunning(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
	puller.pullResult = &cr.PullResult{RootFS: t.TempDir()}

	old := workload.Spec{ID: uuid.New(), Image: "app:v1", AssignedNodeID: uuid.Nil}
	writeSpecEvent(t, workloads, old, uuid.Nil)
	proc := &mockProcessHandle{exitedChan: make(chan cr.ExitStatus, 1)}
	running[old.ID] = &ContainerAndProcess{Spec: old, ProcessHandle: proc}

	updated := old
	updated.Image = "app:v2"
	writeSpecEvent(t, workloads, updated, uuid.Nil)

	runTick(t, puller, runner, workloads, running, cidrs)

	if len(proc.signals) != 1 || proc.signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v, want one SIGTERM", proc.signals)
	}
	if runner.createCalled {
		t.Fatal("new container started before old cleanup")
	}
}

// An explicit image bump overrides restart policy and backoff: the
// operator ordered it, so a Never policy still restarts on the new image.
func TestReconcileDriftOverridesPolicy(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)
	puller.pullResult = &cr.PullResult{RootFS: t.TempDir()}

	old := workload.Spec{ID: uuid.New(), Image: "app:v1", AssignedNodeID: uuid.Nil, Restart: workload.RestartNever}
	writeSpecEvent(t, workloads, old, uuid.Nil)
	running[old.ID] = &ContainerAndProcess{Spec: old, ExitStatus: &cr.ExitStatus{Code: 0}}

	updated := old
	updated.Image = "app:v2"
	writeSpecEvent(t, workloads, updated, uuid.Nil)

	runTick(t, puller, runner, workloads, running, cidrs)

	if !runner.createCalled {
		t.Fatal("expected new container despite Never policy")
	}
}
