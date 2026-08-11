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

	"github.com/google/uuid"
	"github.com/opencontainers/runc/libcontainer"
	"github.com/opencontainers/runc/libcontainer/configs"
	"go.uber.org/zap/zaptest"

	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
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
	writeSpecEvent(t, workloads, workload.Spec{Image: "nginx:latest", ID: uuid.New(), SegmentID: uuid.New()}, uuid.New())
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
	reconcileTick(t.Context(), zaptest.NewLogger(t), uuid.Nil, puller, runner, &mockJournal{}, workloads, running, cidrs, exitEvents, nil)
}

func writeSpecEvent(t *testing.T, workloads *journalview.Workloads, spec workload.Spec, nodeID uuid.UUID) {
	t.Helper()
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := workloads.Apply(t.Context(), journal.NewEvent(nodeID, "workload.spec", payload)); err != nil {
		t.Fatal(err)
	}
}

// TestReconcilerDoesNotStartTombstonedWorkload verifies removed specs are not started.
func TestReconcilerDoesNotStartTombstonedWorkload(t *testing.T) {
	puller, runner, workloads, running, cidrs := setupReconcilerTest(t)

	spec := workload.Spec{
		ID:        uuid.New(),
		Image:     "nginx:latest",
		SegmentID: uuid.Nil,
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

	handleExitEvent(t.Context(), zaptest.NewLogger(t), &mockJournal{}, uuid.Nil, spec.ID, cr.ExitStatus{Code: 0}, running, cidrs, nil)

	if entry.ExitStatus == nil {
		t.Error("expected entry.ExitStatus to be non-nil after handleExitEvent")
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

	reconcileWorkloadSpec(t.Context(), zaptest.NewLogger(t), uuid.Nil, &mockPuller{}, &mockRunner{}, &mockJournal{}, specNever, runningNever, map[uuid.UUID]string{}, nil, nil)
	if _, exists := runningNever[specNever.ID]; !exists {
		t.Error("expected RestartNever workload entry to be retained in running map")
	}

	// Test RestartOnFailure with clean exit (0): should not restart.
	specOnFailClean := workload.Spec{ID: uuid.New(), Restart: workload.RestartOnFailure}
	entryOnFailClean := &ContainerAndProcess{Spec: specOnFailClean, ExitStatus: &cr.ExitStatus{Code: 0}}
	runningOnFailClean := map[uuid.UUID]*ContainerAndProcess{specOnFailClean.ID: entryOnFailClean}

	reconcileWorkloadSpec(t.Context(), zaptest.NewLogger(t), uuid.Nil, &mockPuller{}, &mockRunner{}, &mockJournal{}, specOnFailClean, runningOnFailClean, map[uuid.UUID]string{}, nil, nil)
	if _, exists := runningOnFailClean[specOnFailClean.ID]; !exists {
		t.Error("expected RestartOnFailure with clean exit to be retained in running map")
	}

	// Test RestartAlways: exited workload entry should be cleared and restarted.
	specAlways := workload.Spec{ID: uuid.New(), Restart: workload.RestartAlways}
	entryAlways := &ContainerAndProcess{Spec: specAlways, ExitStatus: &cr.ExitStatus{Code: 0}}
	runningAlways := map[uuid.UUID]*ContainerAndProcess{specAlways.ID: entryAlways}
	pullerAlways := &mockPuller{pullResult: &cr.PullResult{}}

	reconcileWorkloadSpec(t.Context(), zaptest.NewLogger(t), uuid.Nil, pullerAlways, &mockRunner{}, &mockJournal{}, specAlways, runningAlways, map[uuid.UUID]string{}, nil, nil)
	if !pullerAlways.pullCalled {
		t.Error("expected RestartAlways workload to trigger startContainer on tick")
	}
}
