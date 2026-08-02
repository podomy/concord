// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/opencontainers/runc/libcontainer"
	"github.com/opencontainers/runc/libcontainer/configs"
	"go.uber.org/zap/zaptest"

	"github.com/podomy/concord/src/cr"
	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
	"github.com/podomy/concord/src/kvstore"
	"github.com/podomy/concord/src/workload"
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

func (m *mockRunner) Start(ctr *libcontainer.Container, proc *libcontainer.Process) (int, error) {
	m.startCalled = true
	return m.startPID, m.startErr
}

func TestReconcilerPullError(t *testing.T) {
	puller, runner, eventsByType, running, cidrs := setupReconcilerTest(t)
	writeSpecEvent(t, eventsByType, workload.Spec{Image: "nginx:latest", ID: uuid.New()}, uuid.Nil)
	puller.pullErr = errors.New("pull failed")

	runTick(t, puller, runner, eventsByType, running, cidrs)

	if !puller.pullCalled {
		t.Error("Pull was not called")
	}
	if runner.createCalled {
		t.Error("Create was called despite pull failure")
	}
}

func TestReconcilerCreateError(t *testing.T) {
	puller, runner, eventsByType, running, cidrs := setupReconcilerTest(t)
	writeSpecEvent(t, eventsByType, workload.Spec{Image: "nginx:latest", ID: uuid.New()}, uuid.Nil)
	puller.pullResult = &cr.PullResult{RootFS: t.TempDir()}
	runner.createErr = errors.New("create failed")

	runTick(t, puller, runner, eventsByType, running, cidrs)

	if !runner.createCalled {
		t.Error("Create was not called")
	}
	if runner.startCalled {
		t.Error("Start was called despite create failure")
	}
}

func TestReconcilerSkipsWrongNode(t *testing.T) {
	_, _, eventsByType, running, cidrs := setupReconcilerTest(t)
	writeSpecEvent(t, eventsByType, workload.Spec{Image: "nginx:latest", ID: uuid.New()}, uuid.New())
	puller, runner := &mockPuller{}, &mockRunner{}

	runTick(t, puller, runner, eventsByType, running, cidrs)

	if puller.pullCalled {
		t.Error("Puller was called for event belonging to another node")
	}
}

func TestReconcilerSkipsAlreadyRunning(t *testing.T) {
	puller, runner, eventsByType, running, cidrs := setupReconcilerTest(t)
	spec := workload.Spec{Image: "nginx:latest", ID: uuid.New()}
	writeSpecEvent(t, eventsByType, spec, uuid.Nil)
	running[spec.ID] = nil

	runTick(t, puller, runner, eventsByType, running, cidrs)

	if puller.pullCalled {
		t.Error("Puller was called for already-running container")
	}
}

func setupReconcilerTest(t *testing.T) (*mockPuller, *mockRunner, *journalview.EventsByType, map[uuid.UUID]*libcontainer.Container, map[uuid.UUID]string) {
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
		journalview.NewEventsByType(kv),
		map[uuid.UUID]*libcontainer.Container{},
		map[uuid.UUID]string{}
}

func runTick(t *testing.T, puller cr.Puller, runner cr.Runner, eventsByType *journalview.EventsByType, running map[uuid.UUID]*libcontainer.Container, cidrs map[uuid.UUID]string) {
	t.Helper()
	reconcileTick(t.Context(), zaptest.NewLogger(t), uuid.Nil, puller, runner, &mockJournal{}, eventsByType, running, cidrs)
}

func writeSpecEvent(t *testing.T, vt *journalview.EventsByType, spec workload.Spec, nodeID uuid.UUID) {
	t.Helper()
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := vt.Apply(t.Context(), journal.NewEvent(nodeID, "workload.spec", payload)); err != nil {
		t.Fatal(err)
	}
}
