// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/workload"
)

// SIGKILL timer cancels when the target exits before the timeout: only
// SIGTERM fires, no dead-handle signal, no PID-reuse risk.
func TestDestroyContainerCancelsSigkillOnEarlyExit(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{ID: uuid.New(), StopTimeoutSeconds: 1}
	exited := make(chan cr.ExitStatus, 1)
	proc := &mockProcessHandle{exitedChan: exited}
	entry := &ContainerAndProcess{Spec: spec, ProcessHandle: proc}
	running := map[uuid.UUID]*ContainerAndProcess{spec.ID: entry}

	destroyContainer(logs.NewTest(t), spec, running)

	// Simulate the exit path closing done before the 1s timeout.
	handleExitEvent(context.Background(), logs.NewTest(t), &mockJournal{}, uuid.New(), spec.ID, cr.ExitStatus{Code: 0}, running, map[uuid.UUID]string{}, nil, nil)

	time.Sleep(1500 * time.Millisecond)

	if len(proc.signals) != 1 || proc.signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v, want only one SIGTERM (no SIGKILL after early exit)", proc.signals)
	}
	if entry.ExitStatus == nil {
		t.Fatal("expected exit recorded")
	}
}

// Same-image port, file, and resource edits restart through the drift
// path; identical specs do not.
func TestSpecDriftedCoversFullSpec(t *testing.T) {
	t.Parallel()

	base := workload.Spec{ID: uuid.New(), Image: "app:v1", HostPort: 80, ContainerPort: 80}
	if specDrifted(base, base) {
		t.Fatal("identical specs must not drift")
	}

	port := base
	port.HostPort = 8080
	if !specDrifted(base, port) {
		t.Fatal("HostPort change must drift")
	}

	cport := base
	cport.ContainerPort = 8080
	if !specDrifted(base, cport) {
		t.Fatal("ContainerPort change must drift")
	}

	res := base
	res.Resources.MemoryMB = 512
	if !specDrifted(base, res) {
		t.Fatal("Resources change must drift")
	}

	files := base
	files.Files = []workload.FileRef{{Path: "/a", Digest: "sha256:" + string(make([]byte, 0)), Mode: 0o600}}
	// Use a valid digest shape for the drift check (equality only).
	files.Files[0].Digest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if !specDrifted(base, files) {
		t.Fatal("Files change must drift")
	}

	img := base
	img.Image = "app:v2"
	if !specDrifted(base, img) {
		t.Fatal("Image change must drift")
	}
}

// Shutdown drain processes queued exits within the documented bound so
// in-flight exits record stopped status and free network deterministically.
func TestDrainExitEventsProcessesQueue(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{ID: uuid.New()}
	entry := &ContainerAndProcess{Spec: spec}
	running := map[uuid.UUID]*ContainerAndProcess{spec.ID: entry}
	cidrs := map[uuid.UUID]string{}
	exitEvents := make(chan ExitEvent, 10)
	exitEvents <- ExitEvent{WorkloadID: spec.ID, ExitStatus: cr.ExitStatus{Code: 0}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	drainExitEvents(ctx, logs.NewTest(t), &mockJournal{}, uuid.New(), exitEvents, running, cidrs, nil, nil)
	elapsed := time.Since(start)

	if entry.ExitStatus == nil {
		t.Fatal("drain did not record stopped status")
	}
	if elapsed > shutdownDrainBound+time.Second {
		t.Fatalf("drain took %v, want within %v bound", elapsed, shutdownDrainBound)
	}
}
