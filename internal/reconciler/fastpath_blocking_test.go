// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/opencontainers/runc/libcontainer"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/workload"
)

// A probe that hangs past the kill grace is abandoned, counted, and
// warned, and the workload keeps its previous verdict: one wedged exec
// probe never wedges the beat or kills a healthy workload.
func TestRunHealthChecksAbandonsHungProbe(t *testing.T) {
	oldCheck := checkHealth
	oldAfter := abandonedProbeAfter
	checkHealth = func(ctx context.Context, _ *logs.Logger, _ workload.Spec, _ *libcontainer.Container) bool {
		<-ctx.Done()
		return false
	}
	abandonedProbeAfter = 50_000_000
	t.Cleanup(func() {
		checkHealth = oldCheck
		abandonedProbeAfter = oldAfter
	})
	before := probesAbandoned.Load()

	var ctr libcontainer.Container
	proc := &mockProcessHandle{exitedChan: make(chan cr.ExitStatus, 1)}
	entry := &ContainerAndProcess{Container: &ctr, ProcessHandle: proc, Spec: workload.Spec{ID: uuid.New(), HealthAction: workload.HealthActionRestart}}
	running := map[uuid.UUID]*ContainerAndProcess{entry.Spec.ID: entry}

	runHealthChecks(context.Background(), logs.NewTest(t), running, &mockJournal{}, nil, uuid.New())

	if probesAbandoned.Load() != before+1 {
		t.Fatalf("abandoned = %d, want %d", probesAbandoned.Load(), before+1)
	}
	if entry.Stopping {
		t.Fatal("abandoned probe destroyed the workload")
	}
	if len(proc.signals) != 0 {
		t.Fatalf("signals = %v, want none for abandoned probe", proc.signals)
	}
}

// A Stats read that hangs past the timeout is abandoned, counted, and
// warned, and the workload keeps its previous sample.
func TestSampleWorkloadEntryAbandonsHungStats(t *testing.T) {
	oldStats := containerStats
	oldTimeout := statsTimeout
	ctx, cancel := context.WithCancel(context.Background())
	released := make(chan struct{})
	containerStats = func(_ *libcontainer.Container) (*libcontainer.Stats, error) {
		defer close(released)
		<-ctx.Done()
		return nil, nil
	}
	statsTimeout = 50_000_000
	t.Cleanup(func() {
		// Unblock the abandoned read and wait for it to exit before
		// restoring the seam: the timeout leaves it in flight.
		cancel()
		<-released
		containerStats = oldStats
		statsTimeout = oldTimeout
	})
	before := statsAbandoned.Load()

	var ctr libcontainer.Container
	entry := &ContainerAndProcess{Container: &ctr, Spec: workload.Spec{ID: uuid.New()}}
	sampler := node.NewSampler()

	sampleWorkloadEntry(logs.NewTest(t), sampler, entry)

	if statsAbandoned.Load() != before+1 {
		t.Fatalf("abandoned = %d, want %d", statsAbandoned.Load(), before+1)
	}
	if len(sampler.Workloads()) != 0 {
		t.Fatal("hung stats wrote a sample")
	}
}
