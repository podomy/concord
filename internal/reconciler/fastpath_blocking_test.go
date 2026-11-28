// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package reconciler

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/proc"
	"github.com/podomy/concord/internal/workload"
)

// A probe that hangs past the kill grace is abandoned, counted, and
// warned, and the workload keeps its previous verdict: one wedged exec
// probe never wedges the beat or kills a healthy workload.
func TestRunHealthChecksAbandonsHungProbe(t *testing.T) {
	oldCheck := checkHealth
	oldAfter := abandonedProbeAfter
	checkHealth = func(ctx context.Context, _ *logs.Logger, _ workload.Spec) bool {
		<-ctx.Done()
		return false
	}
	abandonedProbeAfter = 50_000_000
	t.Cleanup(func() {
		checkHealth = oldCheck
		abandonedProbeAfter = oldAfter
	})
	before := probesAbandoned.Load()

	handle := &mockHandle{exited: make(chan proc.ExitStatus, 1), pid: 1 << 30}
	entry := &WorkloadEntry{Handle: handle, Spec: workload.Spec{ID: uuid.New(), HealthAction: workload.HealthActionRestart}}
	running := map[uuid.UUID]*WorkloadEntry{entry.Spec.ID: entry}

	runHealthChecks(context.Background(), logs.NewTest(t), running, &mockJournal{}, nil, uuid.New())

	if probesAbandoned.Load() != before+1 {
		t.Fatalf("abandoned = %d, want %d", probesAbandoned.Load(), before+1)
	}
	if entry.Stopping {
		t.Fatal("abandoned probe destroyed the workload")
	}
	if len(handle.signals) != 0 {
		t.Fatalf("signals = %v, want none for abandoned probe", handle.signals)
	}
}

// A dead pid writes no sample: the workload keeps its previous sample
// and the beat moves on with a debug line instead of stalling.
func TestSampleWorkloadEntryDeadPIDKeepsSample(t *testing.T) {
	t.Parallel()

	entry := &WorkloadEntry{Handle: &mockHandle{exited: make(chan proc.ExitStatus, 1), pid: 1 << 30}, Spec: workload.Spec{ID: uuid.New()}}
	sampler := node.NewSampler()

	sampleWorkloadEntry(logs.NewTest(t), sampler, entry)

	if len(sampler.Workloads()) != 0 {
		t.Fatal("dead pid wrote a sample")
	}
}
