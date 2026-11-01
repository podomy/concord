// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/opencontainers/runc/libcontainer"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/clock"
	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/workload"
)

// A restart-action workload failing its probe is destroyed on the fast
// beat: SIGTERM now, SIGKILL escalation on the detached timer.
func TestRunFastTickRestartDestroysUnhealthy(t *testing.T) {
	// TCP without a host port is unhealthy without touching a container.
	sick := workload.Spec{ID: uuid.New(), HealthAction: workload.HealthActionRestart, HealthProbe: workload.HealthProbeTCP}
	var nilCtr libcontainer.Container
	proc := &mockProcessHandle{exitedChan: make(chan cr.ExitStatus, 1)}
	entry := &ContainerAndProcess{Container: &nilCtr, ProcessHandle: proc, Spec: sick}
	running := map[uuid.UUID]*ContainerAndProcess{sick.ID: entry}

	runFastTick(t.Context(), logs.NewTest(t), node.NewSampler(), running, &mockJournal{}, nil, uuid.New(), nil)

	if !entry.Stopping {
		t.Fatal("expected stopping entry after unhealthy fast tick")
	}
	if len(proc.signals) != 1 || proc.signals[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v, want one SIGTERM", proc.signals)
	}
	if entry.unhealthyReported {
		t.Fatal("restart path must not set the signal-path edge flag")
	}
}

// openUnhealthyViews opens a scratch journal and an events-by-ID view for
// counting workload.unhealthy edges in fastpath tests.
func openUnhealthyViews(t *testing.T) (journal.Journal, *journalview.EventsByID, []journalview.View) {
	t.Helper()
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
	return j, eventsByID, []journalview.View{eventsByID}
}

// countUnhealthy counts workload.unhealthy events in the view.
func countUnhealthy(t *testing.T, eventsByID *journalview.EventsByID) int {
	t.Helper()
	list, err := eventsByID.List(context.Background())
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	n := 0
	for _, ev := range list {
		if ev.Type == journalview.EventTypeWorkloadUnhealthy {
			n++
		}
	}
	return n
}

// The signal action is edge-triggered: one journal event per sickness, and
// recovery clears the edge so the next sickness reports again. Driven
// through runHealthChecks directly: runFastTick would proceed to sampling,
// and Stats needs a real container.
func TestRunHealthChecksSignalEdgeDedup(t *testing.T) {
	j, eventsByID, views := openUnhealthyViews(t)

	// TCP without a host port is unhealthy without touching a container;
	// plain HTTP without ports is healthy for the same reason.
	sick := workload.Spec{ID: uuid.New(), HealthAction: workload.HealthActionSignal, HealthProbe: workload.HealthProbeTCP}
	healthy := sick
	healthy.HealthProbe = workload.HealthProbeHTTP
	var nilCtr libcontainer.Container
	entry := &ContainerAndProcess{Container: &nilCtr, ProcessHandle: &mockProcessHandle{exitedChan: make(chan cr.ExitStatus, 1)}, Spec: sick}
	running := map[uuid.UUID]*ContainerAndProcess{sick.ID: entry}
	nodeID := uuid.New()
	logger := logs.NewTest(t)
	ctx := context.Background()

	runHealthChecks(ctx, logger, running, j, views, nodeID)
	if !entry.unhealthyReported {
		t.Fatal("expected edge flag set after first sick beat")
	}
	if n := countUnhealthy(t, eventsByID); n != 1 {
		t.Fatalf("unhealthy events = %d, want 1", n)
	}

	runHealthChecks(ctx, logger, running, j, views, nodeID)
	if n := countUnhealthy(t, eventsByID); n != 1 {
		t.Fatalf("unhealthy events = %d, want edge dedup at 1", n)
	}

	entry.Spec = healthy
	runHealthChecks(ctx, logger, running, j, views, nodeID)
	if entry.unhealthyReported {
		t.Fatal("expected edge flag cleared on recovery")
	}

	entry.Spec = sick
	runHealthChecks(ctx, logger, running, j, views, nodeID)
	if n := countUnhealthy(t, eventsByID); n != 2 {
		t.Fatalf("unhealthy events = %d, want 2 after re-sicken", n)
	}
}

// Samples for workloads that left the running map drop on the fast beat;
// samples for running entries stay even when the beat cannot refresh them.
func TestRunFastTickDropsStaleWorkloadSamples(t *testing.T) {
	sampler := node.NewSampler()
	stale := uuid.New()
	live := uuid.New()
	sampler.SampleWorkload(stale, 1, 1, 1, clock.Now())
	sampler.SampleWorkload(live, 1, 1, 1, clock.Now())
	// Containerless entries skip both health and sampling, isolating the drop loop.
	running := map[uuid.UUID]*ContainerAndProcess{live: {Spec: workload.Spec{ID: live}}}

	runFastTick(t.Context(), logs.NewTest(t), sampler, running, &mockJournal{}, nil, uuid.New(), nil)

	if _, ok := sampler.Workloads()[stale]; ok {
		t.Fatal("stale workload sample not dropped")
	}
	if _, ok := sampler.Workloads()[live]; !ok {
		t.Fatal("live workload sample dropped")
	}
}
