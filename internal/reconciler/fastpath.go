// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package reconciler

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/clock"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/proc"
	"github.com/podomy/concord/internal/workload"
)

// fastTickInterval is the local health beat. It runs an order of magnitude
// faster than the 5s reconcile tick because dead processes and failed health
// checks need millisecond-to-second reactions, not convergent ones. The fast
// beat reads local state only and never schedules, places, or syncs.
const fastTickInterval = 500 * time.Millisecond

// historyInterval captures trend history. Five seconds keeps an hour of
// context in 720 points per series; anything faster belongs in memory on
// the fast beat, not in history.
const historyInterval = 5 * time.Second

// fastProbeConcurrency caps parallel health probes on the fast beat. Each
// probe holds its own 5s timeout, so 8 in flight keeps a normal fleet
// well under the 500ms cadence even when one probe hangs.
const fastProbeConcurrency = 8

// fastProbeTimeout bounds one probe's context. proc.CheckHealth already
// times out internally, but the explicit bound keeps the beat detached
// from caller cancellation semantics.
const fastProbeTimeout = 5 * time.Second

// execKillGrace bounds the post-SIGKILL wait after the probe timeout: an
// exec probe that ignores SIGKILL must not stall the beat behind it. The
// abandoned probe counts in probesAbandoned and its workload keeps its
// previous health verdict for the beat.
const execKillGrace = 5 * time.Second

// abandonedProbeAfter bounds one probe end to end: the probe context plus
// the post-kill grace. A var so unit tests shrink it without waiting out
// two real timeouts.
var abandonedProbeAfter = fastProbeTimeout + execKillGrace

// probesAbandoned counts health probes abandoned after the kill grace.
// Monotonic: a rising count means probes hang past SIGKILL.
var probesAbandoned atomic.Int64

// checkHealth runs one probe. A var so unit tests stub slow probes
// without root or network; production always uses proc.CheckHealth.
var checkHealth = proc.CheckHealth

// runFastTick checks the health of running workloads on the fast beat and
// samples pressure for the scheduler and inspection. It appends journal
// events solely on transitions (see runHealthChecks), so the fast cadence
// never floods the log: raw observations stay in memory, only their edges
// converge.
func runFastTick(ctx context.Context, logger *logs.Logger, sampler *node.Sampler, running map[uuid.UUID]*ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID, peerService *peerdiscovery.MemberService) {
	runHealthChecks(ctx, logger, running, j, views, nodeID)
	samplePressure(logger, sampler, running, peerService)
	// Log ring backpressure surfaces here, one debug line per beat while
	// any ring has ever dropped: the fast beat is the periodic owner and
	// the handleWorkloadLogs API stays untouched.
	if sampler != nil {
		if dropped := sampler.LogDropped(); dropped > 0 {
			logger.Debug("workload log rings dropped lines under backpressure", logs.Uint64("dropped", dropped))
		}
	}
}

func runHealthChecks(ctx context.Context, logger *logs.Logger, running map[uuid.UUID]*ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID) {
	targets := collectProbeTargets(running)
	if len(targets) == 0 {
		return
	}

	// Probe in parallel with bounded concurrency. Each probe gets its own
	// 5s timeout so one hung check cannot stall the beat behind it. An
	// outer abandon bound covers the post-SIGKILL wait: a probe that
	// hangs past it is skipped for the beat, counted, and warned, so one
	// wedged workload never wedges the tick. Per-workload failures stay
	// local to that workload: abandon, unhealthy, and stat errors never
	// abort the rest of the beat.
	results := make([]bool, len(targets))
	abandoned := make([]bool, len(targets))
	sem := make(chan struct{}, fastProbeConcurrency)
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Go(func() {
			probeTarget(ctx, logger, sem, t, &results[i], &abandoned[i])
		})
	}
	wg.Wait()

	for i := range targets {
		if abandoned[i] {
			continue
		}
		healthy := results[i]
		entry := targets[i].entry
		// Check liveness.
		if healthy {
			entry.unhealthyReported = false
			continue
		}

		// Check readiness and resources.
		switch entry.Spec.HealthAction {
		case workload.HealthActionRestart:
			logger.Warn("restarting unhealthy workload")
			destroyContainer(logger, entry.Spec, running)
		case workload.HealthActionSignal:
			signalUnhealthy(ctx, logger, entry, j, views, nodeID)
		}
	}
}

// collectProbeTargets lists running entries needing a probe. Collecting
// first keeps mutation below single-threaded: probes run in parallel and
// destroy/signal decisions apply after, in map order, preserving the old
// sequential semantics.
func collectProbeTargets(running map[uuid.UUID]*ContainerAndProcess) []healthTarget {
	var targets []healthTarget
	for id, entry := range running {
		if entry == nil || entry.Container == nil || entry.Stopping || entry.ExitStatus != nil {
			continue
		}
		targets = append(targets, healthTarget{id: id, entry: entry})
	}
	return targets
}

// healthTarget pairs one workload ID with its running entry for a probe.
type healthTarget struct {
	id    uuid.UUID
	entry *ContainerAndProcess
}

// probeTarget runs one health probe with an outer abandon bound covering
// the post-SIGKILL wait: a probe that hangs past it is skipped for the
// beat, counted, and warned, so one wedged workload never wedges the
// tick. Per-workload failures stay local: abandon, unhealthy, and stat
// errors never abort the rest of the beat. result and abandoned point at
// this target's slots, written before the caller's Wait returns.
func probeTarget(ctx context.Context, logger *logs.Logger, sem chan struct{}, t healthTarget, result, abandoned *bool) {
	sem <- struct{}{}
	defer func() { <-sem }()
	probeCtx, cancel := context.WithTimeout(ctx, fastProbeTimeout)
	defer cancel()
	resCh := make(chan bool, 1)
	go func() {
		resCh <- checkHealth(probeCtx, logger, t.entry.Spec)
	}()
	select {
	case r := <-resCh:
		*result = r
	case <-time.After(abandonedProbeAfter):
		*abandoned = true
		probesAbandoned.Add(1)
		logger.Warn("health probe hung past kill grace, skipping decision this beat",
			logs.String("id", t.id.String()))
	case <-ctx.Done():
		*abandoned = true
	}
}

// The unhealthyReported flag keeps it edge-triggered: without it the 500ms
// beat would append one event per beat for the whole sickness.
// signalUnhealthy appends one workload.unhealthy edge for a sick workload.
func signalUnhealthy(ctx context.Context, logger *logs.Logger, entry *ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID) {
	if entry.unhealthyReported {
		return
	}
	logger.Warn("signaling unhealthy workload")
	payload, err := json.Marshal(workload.Unhealthy{WorkloadID: entry.Spec.ID})
	if err != nil {
		logger.Error("marshal unhealthy event", logs.Error(err))
		return
	}
	event := journal.NewEvent(nodeID, journalview.EventTypeWorkloadUnhealthy, payload)
	err = journalview.RecordEventAndLog(ctx, logger, j, views, event, "workload.unhealthy",
		logs.String("workload_id", entry.Spec.ID.String()),
	)
	if err != nil {
		logger.Error("record event failed", logs.Error(err))
		return
	}
	entry.unhealthyReported = true
}

// samplePressure refreshes host and per-workload utilization and gossips the
// host trio for placement. Sampling failures keep previous readings and log
// at debug: /proc either works or is permanently broken, never flaky.
func samplePressure(logger *logs.Logger, sampler *node.Sampler, running map[uuid.UUID]*ContainerAndProcess, peerService *peerdiscovery.MemberService) {
	pressure, err := sampler.SampleHost()
	if err != nil {
		logger.Debug("sample host pressure", logs.Error(err))
	} else {
		peerService.SetPressure(pressure.CPU, pressure.Mem, pressure.Disk)
		if err := peerService.Publish(); err != nil {
			logger.Debug("publish metadata", logs.Error(err))
		}
	}

	for _, entry := range running {
		if entry == nil || entry.Container == nil || entry.Stopping || entry.ExitStatus != nil {
			continue
		}
		sampleWorkloadEntry(logger, sampler, entry)
	}

	for id := range sampler.Workloads() {
		if _, ok := running[id]; !ok {
			sampler.DropWorkload(id)
		}
	}
}

// sampleWorkloadEntry reads one process's CPU and memory counters into the
// sampler. A process that vanished mid-beat keeps its previous sample;
// a dead pid only costs a debug line, never the beat.
func sampleWorkloadEntry(logger *logs.Logger, sampler *node.Sampler, entry *ContainerAndProcess) {
	if entry.ProcessHandle == nil {
		logger.Debug("sample workload without process", logs.String("id", entry.Spec.ID.String()))
		return
	}
	cpuTotal, memBytes, err := node.ReadProcStat(entry.NamespacePID())
	if err != nil {
		logger.Debug("sample workload stats", logs.String("id", entry.Spec.ID.String()), logs.Error(err))
		return
	}
	var memLimit uint64
	if entry.Spec.Resources.MemoryMB > 0 {
		memLimit = uint64(entry.Spec.Resources.MemoryMB) * 1024 * 1024
	}
	sampler.SampleWorkload(entry.Spec.ID, cpuTotal, memBytes, memLimit, clock.Now())
}
