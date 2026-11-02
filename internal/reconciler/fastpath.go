// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/clock"
	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/peerdiscovery"
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

// fastProbeTimeout bounds one probe's context. cr.CheckHealth already
// times out internally, but the explicit bound keeps the beat detached
// from caller cancellation semantics.
const fastProbeTimeout = 5 * time.Second

// checkHealth runs one probe. A var so unit tests stub slow probes
// without root or network; production always uses cr.CheckHealth.
var checkHealth = cr.CheckHealth

// runFastTick checks the health of running workloads on the fast beat and
// samples pressure for the scheduler and inspection. It appends journal
// events solely on transitions (see runHealthChecks), so the fast cadence
// never floods the log: raw observations stay in memory, only their edges
// converge.
func runFastTick(ctx context.Context, logger *logs.Logger, sampler *node.Sampler, running map[uuid.UUID]*ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID, peerService *peerdiscovery.MemberService) {
	runHealthChecks(ctx, logger, running, j, views, nodeID)
	samplePressure(logger, sampler, running, peerService)
}

func runHealthChecks(ctx context.Context, logger *logs.Logger, running map[uuid.UUID]*ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID) {
	// Collect probe targets first so mutation below stays single-threaded.
	// Probes run in parallel below; destroy/signal decisions apply after,
	// in map order, preserving the old sequential semantics.
	type target struct {
		id    uuid.UUID
		entry *ContainerAndProcess
	}
	var targets []target
	for id, entry := range running {
		if entry == nil || entry.Container == nil || entry.Stopping || entry.ExitStatus != nil {
			continue
		}
		targets = append(targets, target{id: id, entry: entry})
	}
	if len(targets) == 0 {
		return
	}

	// Probe in parallel with bounded concurrency. Each probe gets its own
	// 5s timeout so one hung check cannot stall the beat behind it.
	results := make([]bool, len(targets))
	sem := make(chan struct{}, fastProbeConcurrency)
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			probeCtx, cancel := context.WithTimeout(ctx, fastProbeTimeout)
			defer cancel()
			results[i] = checkHealth(probeCtx, logger, t.entry.Spec, t.entry.Container)
		}(i, t)
	}
	wg.Wait()

	for i := range targets {
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

// signalUnhealthy appends one workload.unhealthy edge for a sick workload.
// The unhealthyReported flag keeps it edge-triggered: without it the 500ms
// beat would append one event per beat for the whole sickness.
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
// at debug: /proc and cgroupfs either work or are permanently broken, never
// flaky.
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

// sampleWorkloadEntry reads one container's cgroup counters into the sampler.
// A container that vanished mid-beat keeps its previous sample.
func sampleWorkloadEntry(logger *logs.Logger, sampler *node.Sampler, entry *ContainerAndProcess) {
	stats, err := entry.Stats()
	if err != nil {
		logger.Debug("sample workload stats", logs.String("id", entry.Spec.ID.String()), logs.Error(err))
		return
	}
	if stats == nil || stats.CgroupStats == nil {
		return
	}
	var memLimit uint64
	if entry.Spec.Resources.MemoryMB > 0 {
		memLimit = uint64(entry.Spec.Resources.MemoryMB) * 1024 * 1024
	}
	sampler.SampleWorkload(entry.Spec.ID, stats.CgroupStats.CpuStats.CpuUsage.TotalUsage, stats.CgroupStats.MemoryStats.Usage.Usage, memLimit, clock.Now())
}
