// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

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

// runFastTick checks the health of running workloads on the fast beat and
// samples pressure for the scheduler and inspection. It appends journal
// events solely on transitions (see runHealthChecks), so the fast cadence
// never floods the log: raw observations stay in memory, only their edges
// converge.
func runFastTick(ctx context.Context, logger *zap.Logger, sampler *node.Sampler, running map[uuid.UUID]*ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID, peerService *peerdiscovery.MemberService) {
	runHealthChecks(ctx, logger, running, j, views, nodeID)
	samplePressure(logger, sampler, running, peerService)
}

func runHealthChecks(ctx context.Context, logger *zap.Logger, running map[uuid.UUID]*ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID) {
	for _, entry := range running {
		if entry == nil || entry.Container == nil || entry.Stopping || entry.ExitStatus != nil {
			continue
		}
		healthy := cr.CheckHealth(ctx, logger, entry.Spec)
		// Check liveness.
		if healthy {
			entry.unhealthyReported = false
			continue
		}

		// Check readiness and resources.
		switch entry.Spec.HealthAction {
		case workload.HealthActionRestart:
			logger.Warn("restarting unhealthy workload")
			destroyContainer(ctx, logger, entry.Spec, running)
		case workload.HealthActionSignal:
			signalUnhealthy(ctx, logger, entry, j, views, nodeID)
		}
	}
}

// signalUnhealthy appends one workload.unhealthy edge for a sick workload.
// The unhealthyReported flag keeps it edge-triggered: without it the 500ms
// beat would append one event per beat for the whole sickness.
func signalUnhealthy(ctx context.Context, logger *zap.Logger, entry *ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID) {
	if entry.unhealthyReported {
		return
	}
	logger.Warn("signaling unhealthy workload")
	payload, err := json.Marshal(workload.Unhealthy{WorkloadID: entry.Spec.ID})
	if err != nil {
		logger.Error("marshal unhealthy event", zap.Error(err))
		return
	}
	event := journal.NewEvent(nodeID, journalview.EventTypeWorkloadUnhealthy, payload)
	err = journalview.RecordEventAndLog(ctx, logger, j, views, event, "workload.unhealthy",
		zap.String("workload_id", entry.Spec.ID.String()),
	)
	if err != nil {
		logger.Error("record event failed", zap.Error(err))
		return
	}
	entry.unhealthyReported = true
}

// samplePressure refreshes host and per-workload utilization and gossips the
// host trio for placement. Sampling failures keep previous readings and log
// at debug: /proc and cgroupfs either work or are permanently broken, never
// flaky.
func samplePressure(logger *zap.Logger, sampler *node.Sampler, running map[uuid.UUID]*ContainerAndProcess, peerService *peerdiscovery.MemberService) {
	pressure, err := sampler.SampleHost()
	if err != nil {
		logger.Debug("sample host pressure", zap.Error(err))
	} else {
		peerService.SetPressure(pressure.CPU, pressure.Mem, pressure.Disk)
		if err := peerService.Publish(); err != nil {
			logger.Debug("publish metadata", zap.Error(err))
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
func sampleWorkloadEntry(logger *zap.Logger, sampler *node.Sampler, entry *ContainerAndProcess) {
	stats, err := entry.Stats()
	if err != nil {
		logger.Debug("sample workload stats", zap.String("id", entry.Spec.ID.String()), zap.Error(err))
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
