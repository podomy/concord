// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package reconciler drives host processes from journal events.
//
// It watches for workload events in the local journal and reconciles
// desired state (workload.Spec records) against actual state (running
// processes). On startup it rebuilds desired state from journal replay.
//
// This is the execution counterpart to peersync (data sync). peersync
// feeds the journal with remote events; this package consumes local
// events to create, start, stop, and restart processes.
package reconciler

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord-sdk/fileart"
	"github.com/podomy/concord/internal/clock"
	"github.com/podomy/concord/internal/cn"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/proc"
	"github.com/podomy/concord/internal/registry"
	"github.com/podomy/concord/internal/workload"
)

// WorkloadEntry holds one running host process: its launcher handle plus
// restart accounting. Spec is stored so the exit path and drift checks
// see port mappings and workload metadata during asynchronous teardown.
type WorkloadEntry struct {
	proc.Handle
	restartAfter time.Time
	// startedAt records when the current process began, so a fast
	// non-zero exit can warn about a likely port conflict.
	startedAt    time.Time
	ExitStatus   *proc.ExitStatus
	Spec         workload.Spec
	Stopping     bool
	restartCount int
	// unhealthyReported is edge-triggered dedup for workload.unhealthy: the
	// fast beat runs every 500ms, so without it a still-sick workload would
	// append one journal event per beat. Set on first report, cleared on
	// recovery. Memory-only: a daemon restart re-reports a still-sick
	// workload once, which is the correct edge after the state loss.
	unhealthyReported bool
	// done closes when handleExitEvent records the exit. The SIGKILL timer
	// watches it and cancels on early exit, so no dead-handle signal fires.
	// Main goroutine only: created in destroyContainer, closed in
	// handleExitEvent. Nil means pre-change entry; timer then waits out
	// the timeout.
	done chan struct{}
}

// ExitEvent carries a process exit status paired with its workload ID to the main loop channel.
type ExitEvent struct {
	// ExitStatus contains the exit code and error outcome of the process execution.
	ExitStatus proc.ExitStatus
	// WorkloadID identifies the specific workload spec associated with the exited process.
	WorkloadID uuid.UUID
}

// BlobEnsurer is the sync fast path the reconciler nudges on a miss.
// It matches peersync.BlobSyncer.Ensure structurally so runtime can pass
// the syncer without reconciler importing peersync.
//
// Decision: package setter, not a direct peersync import or a new RunLoop
// parameter. Grep shows peersync never imports reconciler, so no cycle
// exists today, but a direct import would couple execution to sync and a
// new parameter would churn RunLoop plus every test caller. The setter
// keeps the dependency inverted, preserves all callers, and stays nil
// safe in tests where no syncer is wired.
type BlobEnsurer interface {
	Ensure(ctx context.Context, digests []string) error
}

// blobEnsurer is set by runtime after the registry store exists and
// before the reconciler starts. Nil means no syncer (tests, or nil peer
// service); miss paths then just retry next tick.
var blobEnsurer BlobEnsurer

// SetBlobEnsurer wires the blob syncer into miss handling. Call once
// before RunLoop; runtime owns the ordering.
func SetBlobEnsurer(e BlobEnsurer) {
	blobEnsurer = e
}

// ensureWantedFiles nudges the syncer with this spec's file digests after
// a pull or materialize miss. Ensure registers the digests as wanted and
// fetches now; the start still fails and the next tick retries. A nil
// ensurer or empty digest list is a no-op. Errors stay debug: a miss is
// routine until the background tick converges.
func ensureWantedFiles(ctx context.Context, logger *logs.Logger, spec workload.Spec) {
	if blobEnsurer == nil {
		return
	}
	digests := wantedFileDigests(spec)
	if len(digests) == 0 {
		return
	}
	err := blobEnsurer.Ensure(ctx, digests)
	if err != nil {
		logger.Debug("blob sync requested, retrying next tick", logs.String("id", spec.ID.String()), logs.Error(err))
	}
}

// wantedFileDigests mirrors peersync.WantedFromSpecs for one spec: the
// deduped file pointer digests, first seen order. Skips blanks and
// non-canonical prefixes; full validation lives in the syncer. Image
// layer digests are not listed: the reconciler only learns them inside
// the pull, so image misses retry on the background tick.
func wantedFileDigests(spec workload.Spec) []string {
	seen := make(map[string]struct{}, len(spec.Files))
	var out []string
	for _, file := range spec.Files {
		digest := file.Digest
		if len(digest) < 8 || digest[:7] != "sha256:" {
			continue
		}
		if _, ok := seen[digest]; ok {
			continue
		}
		seen[digest] = struct{}{}
		out = append(out, digest)
	}
	return out
}

// shutdownDrainBound caps the post-cancel drain of in-flight exits. Exits
// already queued record stopped status and free network deterministically;
// anything later is lost with the process. No WaitGroup: monitor
// goroutines are detached and the channel is the accounting, so the bound
// is documented, not exact.
const shutdownDrainBound = 5 * time.Second

// RunLoop watches for workload events and drives host processes.
// It blocks until ctx is cancelled. Always launch as a goroutine.
func RunLoop(
	ctx context.Context,
	logger *logs.Logger,
	nodeID uuid.UUID,
	launcher proc.Launcher,
	j journal.Journal,
	workloads *journalview.Workloads,
	views []journalview.View,
	peerService *peerdiscovery.MemberService,
	sampler *node.Sampler,
) {
	running := map[uuid.UUID]*WorkloadEntry{}
	// blocked counts consecutive ticks each workload waits on same-node
	// dependencies. Crossing blockedTickStallThreshold reports the stall
	// loudly; the wait itself never auto-breaks.
	blocked := map[uuid.UUID]int{}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// The fast beat checks local health; the slow beat converges the world.
	// History captures on its own slow beat so trends cost one append per
	// 5s, never per sample. All three run in this goroutine so the running
	// maps need no locking.
	fastTicker := time.NewTicker(fastTickInterval)
	defer fastTicker.Stop()
	historyTicker := time.NewTicker(historyInterval)
	defer historyTicker.Stop()

	exitEvents := make(chan ExitEvent, 100)

	for {
		select {
		case <-ctx.Done():
			drainExitEvents(ctx, logger, j, nodeID, exitEvents, running, peerService, views)
			return

		case ev := <-exitEvents:
			handleExitEvent(ctx, logger, j, nodeID, ev.WorkloadID, ev.ExitStatus, running, peerService, views)

		case <-ticker.C:
			if isLeader(nodeID, peerService) {
				scheduleWorkloads(ctx, logger, j, workloads, peerService, nodeID, views)
			}

			reconcileTick(ctx, logger, nodeID, launcher, j, workloads, running, blocked, exitEvents, peerService, views, sampler)

		case <-fastTicker.C:
			runFastTick(ctx, logger, sampler, running, j, views, nodeID, peerService)

		case <-historyTicker.C:
			sampler.CaptureTrend()
		}
	}
}

// drainExitEvents processes queued exits after cancel for at most
// shutdownDrainBound, so in-flight exits record stopped status and free
// network deterministically instead of stranding. A detached context
// carries the writes: the parent ctx is already done. It returns early
// after 200ms idle so a quiet shutdown does not pay the full bound;
// the 5s bound only caps a busy queue or racing monitors.
func drainExitEvents(
	ctx context.Context,
	logger *logs.Logger,
	j journal.Journal,
	nodeID uuid.UUID,
	exitEvents <-chan ExitEvent,
	running map[uuid.UUID]*WorkloadEntry,
	peerService *peerdiscovery.MemberService,
	views []journalview.View,
) {
	timeout := time.NewTimer(shutdownDrainBound)
	defer timeout.Stop()
	drainCtx := context.WithoutCancel(ctx)
	idle := time.NewTimer(200 * time.Millisecond)
	defer idle.Stop()
	for {
		select {
		case ev := <-exitEvents:
			handleExitEvent(drainCtx, logger, j, nodeID, ev.WorkloadID, ev.ExitStatus, running, peerService, views)
			if !idle.Stop() {
				select {
				case <-idle.C:
				default:
				}
			}
			idle.Reset(200 * time.Millisecond)
		case <-timeout.C:
			return
		case <-idle.C:
			return
		}
	}
}

// detectSubnetDrift compares the live membership order's subnet index
// against the boot-time index and warns on mismatch. The index stays
// sticky: no auto-migration, no restart, no bridge change, so overlapping
// /24s can never flap into existence mid-tick. The operator action is a
// node restart, which re-picks the index and lets CreateBridge reconcile
// stale gateway addrs on boot. A missing or failing member list skips
// quietly: uncertainty never warns.
func detectSubnetDrift(logger *logs.Logger, nodeID uuid.UUID, peerService *peerdiscovery.MemberService) {
	if peerService == nil {
		return
	}
	members, err := peerService.Members()
	if err != nil {
		logger.Debug("subnet drift check skipped, members failed", logs.Error(err))
		return
	}
	drifted, current, active, err := checkSubnetDrift(members, nodeID)
	if err != nil {
		logger.Debug("subnet drift check skipped, self absent", logs.Error(err))
		return
	}
	if drifted {
		logger.Warn("subnet index drift: membership now selects index, node keeps boot index; restart the node to re-pick its /24",
			logs.Int("current_index", current),
			logs.Int("active_index", active),
			logs.String("node_id", nodeID.String()),
		)
	}
}

// checkSubnetDrift compares the live membership order's subnet index
// against the boot-time index. It reports drift plus both indexes. Self
// absent from the list is an error, not drift: uncertainty never warns.
func checkSubnetDrift(members []peerdiscovery.Node, nodeID uuid.UUID) (bool, int, int, error) {
	active := cn.NodeSubnetIndex()
	current, err := cn.SubnetIndexFor(members, nodeID)
	if err != nil {
		return false, 0, active, fmt.Errorf("subnet index: %w", err)
	}
	return current != active, current, active, nil
}

// reconcileTick reconciles all workload specs for this node against running processes.
func reconcileTick(
	ctx context.Context,
	logger *logs.Logger,
	nodeID uuid.UUID,
	launcher proc.Launcher,
	j journal.Journal,
	workloadsView *journalview.Workloads,
	running map[uuid.UUID]*WorkloadEntry,
	blocked map[uuid.UUID]int,
	exitEvents chan<- ExitEvent,
	peerService *peerdiscovery.MemberService,
	views []journalview.View,
	sampler *node.Sampler,
) {
	// Subnet index is sticky: membership churn never re-picks it. Detect
	// drift every tick and warn; the operator restarts the node to
	// re-pick. See detectSubnetDrift.
	detectSubnetDrift(logger, nodeID, peerService)

	workloadSpecs, err := workloadsView.List(ctx)
	if err != nil {
		logger.Error("list workloads", logs.Error(err))
		return
	}

	for _, spec := range workloadSpecs {
		reconcileWorkloadSpec(ctx, logger, nodeID, launcher, j, spec, running, blocked, exitEvents, peerService, views, sampler, specsByID(workloadSpecs))
	}
	pruneBlocked(blocked, workloadSpecs)

	reapDisplaced(ctx, logger, nodeID, workloadsView, running, peerService)
}

// reapDisplaced stops running processes whose live spec no longer points
// here, and drops exited entries the view has moved on from. Preemption
// unassigns live specs on live nodes; without this the victim process
// would run forever with nobody owning it. Lookups that fail or miss
// leave entries alone: uncertainty never kills.
func reapDisplaced(
	ctx context.Context,
	logger *logs.Logger,
	nodeID uuid.UUID,
	workloadsView *journalview.Workloads,
	running map[uuid.UUID]*WorkloadEntry,
	peerService *peerdiscovery.MemberService,
) {
	for id, entry := range running {
		if entry == nil {
			continue
		}
		spec, err := workloadsView.Get(ctx, id)
		if err != nil {
			logger.Debug("reap lookup failed, leaving entry", logs.String("id", id.String()), logs.Error(err))
			continue
		}
		if spec == nil || spec.Removed || spec.AssignedNodeID == nodeID {
			continue
		}
		if entry.ExitStatus == nil && !entry.Stopping {
			destroyContainer(logger, entry.Spec, running)
			continue
		}
		if entry.ExitStatus != nil {
			delete(running, id)
			peerService.SetWorkloadCount(countRunning(running))
		}
	}
}

// specsByID indexes listed specs for dependency gates.
func specsByID(specs []workload.Spec) map[uuid.UUID]workload.Spec {
	byID := make(map[uuid.UUID]workload.Spec, len(specs))
	for _, spec := range specs {
		byID[spec.ID] = spec
	}
	return byID
}

// blockedTickStallThreshold is the number of consecutive reconcile ticks a
// workload may wait on same-node dependencies before the reconciler reports
// the stall. The wait never auto-breaks: placement correctness outranks
// liveness, and an operator must resolve the cycle. CheckNoCycle stays
// submit-path only; stored or remote specs bypass it, so a hand-written
// cycle stalls here forever by design, loudly.
const blockedTickStallThreshold = 12

// noteBlockedTick counts one more tick of dependency waiting. On crossing
// the stall threshold it warns naming the workload and its dependencies and
// records one workload.unhealthy edge so the stall is visible in the
// journal, not just logs. Later ticks stay debug-quiet until progress
// resets the count.
func noteBlockedTick(ctx context.Context, logger *logs.Logger, j journal.Journal, views []journalview.View, nodeID uuid.UUID, spec workload.Spec, blocked map[uuid.UUID]int) {
	if blocked == nil {
		return
	}
	blocked[spec.ID]++
	if blocked[spec.ID] != blockedTickStallThreshold {
		return
	}
	logger.Warn("dependency stall: workload waits on dependencies",
		logs.String("id", spec.ID.String()),
		logs.Strings("depends_on", workloadIDStrings(spec.DependsOn)),
		logs.Int("blocked_ticks", blocked[spec.ID]),
	)
	if j == nil {
		return
	}
	payload, err := json.Marshal(workload.Unhealthy{WorkloadID: spec.ID})
	if err != nil {
		logger.Error("marshal unhealthy event", logs.Error(err))
		return
	}
	event := journal.NewEvent(nodeID, journalview.EventTypeWorkloadUnhealthy, payload)
	if err := journalview.RecordEventAndLog(ctx, logger, j, views, event, "workload.unhealthy",
		logs.String("workload_id", spec.ID.String()),
	); err != nil {
		logger.Error("record event failed", logs.Error(err))
	}
}

// pruneBlocked drops stall counters for specs that left the view, so a
// removed workload never carries a stale count into a future same-ID spec.
func pruneBlocked(blocked map[uuid.UUID]int, specs []workload.Spec) {
	if blocked == nil {
		return
	}
	live := make(map[uuid.UUID]struct{}, len(specs))
	for _, spec := range specs {
		live[spec.ID] = struct{}{}
	}
	for id := range blocked {
		if _, ok := live[id]; !ok {
			delete(blocked, id)
		}
	}
}

// workloadIDStrings renders dependency IDs for stall logs.
func workloadIDStrings(ids []uuid.UUID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		out = append(out, id.String())
	}
	return out
}

// depsReady reports whether spec's same-node dependencies are running. A
// dependency gates only when it is live and assigned to this node:
// unknown, removed, unassigned, or elsewhere-assigned IDs are ignored,
// never waited on, and the spec itself is skipped so a hand-written
// cycle cannot stall it forever. Health is not consulted: flapping
// health must not stall dependents in sympathy.
func depsReady(running map[uuid.UUID]*WorkloadEntry, specs map[uuid.UUID]workload.Spec, nodeID uuid.UUID, spec workload.Spec) bool {
	for _, dep := range spec.DependsOn {
		if dep == spec.ID {
			continue
		}
		depSpec, ok := specs[dep]
		if !ok || depSpec.Removed || depSpec.AssignedNodeID != nodeID {
			continue
		}
		entry, ok := running[dep]
		if !ok || entry == nil || entry.ExitStatus != nil {
			return false
		}
	}
	return true
}

// reconcileRemovedSpec handles cleanup for specs that have been marked as removed.
func reconcileRemovedSpec(
	logger *logs.Logger,
	spec workload.Spec,
	running map[uuid.UUID]*WorkloadEntry,
	entry *WorkloadEntry,
	peerService *peerdiscovery.MemberService,
) {
	if entry == nil {
		return
	}
	if !entry.Stopping && entry.ExitStatus == nil {
		destroyContainer(logger, spec, running)
	}
	if entry.ExitStatus != nil {
		delete(running, spec.ID)
		peerService.SetWorkloadCount(countRunning(running))
	}
}

// reconcileWorkloadSpec checks a single workload spec and drives process creation, restart, or removal.
func reconcileWorkloadSpec(
	ctx context.Context,
	logger *logs.Logger,
	nodeID uuid.UUID,
	launcher proc.Launcher,
	j journal.Journal,
	spec workload.Spec,
	running map[uuid.UUID]*WorkloadEntry,
	blocked map[uuid.UUID]int,
	exitEvents chan<- ExitEvent,
	peerService *peerdiscovery.MemberService,
	views []journalview.View,
	sampler *node.Sampler,
	specs map[uuid.UUID]workload.Spec,
) {
	if spec.AssignedNodeID != nodeID {
		return
	}

	entry, exists := running[spec.ID]

	// 1. Spec was removed -> stop process if running, or purge entry if exited.
	if spec.Removed {
		delete(blocked, spec.ID)
		reconcileRemovedSpec(logger, spec, running, entry, peerService)
		return
	}

	// 2. Spec drift on a running process -> stop it; the exited
	// entry below picks up the new spec after the normal exit cleanup.
	// Checked before the running early-return so drift always stops.
	// Behavior change: image drift alone used to restart; now HostPort,
	// ContainerPort, Files, and Resources drift restart the same way, so
	// a same-image port or resource edit converges instead of sticking
	// on the old process.
	if stopOnDrift(logger, spec, entry, exists, running) {
		return
	}

	// 3. Workload is actively running -> nothing to do.
	if exists && (entry == nil || entry.ExitStatus == nil) {
		return
	}

	// 4. Workload previously exited -> restart, update, or wait.
	if exists && entry != nil && entry.ExitStatus != nil {
		if !handleExitedEntry(entry, spec, running, peerService) {
			return
		}
	}

	// 5. Start new container instance, gated on same-node dependencies.
	if !depsReady(running, specs, nodeID, spec) {
		noteBlockedTick(ctx, logger, j, views, nodeID, spec, blocked)
		logger.Debug("waiting on dependencies", logs.String("id", spec.ID.String()))
		return
	}
	delete(blocked, spec.ID)
	startProcess(ctx, logger, launcher, j, nodeID, spec, running, exitEvents, peerService, views, sampler)
}

// stopOnDrift stops a running process whose spec differs from the
// view, reporting whether it acted. The exited entry picks up the new
// spec through the normal exit path on a later tick.
// Drift covers image plus HostPort, ContainerPort, Files, and Resources:
// same-image edits to those fields restart through this same path.
// Other fields (command, env, QoS, health, etc.) do not trigger a
// restart here; they take effect on the next natural restart.
func stopOnDrift(
	logger *logs.Logger,
	spec workload.Spec,
	entry *WorkloadEntry,
	exists bool,
	running map[uuid.UUID]*WorkloadEntry,
) bool {
	if !exists || entry == nil || entry.ExitStatus != nil || entry.Stopping || !specDrifted(entry.Spec, spec) {
		return false
	}
	logger.Info("spec update, stopping old process",
		logs.String("id", spec.ID.String()))
	destroyContainer(logger, entry.Spec, running)
	return true
}

// specDrifted reports whether the live spec needs a restart to converge:
// image, ports, files, or resources changed. Same-image edits to ports,
// files, or resources restart through the image-drift path.
func specDrifted(old, next workload.Spec) bool {
	if old.Image != next.Image {
		return true
	}
	if old.HostPort != next.HostPort || old.ContainerPort != next.ContainerPort {
		return true
	}
	if old.Resources != next.Resources {
		return true
	}
	if len(old.Files) != len(next.Files) {
		return true
	}
	for i := range old.Files {
		if old.Files[i] != next.Files[i] {
			return true
		}
	}
	return false
}

// handleExitedEntry processes an exited entry and reports whether to
// start a fresh process now. An explicit spec update (ports, files, or
// resources) drops policy but keeps the backoff: the operator ordered a
// new spec, so a Never policy still restarts, but the normal cooldown
// still paces a hot crash loop. Otherwise the restart policy decides.
func handleExitedEntry(entry *WorkloadEntry, spec workload.Spec, running map[uuid.UUID]*WorkloadEntry, peerService *peerdiscovery.MemberService) bool {
	if specDrifted(entry.Spec, spec) {
		// Drift restarts ride the same cooldown as normal restarts.
		// Without this a crash-looping workload that also drifted
		// would bypass backoff on every tick.
		if entry.restartAfter.After(clock.Now()) {
			return false
		}
		delete(running, spec.ID)
		peerService.SetWorkloadCount(countRunning(running))
		return true
	}
	return evaluateRestart(entry, spec, running, peerService)
}

// evaluateRestart applies the restart policy to an exited entry. It
// reports whether to start a new instance now, clearing the old entry
// when policy allows. Cooling down or policy refusal both mean wait.
func evaluateRestart(entry *WorkloadEntry, spec workload.Spec, running map[uuid.UUID]*WorkloadEntry, peerService *peerdiscovery.MemberService) bool {
	if !shouldRestartWorkload(spec.Restart, *entry.ExitStatus) {
		return false // Restart policy dictates no restart; retain exited status entry.
	}
	if entry.restartAfter.After(clock.Now()) {
		// We are still cooling down, can't restart now.
		return false
	}

	delete(running, spec.ID) // Policy allows restart; clear old entry for new process start.
	peerService.SetWorkloadCount(countRunning(running))
	return true
}

// logDeviceSummary records the device rules about to be programmed: how
// many, and whether null is allowed. The eBPF program's content lives
// and dies inside one Start call, unobservable from outside, so this
// line pins our exact input in every run's logs: same input through the
// deterministic compiler means the same program.
// registryAddr returns the embedded registry address for file pulls.
// Loopback only: artifacts never cross the network except through the
// registry's own peer sync.
func registryAddr() string {
	return "127.0.0.1:" + strconv.Itoa(registry.Port)
}

// stageFetch pulls one blob for staging. A var so unit tests stub the
// registry with canned bytes; production pulls from the embedded
// registry over loopback.
var stageFetch = func(ctx context.Context, digest, dest string) error {
	artifacts := fileart.NewClient(registryAddr())
	return artifacts.PullTo(ctx, digest, dest)
}

// startProcess stages the spec files, resolves the binary, and starts it
// on the host. Files stage before the process exists: the process can
// never observe a half-written set, and a missing blob (sync has not
// arrived) fails the start so the next tick retries. A required blob
// that never arrives blocks this workload forever by design; it must
// not start without its files. See docs/files.
func startProcess(
	ctx context.Context,
	logger *logs.Logger,
	launcher proc.Launcher,
	j journal.Journal,
	nodeID uuid.UUID,
	spec workload.Spec,
	running map[uuid.UUID]*WorkloadEntry,
	exitEvents chan<- ExitEvent,
	peerService *peerdiscovery.MemberService,
	views []journalview.View,
	sampler *node.Sampler,
) {
	stageDir, err := stageDirPath(spec.ID)
	if err != nil {
		logger.Error("stage dir", logs.Error(err))
		return
	}

	var binPath string
	if len(spec.Files) > 0 {
		binPath, err = proc.StageFiles(ctx, stageFetch, spec, stageDir)
		if err != nil {
			logger.Debug("stage files, retrying next tick", logs.String("id", spec.ID.String()), logs.Error(err))
			ensureWantedFiles(ctx, logger, spec)
			return
		}
	} else {
		logger.Error("no files to stage, workload has no binary", logs.String("id", spec.ID.String()))
		return
	}

	cfg := buildStartConfig(spec, binPath, sampler)
	logger.Info("start workload process", logs.String("workload_id", spec.ID.String()), logs.Strings("args", cfg.Args), logs.Int("mem_mb", int(spec.Resources.MemoryMB)))
	handle, err := launcher.Start(ctx, cfg)
	if err != nil {
		logger.Error("start process", logs.Error(err))
		return
	}

	entry := WorkloadEntry{
		Handle:    handle,
		Spec:      spec,
		startedAt: clock.Now(),
		done:      make(chan struct{}),
	}
	running[spec.ID] = &entry
	peerService.SetWorkloadCount(countRunning(running))

	// Spawn background exit monitor so both natural process exits and forced shutdowns
	// forward process termination events back to the main loop via exitEvents channel.
	go monitorProcessExit(ctx, spec.ID, handle, exitEvents)

	recordInstanceEvent(ctx, logger, j, views, spec, nodeID, workload.StateRunning, handle.PID())
}

// monitorProcessExit waits on Handle.Exited() and forwards the exit status to exitEvents.
func monitorProcessExit(
	ctx context.Context,
	id uuid.UUID,
	handle proc.Handle,
	exitEvents chan<- ExitEvent,
) {
	select {
	case exitStatus := <-handle.Exited():
		if exitEvents != nil {
			select {
			case exitEvents <- ExitEvent{WorkloadID: id, ExitStatus: exitStatus}:
			case <-ctx.Done():
			}
		}
	case <-ctx.Done():
	}
}

// stopTimeout returns the graceful shutdown timeout duration for a workload spec.
// If StopTimeoutSeconds is <= 0, it defaults to 60 seconds.
func stopTimeout(spec workload.Spec) time.Duration {
	if spec.StopTimeoutSeconds > 0 {
		return time.Duration(spec.StopTimeoutSeconds) * time.Second
	}
	return 60 * time.Second
}

// destroyContainer initiates asynchronous shutdown of a process by sending SIGTERM.
// If SIGTERM times out after stopTimeout(spec), a SIGKILL fallback is sent
// unless the target already exited: the timer watches the entry done
// channel closed by handleExitEvent and cancels on early exit, so no
// dead-handle signal fires and no PID-reuse risk remains.
func destroyContainer(
	logger *logs.Logger,
	spec workload.Spec,
	running map[uuid.UUID]*WorkloadEntry,
) {
	entry, exists := running[spec.ID]
	if !exists || entry == nil || entry.Stopping {
		return
	}

	entry.Stopping = true
	if entry.done == nil {
		entry.done = make(chan struct{})
	}
	done := entry.done
	logger.Debug("stopping process", logs.String("id", spec.ID.String()))
	if err := entry.Signal(syscall.SIGTERM); err != nil {
		logger.Error("SIGTERM failed", logs.String("id", spec.ID.String()), logs.Error(err))
	}

	handle := entry.Handle
	id := spec.ID
	timeout := stopTimeout(spec)

	// Launch background fallback timer for SIGKILL escalation if SIGTERM times out.
	// Detached from ctx on purpose: daemon shutdown cancels ctx, and
	// aborting the timer there would strand SIGTERM-ignoring processes as
	// orphans with no escalation. The timer also watches done: a clean
	// exit before the timeout cancels escalation, so SIGKILL never fires
	// at a dead handle or a recycled PID.
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-done:
			return
		case <-timer.C:
		}
		select {
		case <-done:
			return
		default:
		}
		logger.Warn("process SIGTERM timed out, sending SIGKILL", logs.String("id", id.String()))
		if err := handle.Signal(syscall.SIGKILL); err != nil {
			logger.Error("SIGKILL failed", logs.String("id", id.String()), logs.Error(err))
		}
	}()
}

// shouldRestartWorkload determines if a workload should be restarted based on its restart policy and exit status.
func shouldRestartWorkload(policy workload.RestartPolicy, status proc.ExitStatus) bool {
	switch policy {
	case workload.RestartAlways:
		return true
	case workload.RestartOnFailure:
		return status.Code != 0 || status.Err != nil
	case workload.RestartNever:
		return false
	default:
		return false
	}
}

// logProcessExit logs process exit status at appropriate log levels.
func logProcessExit(logger *logs.Logger, id uuid.UUID, status proc.ExitStatus) {
	if status.Err != nil {
		logger.Error("process exit error", logs.String("id", id.String()), logs.Int("code", status.Code), logs.Error(status.Err))
	} else {
		logger.Info("process exited cleanly", logs.String("id", id.String()), logs.Int("code", status.Code))
	}
}

// handleExitEvent processes process exit events in the main event loop,
// recording the stopped instance state. It closes the entry done channel
// so a pending SIGKILL timer cancels.
func handleExitEvent(
	ctx context.Context,
	logger *logs.Logger,
	j journal.Journal,
	nodeID uuid.UUID,
	id uuid.UUID,
	status proc.ExitStatus,
	running map[uuid.UUID]*WorkloadEntry,
	peerService *peerdiscovery.MemberService,
	views []journalview.View,
) {
	entry, exists := running[id]
	if !exists || entry == nil {
		return
	}

	logProcessExit(logger, id, status)

	entry.ExitStatus = &status
	entry.restartCount++
	entry.restartAfter = clock.Now().Add(backOff(entry.restartCount))
	if entry.done != nil {
		select {
		case <-entry.done:
		default:
			close(entry.done)
		}
	}

	if status.Code != 0 && entry.Spec.HostPort != 0 && clock.Now().Sub(entry.startedAt) < time.Second {
		logger.Warn("process exited fast, possible port conflict",
			logs.String("id", id.String()),
			logs.Int("code", status.Code),
			logs.Uint16("host_port", entry.Spec.HostPort),
		)
	}

	recordInstanceEvent(ctx, logger, j, views, entry.Spec, nodeID, workload.StateStopped, 0)
	peerService.SetWorkloadCount(countRunning(running))
}

// countRunning returns the number of active, non-exited workloads in the running map.
func countRunning(running map[uuid.UUID]*WorkloadEntry) int {
	count := 0
	for _, entry := range running {
		if entry != nil && entry.ExitStatus == nil {
			count++
		}
	}
	return count
}

// buildStartConfig renders the launcher start config from the spec and
// the staged binary path. A spec command runs wholesale; an empty spec
// command runs the binary bare. Container output flows into the
// sampler's per-workload log rings; a nil sampler leaves writers unset
// and output discards as before.
func buildStartConfig(spec workload.Spec, binPath string, sampler *node.Sampler) proc.StartConfig {
	args := spec.Command

	env := make([]string, 0, len(spec.Env)+1)
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}
	// CONCORD_MEM_LIMIT_MB carries the scheduled memory request into
	// the process so memory-aware binaries can consume up to their cap.
	// Additive only: binaries that ignore unknown env are unaffected.
	if spec.Resources.MemoryMB > 0 {
		env = append(env, "CONCORD_MEM_LIMIT_MB="+strconv.FormatInt(spec.Resources.MemoryMB, 10))
	}

	cfg := proc.StartConfig{
		ID:      spec.ID,
		BinPath: binPath,
		Args:    args,
		Env:     env,
		Dir:     filepath.Dir(binPath),
	}
	if sampler != nil {
		cfg.Stdout = sampler.LogWriter(spec.ID, node.LogStdout)
		cfg.Stderr = sampler.LogWriter(spec.ID, node.LogStderr)
	}

	return cfg
}

// recordInstanceEvent writes a workload instance state event to the journal
// and applies it to views, like every other recorded event. Instance
// events previously appended journal bytes without applying views, so
// live generic indexes missed them until the next rebuild.
func recordInstanceEvent(
	ctx context.Context,
	logger *logs.Logger,
	j journal.Journal,
	views []journalview.View,
	spec workload.Spec,
	nodeID uuid.UUID,
	state workload.State,
	pid int,
) {
	inst := workload.Instance{
		State:  state,
		ID:     uuid.New(),
		SpecID: spec.ID,
		NodeID: nodeID,
		PID:    pid,
	}

	payload, err := json.Marshal(inst)
	if err != nil {
		logger.Error("marshal instance", logs.Error(err))
		return
	}

	event := journal.NewEvent(nodeID, journalview.EventTypeWorkloadInstancePrefix+string(state), payload)
	if err := journalview.RecordEvent(ctx, j, views, event); err != nil {
		logger.Error("record instance event", logs.Error(err))
	}
}

// stageDirPath returns the staging directory path for a given spec ID.
func stageDirPath(specID uuid.UUID) (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config dir: %w", err)
	}

	return filepath.Join(dir, "concord", "stages", specID.String()), nil
}

// backOff computes an exponential backoff duration based on restart attempts, capped at 60s.
func backOff(restartCount int) time.Duration {
	maxBackoff := time.Second * 60

	duration := time.Duration(restartCount*2) * time.Second

	if duration > maxBackoff {
		return maxBackoff
	}

	return duration
}
