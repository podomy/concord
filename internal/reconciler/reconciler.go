// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package reconciler drives container lifecycle from journal events.
//
// It watches for workload events in the local journal and reconciles
// desired state (workload.Spec records) against actual state (running
// libcontainer instances). On startup it rebuilds desired state from
// journal replay.
//
// This is the execution counterpart to peersync (data sync). peersync
// feeds the journal with remote events; this package consumes local
// events to create, start, stop, and restart containers.
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
	cdevices "github.com/opencontainers/cgroups/devices/config"
	"github.com/opencontainers/runc/libcontainer"
	"github.com/opencontainers/runc/libcontainer/configs"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/clock"
	"github.com/podomy/concord/internal/cn"
	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/registry"
	"github.com/podomy/concord/internal/workload"
	"github.com/podomy/concord/sdk/fileart"
)

// ContainerAndProcess holds container state, process handle, stopping status, and exit status.
// Spec is stored so handleExitEvent can access port mappings, veth interfaces, and
// workload metadata during asynchronous container teardown.
type ContainerAndProcess struct {
	*libcontainer.Container
	cr.ProcessHandle
	restartAfter time.Time
	ExitStatus   *cr.ExitStatus
	Spec         workload.Spec
	Stopping     bool
	restartCount int
	// unhealthyReported is edge-triggered dedup for workload.unhealthy: the
	// fast beat runs every 500ms, so without it a still-sick workload would
	// append one journal event per beat. Set on first report, cleared on
	// recovery. Memory-only: a daemon restart re-reports a still-sick
	// workload once, which is the correct edge after the state loss.
	unhealthyReported bool
}

// ExitEvent carries a process exit status paired with its workload ID to the main loop channel.
type ExitEvent struct {
	// ExitStatus contains the exit code and error outcome of the process execution.
	ExitStatus cr.ExitStatus
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
func ensureWantedFiles(ctx context.Context, logger *zap.Logger, spec workload.Spec) {
	if blobEnsurer == nil {
		return
	}
	digests := wantedFileDigests(spec)
	if len(digests) == 0 {
		return
	}
	err := blobEnsurer.Ensure(ctx, digests)
	if err != nil {
		logger.Debug("blob sync requested, retrying next tick", zap.String("id", spec.ID.String()), zap.Error(err))
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

// RunLoop watches for workload events and drives the container lifecycle.
// It blocks until ctx is cancelled. Always launch as a goroutine.
func RunLoop(
	ctx context.Context,
	logger *zap.Logger,
	nodeID uuid.UUID,
	puller cr.Puller,
	runtime cr.Runner,
	j journal.Journal,
	workloads *journalview.Workloads,
	views []journalview.View,
	peerService *peerdiscovery.MemberService,
	sampler *node.Sampler,
) {
	running := map[uuid.UUID]*ContainerAndProcess{}
	ipAndCIDRs := map[uuid.UUID]string{}

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

	// On restart or full shutdown of concord we pickup the containers that were running.
	stateDir, err := cr.StateDirPath()
	if err != nil {
		logger.Error("State dir path", zap.Error(err))
	}
	adoptContainers(ctx, logger, nodeID, running, workloads, stateDir, exitEvents)

	for {
		select {
		case <-ctx.Done():
			return

		case ev := <-exitEvents:
			handleExitEvent(ctx, logger, j, nodeID, ev.WorkloadID, ev.ExitStatus, running, ipAndCIDRs, peerService, views)

		case <-ticker.C:
			if isLeader(nodeID, peerService) {
				scheduleWorkloads(ctx, logger, j, workloads, peerService, nodeID, views)
			}

			reconcileTick(ctx, logger, nodeID, puller, runtime, j, workloads, running, ipAndCIDRs, exitEvents, peerService, views, sampler)

		case <-fastTicker.C:
			runFastTick(ctx, logger, sampler, running, j, views, nodeID, peerService)

		case <-historyTicker.C:
			sampler.CaptureTrend()
		}
	}
}

// reconcileTick reconciles all workload specs for this node against running containers.
func reconcileTick(
	ctx context.Context,
	logger *zap.Logger,
	nodeID uuid.UUID,
	puller cr.Puller,
	runtime cr.Runner,
	j journal.Journal,
	workloadsView *journalview.Workloads,
	running map[uuid.UUID]*ContainerAndProcess,
	ipAndCIDRs map[uuid.UUID]string,
	exitEvents chan<- ExitEvent,
	peerService *peerdiscovery.MemberService,
	views []journalview.View,
	sampler *node.Sampler,
) {
	workloadSpecs, err := workloadsView.List(ctx)
	if err != nil {
		logger.Error("list workloads", zap.Error(err))
		return
	}

	for _, spec := range workloadSpecs {
		reconcileWorkloadSpec(ctx, logger, nodeID, puller, runtime, j, spec, running, ipAndCIDRs, exitEvents, peerService, views, sampler, specsByID(workloadSpecs))
	}

	reapDisplaced(ctx, logger, nodeID, workloadsView, running, peerService)
}

// reapDisplaced stops running containers whose live spec no longer points
// here, and drops exited entries the view has moved on from. Preemption
// unassigns live specs on live nodes; without this the victim container
// would run forever with nobody owning it. Lookups that fail or miss
// leave entries alone: uncertainty never kills.
func reapDisplaced(
	ctx context.Context,
	logger *zap.Logger,
	nodeID uuid.UUID,
	workloadsView *journalview.Workloads,
	running map[uuid.UUID]*ContainerAndProcess,
	peerService *peerdiscovery.MemberService,
) {
	for id, entry := range running {
		if entry == nil {
			continue
		}
		spec, err := workloadsView.Get(ctx, id)
		if err != nil {
			logger.Debug("reap lookup failed, leaving entry", zap.String("id", id.String()), zap.Error(err))
			continue
		}
		if spec == nil || spec.Removed || spec.AssignedNodeID == nodeID {
			continue
		}
		if entry.ExitStatus == nil && !entry.Stopping {
			destroyContainer(ctx, logger, entry.Spec, running)
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

// depsReady reports whether spec's same-node dependencies are running. A
// dependency gates only when it is live and assigned to this node:
// unknown, removed, unassigned, or elsewhere-assigned IDs are ignored,
// never waited on, and the spec itself is skipped so a hand-written
// cycle cannot stall it forever. Health is not consulted: flapping
// health must not stall dependents in sympathy.
func depsReady(running map[uuid.UUID]*ContainerAndProcess, specs map[uuid.UUID]workload.Spec, nodeID uuid.UUID, spec workload.Spec) bool {
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
	ctx context.Context,
	logger *zap.Logger,
	spec workload.Spec,
	running map[uuid.UUID]*ContainerAndProcess,
	entry *ContainerAndProcess,
	peerService *peerdiscovery.MemberService,
) {
	if entry == nil {
		return
	}
	if !entry.Stopping && entry.ExitStatus == nil {
		destroyContainer(ctx, logger, spec, running)
	}
	if entry.ExitStatus != nil {
		delete(running, spec.ID)
		peerService.SetWorkloadCount(countRunning(running))
	}
}

// reconcileWorkloadSpec checks a single workload spec and drives container creation, restart, or removal.
func reconcileWorkloadSpec(
	ctx context.Context,
	logger *zap.Logger,
	nodeID uuid.UUID,
	puller cr.Puller,
	runtime cr.Runner,
	j journal.Journal,
	spec workload.Spec,
	running map[uuid.UUID]*ContainerAndProcess,
	ipAndCIDRs map[uuid.UUID]string,
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
		reconcileRemovedSpec(ctx, logger, spec, running, entry, peerService)
		return
	}

	// 2. Image drift on a running container -> stop it; the exited
	// entry below picks up the new image after the normal exit cleanup
	// (veth and port mappings free through the exit path, never leak).
	// Checked before the running early-return so drift always stops.
	if stopOnDrift(ctx, logger, spec, entry, exists, running) {
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
		logger.Debug("waiting on dependencies", zap.String("id", spec.ID.String()))
		return
	}
	startContainer(ctx, logger, puller, runtime, j, nodeID, spec, running, ipAndCIDRs, exitEvents, peerService, views, sampler)
}

// stopOnDrift stops a running container whose image differs from the
// view, reporting whether it acted. The exited entry picks up the new
// image through the normal exit path on a later tick, after veth and
// port mappings free through exit cleanup.
func stopOnDrift(
	ctx context.Context,
	logger *zap.Logger,
	spec workload.Spec,
	entry *ContainerAndProcess,
	exists bool,
	running map[uuid.UUID]*ContainerAndProcess,
) bool {
	if !exists || entry == nil || entry.ExitStatus != nil || entry.Stopping || entry.Spec.Image == spec.Image {
		return false
	}
	logger.Info("image update, stopping old container",
		zap.String("id", spec.ID.String()),
		zap.String("image", spec.Image))
	destroyContainer(ctx, logger, entry.Spec, running)
	return true
}

// handleExitedEntry processes an exited entry and reports whether to
// start a fresh container now. An explicit image update drops policy
// and backoff: the operator ordered a new image, and the old container
// already exited and cleaned up. Otherwise the restart policy decides.
func handleExitedEntry(entry *ContainerAndProcess, spec workload.Spec, running map[uuid.UUID]*ContainerAndProcess, peerService *peerdiscovery.MemberService) bool {
	if entry.Spec.Image != spec.Image {
		delete(running, spec.ID)
		peerService.SetWorkloadCount(countRunning(running))
		return true
	}
	return evaluateRestart(entry, spec, running, peerService)
}

// evaluateRestart applies the restart policy to an exited entry. It
// reports whether to start a new instance now, clearing the old entry
// when policy allows. Cooling down or policy refusal both mean wait.
func evaluateRestart(entry *ContainerAndProcess, spec workload.Spec, running map[uuid.UUID]*ContainerAndProcess, peerService *peerdiscovery.MemberService) bool {
	if !shouldRestartWorkload(spec.Restart, *entry.ExitStatus) {
		return false // Restart policy dictates no restart; retain exited status entry.
	}
	if entry.restartAfter.After(clock.Now()) {
		// We are still cooling down, can't restart now.
		return false
	}

	delete(running, spec.ID) // Policy allows restart; clear old entry for new container start.
	peerService.SetWorkloadCount(countRunning(running))
	return true
}

// logDeviceSummary records the device rules about to be programmed: how
// many, and whether null is allowed. The eBPF program's content lives
// and dies inside one Start call, unobservable from outside, so this
// line pins our exact input in every run's logs: same input through the
// deterministic compiler means the same program.
func logDeviceSummary(logger *zap.Logger, id uuid.UUID, cfg *configs.Config) {
	rules := 0
	null := false
	if cfg.Cgroups != nil && cfg.Cgroups.Resources != nil {
		rules = len(cfg.Cgroups.Devices)
		for _, rule := range cfg.Cgroups.Devices {
			if rule.Type == cdevices.CharDevice && rule.Major == 1 && rule.Minor == 3 && rule.Allow && string(rule.Permissions) == "rwm" {
				null = true
			}
		}
	}
	logger.Debug("device rules for container", zap.String("id", id.String()), zap.Int("rules", rules), zap.Bool("null_allowed", null))
}

// setupContainerNetwork configures veth pairs and host port mappings for a started container process.
// If network setup fails, it performs anti-leak cleanup by killing the process and destroying container resources.
func setupContainerNetwork(
	ctx context.Context,
	logger *zap.Logger,
	ctr *libcontainer.Container,
	processHandle cr.ProcessHandle,
	spec workload.Spec,
	ipAndCIDRs map[uuid.UUID]string,
) error {
	ipAndCIDRstring, err := cn.CreateVethPair(ctx, spec.ID.String(), processHandle.NamespacePID())
	if err != nil {
		logger.Error("create veth pair", zap.Error(err))
		cleanupContainerStartFailure(logger, ctr, processHandle)
		return fmt.Errorf("create veth pair: %w", err)
	}

	ipAndCIDRs[spec.ID] = ipAndCIDRstring

	if spec.HostPort == 0 {
		return nil
	}

	err = cn.AddPortMapping(ctx, spec.HostPort, ipAndCIDRstring, spec.ContainerPort)
	if err != nil {
		logger.Error("add port mapping", zap.Error(err))
		if linkErr := cn.DeleteLink(cn.VethHostName(spec.ID.String(), cn.VethA)); linkErr != nil {
			logger.Error("delete veth link on port mapping failure", zap.Error(linkErr))
		}
		delete(ipAndCIDRs, spec.ID)
		cleanupContainerStartFailure(logger, ctr, processHandle)
		return fmt.Errorf("add port mapping: %w", err)
	}

	return nil
}

func cleanupContainerStartFailure(logger *zap.Logger, ctr *libcontainer.Container, processHandle cr.ProcessHandle) {
	logger.Warn("destroying container after failed start")
	if sigErr := processHandle.Signal(syscall.SIGKILL); sigErr != nil {
		logger.Error("kill process on network setup failure", zap.Error(sigErr))
	}
	if destroyErr := ctr.Destroy(); destroyErr != nil {
		logger.Error("destroy container on network setup failure", zap.Error(destroyErr))
	}
}

// registryAddr returns the embedded registry address for file pulls.
// Loopback only: artifacts never cross the network except through the
// registry's own peer sync.
func registryAddr() string {
	return "127.0.0.1:" + strconv.Itoa(registry.Port)
}

// startContainer pulls the image, builds the bundle, creates the container,
// and starts the init process.
func startContainer(
	ctx context.Context,
	logger *zap.Logger,
	puller cr.Puller,
	runtime cr.Runner,
	j journal.Journal,
	nodeID uuid.UUID,
	spec workload.Spec,
	running map[uuid.UUID]*ContainerAndProcess,
	ipAndCIDRs map[uuid.UUID]string,
	exitEvents chan<- ExitEvent,
	peerService *peerdiscovery.MemberService,
	views []journalview.View,
	sampler *node.Sampler,
) {
	bundleDir, err := bundleDirPath(spec.ID)
	if err != nil {
		logger.Error("bundle dir", zap.Error(err))
		return
	}

	pullResult, err := puller.Pull(ctx, spec.Image, bundleDir)
	if err != nil {
		logger.Error("pull image", zap.String("image", spec.Image), zap.Error(err))
		ensureWantedFiles(ctx, logger, spec)
		return
	}

	// Files materialize into the rootfs before the container exists: the
	// process can never observe a half-written set, and a missing blob
	// (sync has not arrived) fails the start so the next tick retries.
	// A required blob that never arrives blocks this workload forever by
	// design; it must not start without its files. See docs/files.
	if len(spec.Files) > 0 {
		artifacts := fileart.NewClient(registryAddr())
		refs := make([]fileart.File, 0, len(spec.Files))
		for _, file := range spec.Files {
			refs = append(refs, fileart.File{Path: file.Path, Digest: file.Digest, Mode: file.Mode})
		}
		fetch := func(fetchCtx context.Context, digest, dest string) error {
			return artifacts.PullTo(fetchCtx, digest, dest)
		}
		if err := fileart.Materialize(ctx, pullResult.RootFS, refs, fetch); err != nil {
			logger.Debug("materialize files, retrying next tick", zap.String("id", spec.ID.String()), zap.Error(err))
			ensureWantedFiles(ctx, logger, spec)
			return
		}
	}

	cfg, err := cr.BundleBuilder(ctx, *pullResult, spec)
	if err != nil {
		logger.Error("build bundle", zap.Error(err))
		return
	}
	logDeviceSummary(logger, spec.ID, cfg)

	ctr, err := runtime.Create(spec.ID.String(), cfg)
	if err != nil {
		logger.Error("create container", zap.Error(err))
		return
	}

	proc, err := buildProcess(spec, pullResult, sampler)
	if err != nil {
		logger.Error("build process", zap.Error(err))
		return
	}
	processHandle, err := runtime.Start(ctr, proc)
	if err != nil {
		logger.Error("start container", zap.Error(err))
		destroyErr := ctr.Destroy()
		if destroyErr != nil {
			logger.Error("destroy container on start failure", zap.Error(destroyErr))
		}
		return
	}

	if err := setupContainerNetwork(ctx, logger, ctr, processHandle, spec, ipAndCIDRs); err != nil {
		return
	}

	containerAndProcess := ContainerAndProcess{
		Container:     ctr,
		ProcessHandle: processHandle,
		Spec:          spec,
	}
	running[spec.ID] = &containerAndProcess
	peerService.SetWorkloadCount(countRunning(running))

	// Spawn background exit monitor so both natural process exits and forced shutdowns
	// forward process termination events back to the main loop via exitEvents channel.
	go monitorProcessExit(ctx, spec.ID, processHandle, exitEvents)

	recordInstanceEvent(ctx, logger, j, views, spec, nodeID, workload.StateRunning, processHandle.NamespacePID())
}

// monitorProcessExit waits on ProcessHandle.Exited() and forwards the exit status to exitEvents.
func monitorProcessExit(
	ctx context.Context,
	id uuid.UUID,
	proc cr.ProcessHandle,
	exitEvents chan<- ExitEvent,
) {
	select {
	case exitStatus := <-proc.Exited():
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

// destroyContainer initiates asynchronous shutdown of a container by sending SIGTERM.
// If SIGTERM times out after stopTimeout(spec), a SIGKILL fallback is sent.
func destroyContainer(
	ctx context.Context,
	logger *zap.Logger,
	spec workload.Spec,
	running map[uuid.UUID]*ContainerAndProcess,
) {
	entry, exists := running[spec.ID]
	if !exists || entry == nil || entry.Stopping {
		return
	}

	entry.Stopping = true
	logger.Debug("stopping container", zap.String("id", spec.ID.String()))
	if err := entry.ProcessHandle.Signal(syscall.SIGTERM); err != nil {
		logger.Error("SIGTERM failed", zap.String("id", spec.ID.String()), zap.Error(err))
	}

	proc := entry.ProcessHandle
	id := spec.ID
	timeout := stopTimeout(spec)

	// Launch background fallback timer for SIGKILL escalation if SIGTERM times out.
	go func() {
		timer := time.NewTimer(timeout)
		defer timer.Stop()

		select {
		case <-timer.C:
			logger.Warn("process SIGTERM timed out, sending SIGKILL", zap.String("id", id.String()))
			if err := proc.Signal(syscall.SIGKILL); err != nil {
				logger.Error("SIGKILL failed", zap.String("id", id.String()), zap.Error(err))
			}
		case <-ctx.Done():
		}
	}()
}

// shouldRestartWorkload determines if a workload should be restarted based on its restart policy and exit status.
func shouldRestartWorkload(policy workload.RestartPolicy, status cr.ExitStatus) bool {
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
func logProcessExit(logger *zap.Logger, id uuid.UUID, status cr.ExitStatus) {
	if status.Err != nil {
		logger.Error("process exit error", zap.String("id", id.String()), zap.Int("code", status.Code), zap.Error(status.Err))
	} else {
		logger.Info("process exited cleanly", zap.String("id", id.String()), zap.Int("code", status.Code))
	}
}

// cleanupContainerResources tears down network veth links, port mappings, and container resources.
func cleanupContainerResources(
	ctx context.Context,
	logger *zap.Logger,
	entry *ContainerAndProcess,
	ipAndCIDRs map[uuid.UUID]string,
) {
	id := entry.Spec.ID
	if err := cn.DeleteLink(cn.VethHostName(id.String(), cn.VethA)); err != nil {
		logger.Error("delete veth A end", zap.Error(err))
	}

	if entry.Spec.HostPort > 0 {
		if cidr, ok := ipAndCIDRs[id]; ok {
			if err := cn.RemovePortMapping(ctx, entry.Spec.HostPort, cidr, entry.Spec.ContainerPort); err != nil {
				logger.Error("remove port mapping", zap.Error(err))
			}
		}
	}
	delete(ipAndCIDRs, id)

	if entry.Container != nil {
		logger.Debug("destroying container", zap.String("id", id.String()))
		if err := entry.Destroy(); err != nil {
			logger.Error("destroy container", zap.Error(err))
		}
	}
}

// handleExitEvent processes container process exit events in the main event loop,
// performing network and container cleanup and recording the stopped instance state.
func handleExitEvent(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	nodeID uuid.UUID,
	id uuid.UUID,
	status cr.ExitStatus,
	running map[uuid.UUID]*ContainerAndProcess,
	ipAndCIDRs map[uuid.UUID]string,
	peerService *peerdiscovery.MemberService,
	views []journalview.View,
) {
	entry, exists := running[id]
	if !exists || entry == nil {
		return
	}

	logProcessExit(logger, id, status)
	cleanupContainerResources(ctx, logger, entry, ipAndCIDRs)

	entry.ExitStatus = &status
	entry.restartCount++
	entry.restartAfter = clock.Now().Add(backOff(entry.restartCount))

	recordInstanceEvent(ctx, logger, j, views, entry.Spec, nodeID, workload.StateStopped, 0)
	peerService.SetWorkloadCount(countRunning(running))
}

// countRunning returns the number of active, non-exited workloads in the running map.
func countRunning(running map[uuid.UUID]*ContainerAndProcess) int {
	count := 0
	for _, entry := range running {
		if entry != nil && entry.ExitStatus == nil {
			count++
		}
	}
	return count
}

// buildProcess constructs a libcontainer Process from the spec and image config.
// A spec command overrides the image default wholesale; an empty spec
// command falls back to the image entrypoint plus cmd, which is the
// standard container contract for a commandless submit. Both empty is a
// caller-visible error so runc never sees an empty argv. Container
// output flows into the sampler's per-workload log rings; a nil
// sampler leaves writers unset and output discards as before.
func buildProcess(spec workload.Spec, result *cr.PullResult, sampler *node.Sampler) (*libcontainer.Process, error) {
	args := spec.Command
	if len(args) == 0 {
		if result == nil || result.Config == nil {
			return nil, fmt.Errorf("workload %s has no command and no image config to fall back to", spec.ID)
		}
		img := result.Config.Config
		fallback := make([]string, 0, len(img.Entrypoint)+len(img.Cmd))
		fallback = append(fallback, img.Entrypoint...)
		fallback = append(fallback, img.Cmd...)
		if len(fallback) == 0 {
			return nil, fmt.Errorf("workload %s has no command and image %q sets no entrypoint or cmd", spec.ID, spec.Image)
		}
		args = fallback
	}

	env := result.Config.Config.Env
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}

	cwd := result.Config.Config.WorkingDir
	if cwd == "" {
		cwd = "/"
	}

	proc := &libcontainer.Process{
		Args: args,
		Env:  env,
		Cwd:  cwd,
		// Init selects the container-creation path: the init process
		// creates the cgroup and namespaces, while Init:false joins an
		// existing container (the exec-probe path). Without this the
		// initial start tries to join a cgroup nothing created yet and
		// fails opening it. Do not remove.
		Init: true,
	}
	if sampler != nil {
		proc.Stdout = sampler.LogWriter(spec.ID, node.LogStdout)
		proc.Stderr = sampler.LogWriter(spec.ID, node.LogStderr)
	}

	return proc, nil
}

// recordInstanceEvent writes a workload instance state event to the journal
// and applies it to views, like every other recorded event. Instance
// events previously appended journal bytes without applying views, so
// live generic indexes missed them until the next rebuild.
func recordInstanceEvent(
	ctx context.Context,
	logger *zap.Logger,
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
		logger.Error("marshal instance", zap.Error(err))
		return
	}

	event := journal.NewEvent(nodeID, journalview.EventTypeWorkloadInstancePrefix+string(state), payload)
	if err := journalview.RecordEvent(ctx, j, views, event); err != nil {
		logger.Error("record instance event", zap.Error(err))
	}
}

// bundleDirPath returns the bundle directory path for a given spec ID.
func bundleDirPath(specID uuid.UUID) (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config dir: %w", err)
	}

	return filepath.Join(dir, "concord", "bundles", specID.String()), nil
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

// adoptContainers inspects the state directory on startup and re-attaches running containers.
func adoptContainers(ctx context.Context, logger *zap.Logger, nodeID uuid.UUID,
	running map[uuid.UUID]*ContainerAndProcess, workloadsView *journalview.Workloads,
	stateDir string, exitEvents chan<- ExitEvent,
) {
	specs, err := workloadsView.List(ctx)
	if err != nil {
		logger.Error("workloads view list", zap.Error(err))
		return
	}

	for _, spec := range specs {
		if spec.Removed || spec.AssignedNodeID != nodeID {
			continue
		}

		ctr, err := libcontainer.Load(stateDir, spec.ID.String())
		if err != nil {
			logger.Error("libcontainer load", zap.Error(err))
			continue
		}

		status, err := ctr.Status()
		if err != nil || status != libcontainer.Running {
			continue
		}

		state, err := ctr.State()
		if err != nil {
			logger.Error("container state", zap.Error(err))
			continue
		}

		proc := cr.AdoptProcess(state.InitProcessPid)
		running[spec.ID] = &ContainerAndProcess{
			Container:     ctr,
			ProcessHandle: proc,
			Spec:          spec,
		}
		go monitorProcessExit(ctx, spec.ID, proc, exitEvents)
	}
}
