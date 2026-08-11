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
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/opencontainers/runc/libcontainer"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/cn"
	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/workload"
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
}

// ExitEvent carries a process exit status paired with its workload ID to the main loop channel.
type ExitEvent struct {
	// ExitStatus contains the exit code and error outcome of the process execution.
	ExitStatus cr.ExitStatus
	// WorkloadID identifies the specific workload spec associated with the exited process.
	WorkloadID uuid.UUID
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
) {
	running := map[uuid.UUID]*ContainerAndProcess{}
	ipAndCIDRs := map[uuid.UUID]string{}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

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
			handleExitEvent(ctx, logger, j, nodeID, ev.WorkloadID, ev.ExitStatus, running, ipAndCIDRs, peerService)

		case <-ticker.C:
			if isLeader(nodeID, peerService) {
				scheduleWorkloads(ctx, logger, j, workloads, peerService, nodeID, views)
			}

			reconcileTick(ctx, logger, nodeID, puller, runtime, j, workloads, running, ipAndCIDRs, exitEvents, peerService)
			runHealthChecks(ctx, logger, running, j, views, nodeID)
		}
	}
}

func runHealthChecks(ctx context.Context, logger *zap.Logger, running map[uuid.UUID]*ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID) {
	for _, entry := range running {
		if entry == nil || entry.Container == nil || entry.Stopping || entry.ExitStatus != nil {
			continue
		}
		healthy := cr.CheckHealth(ctx, logger, entry.Spec)
		// Check liveness.
		if healthy {
			continue
		}

		// Check readiness and resources.
		switch entry.Spec.HealthAction {
		case workload.HealthActionRestart:
			logger.Warn("restarting unhealthy workload")
			destroyContainer(ctx, logger, entry.Spec, running)
		case workload.HealthActionSignal:
			logger.Warn("signaling unhealthy workload")
			// Emmitting a journal event.
			event := journal.NewEvent(nodeID, "workload.unhealthy", json.RawMessage{})
			err := journalview.RecordEvent(ctx, j, views, event)
			if err != nil {
				logger.Error("record event failed", zap.Error(err))
				continue
			}
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
) {
	workloadSpecs, err := workloadsView.List(ctx)
	if err != nil {
		logger.Error("list workloads", zap.Error(err))
		return
	}

	for _, spec := range workloadSpecs {
		reconcileWorkloadSpec(ctx, logger, nodeID, puller, runtime, j, spec, running, ipAndCIDRs, exitEvents, peerService)
	}
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
) {
	if spec.SegmentID != nodeID {
		return
	}

	entry, exists := running[spec.ID]

	// 1. Spec was removed -> stop process if running, or purge entry if exited.
	if spec.Removed {
		if exists {
			reconcileRemovedSpec(ctx, logger, spec, running, entry, peerService)
		}
		return
	}

	// 2. Workload is actively running -> nothing to do.
	if exists && (entry == nil || entry.ExitStatus == nil) {
		return
	}

	// 3. Workload previously exited -> evaluate restart policy.
	if exists && entry != nil && entry.ExitStatus != nil {
		if !shouldRestartWorkload(spec.Restart, *entry.ExitStatus) {
			return // Restart policy dictates no restart; retain exited status entry.
		}
		if entry.restartAfter.After(time.Now()) {
			// We are still cooling down, can't restart now.
			return
		}

		delete(running, spec.ID) // Policy allows restart; clear old entry for new container start.
		peerService.SetWorkloadCount(countRunning(running))
	}

	// 4. Start new container instance.
	startContainer(ctx, logger, puller, runtime, j, nodeID, spec, running, ipAndCIDRs, exitEvents, peerService)
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
	if sigErr := processHandle.Signal(syscall.SIGKILL); sigErr != nil {
		logger.Error("kill process on network setup failure", zap.Error(sigErr))
	}
	if destroyErr := ctr.Destroy(); destroyErr != nil {
		logger.Error("destroy container on network setup failure", zap.Error(destroyErr))
	}
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
) {
	bundleDir, err := bundleDirPath(spec.ID)
	if err != nil {
		logger.Error("bundle dir", zap.Error(err))
		return
	}

	pullResult, err := puller.Pull(ctx, spec.Image, bundleDir)
	if err != nil {
		logger.Error("pull image", zap.String("image", spec.Image), zap.Error(err))
		return
	}

	cfg, err := cr.BundleBuilder(ctx, *pullResult, spec)
	if err != nil {
		logger.Error("build bundle", zap.Error(err))
		return
	}

	ctr, err := runtime.Create(spec.ID.String(), cfg)
	if err != nil {
		logger.Error("create container", zap.Error(err))
		return
	}

	proc := buildProcess(spec, pullResult)
	processHandle, err := runtime.Start(ctr, proc)
	if err != nil {
		logger.Error("start container", zap.Error(err))
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

	recordInstanceEvent(ctx, logger, j, spec, nodeID, workload.StateRunning, processHandle.NamespacePID())
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
) {
	entry, exists := running[id]
	if !exists || entry == nil {
		return
	}

	logProcessExit(logger, id, status)
	cleanupContainerResources(ctx, logger, entry, ipAndCIDRs)

	entry.ExitStatus = &status
	entry.restartCount++
	entry.restartAfter = time.Now().Add(backOff(entry.restartCount))

	recordInstanceEvent(ctx, logger, j, entry.Spec, nodeID, workload.StateStopped, 0)
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
func buildProcess(spec workload.Spec, result *cr.PullResult) *libcontainer.Process {
	env := result.Config.Config.Env
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}

	cwd := result.Config.Config.WorkingDir
	if cwd == "" {
		cwd = "/"
	}

	proc := &libcontainer.Process{
		Args: spec.Command,
		Env:  env,
		Cwd:  cwd,
	}

	return proc
}

// recordInstanceEvent writes a workload instance state event to the journal.
func recordInstanceEvent(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
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

	event := journal.NewEvent(nodeID, "workload.instance."+string(state), payload)
	if err := j.Append(ctx, event); err != nil {
		logger.Error("append instance event", zap.Error(err))
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
		if spec.Removed || spec.SegmentID != nodeID {
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
