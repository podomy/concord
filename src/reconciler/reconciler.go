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
	"time"

	"github.com/google/uuid"
	"github.com/opencontainers/runc/libcontainer"
	"go.uber.org/zap"

	"github.com/podomy/concord/src/cr"
	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
	"github.com/podomy/concord/src/workload"
)

const eventTypeWorkloadSpec = "workload.spec"

// RunLoop watches for workload events and drives the container lifecycle.
// It blocks until ctx is cancelled. Always launch as a goroutine.
func RunLoop(
	ctx context.Context,
	logger *zap.Logger,
	nodeID uuid.UUID,
	puller *cr.ImagePuller,
	runtime *cr.ContainerRuntime,
	j journal.Journal,
	eventsByType *journalview.EventsByType,
) {
	running := map[uuid.UUID]*libcontainer.Container{}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			reconcileTick(ctx, logger, nodeID, puller, runtime, j, eventsByType, running)
		}
	}
}

// reconcileTick reconciles all workload specs for this node against running containers.
func reconcileTick(
	ctx context.Context,
	logger *zap.Logger,
	nodeID uuid.UUID,
	puller *cr.ImagePuller,
	runtime *cr.ContainerRuntime,
	j journal.Journal,
	eventsByType *journalview.EventsByType,
	running map[uuid.UUID]*libcontainer.Container,
) {
	events, err := eventsByType.List(ctx, eventTypeWorkloadSpec)
	if err != nil {
		logger.Error("list workload events", zap.Error(err))
		return
	}

	for _, event := range events {
		if event.NodeID != nodeID {
			continue
		}

		var spec workload.Spec
		if err := json.Unmarshal(event.Payload, &spec); err != nil {
			logger.Error("unmarshal workload spec", zap.Error(err))
			continue
		}

		if spec.Removed {
			destroyContainer(ctx, logger, j, nodeID, spec, running)
			continue
		}

		if _, exists := running[spec.ID]; exists {
			continue
		}

		startContainer(ctx, logger, puller, runtime, j, nodeID, spec, running)
	}
}

// startContainer pulls the image, builds the bundle, creates the container,
// and starts the init process.
func startContainer(
	ctx context.Context,
	logger *zap.Logger,
	puller *cr.ImagePuller,
	runtime *cr.ContainerRuntime,
	j journal.Journal,
	nodeID uuid.UUID,
	spec workload.Spec,
	running map[uuid.UUID]*libcontainer.Container,
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
	if err = runtime.Start(ctr, proc); err != nil {
		logger.Error("start container", zap.Error(err))
		return
	}

	running[spec.ID] = ctr

	pid, err := proc.Pid()
	if err != nil {
		logger.Error("get pid", zap.Error(err))
	}

	recordInstanceEvent(ctx, logger, j, spec, nodeID, workload.StateRunning, pid)
}

// destroyContainer stops and removes a container, then removes it from the running set.
func destroyContainer(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	nodeID uuid.UUID,
	spec workload.Spec,
	running map[uuid.UUID]*libcontainer.Container,
) {
	ctr, exists := running[spec.ID]
	if !exists {
		return
	}

	delete(running, spec.ID)

	if err := ctr.Destroy(); err != nil {
		logger.Error("destroy container", zap.Error(err))
	}

	recordInstanceEvent(ctx, logger, j, spec, nodeID, workload.StateStopped, 0)
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
