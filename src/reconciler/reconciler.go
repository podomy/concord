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

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/src/cr"
	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
)

// RunLoop watches for workload events and drives the container lifecycle.
// It blocks until ctx is cancelled.
func RunLoop(
	ctx context.Context,
	logger *zap.Logger,
	nodeID uuid.UUID,
	puller *cr.ImagePuller,
	runtime *cr.ContainerRuntime,
	j journal.Journal,
	views []journalview.View,
	eventsByType *journalview.EventsByType,
) {
	// TODO: rebuild desired state from journal replay
	// TODO: tick loop watching for new workload events
	// TODO: reconcile: pull image → build bundle → create → start
	// TODO: handle restart policies
	// TODO: emit state-change events
}
