// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
)

// fastTickInterval is the local health beat. It runs an order of magnitude
// faster than the 5s reconcile tick because dead processes and failed health
// checks need millisecond-to-second reactions, not convergent ones. The fast
// beat reads local state only and never schedules, places, or syncs.
const fastTickInterval = 500 * time.Millisecond

// runFastTick checks the health of running workloads on the fast beat.
// It appends journal events solely on transitions (see runHealthChecks),
// so the fast cadence never floods the log: raw observations stay in
// memory, only their edges converge.
func runFastTick(ctx context.Context, logger *zap.Logger, running map[uuid.UUID]*ContainerAndProcess, j journal.Journal, views []journalview.View, nodeID uuid.UUID) {
	runHealthChecks(ctx, logger, running, j, views, nodeID)
}
