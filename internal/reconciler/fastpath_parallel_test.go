// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/opencontainers/runc/libcontainer"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/workload"
)

// Many slow probes still beat well under the 500ms cadence: bounded
// concurrency (cap 8) keeps the fast beat parallel, with per-probe 5s
// timeout preserved by the context wrapper in runHealthChecks.
func TestRunHealthChecksManyWorkloadsCompletesFast(t *testing.T) {
	t.Parallel()

	old := checkHealth
	checkHealth = func(_ context.Context, _ *logs.Logger, _ workload.Spec, _ *libcontainer.Container) bool {
		time.Sleep(100 * time.Millisecond)
		return true
	}
	t.Cleanup(func() { checkHealth = old })

	running := map[uuid.UUID]*ContainerAndProcess{}
	var ctr libcontainer.Container
	for range 20 {
		id := uuid.New()
		running[id] = &ContainerAndProcess{
			Container: &ctr,
			Spec:      workload.Spec{ID: id},
		}
	}

	start := time.Now()
	runHealthChecks(context.Background(), logs.NewTest(t), running, &mockJournal{}, nil, uuid.New())
	elapsed := time.Since(start)

	// Sequential 20x100ms would take 2s; parallel cap 8 takes about 3
	// waves (300ms). Assert well under 1s to prove parallelism without
	// flaking on loaded CI.
	if elapsed >= 1*time.Second {
		t.Fatalf("beat took %v, want < 1s for 20x100ms probes with cap 8", elapsed)
	}
}
