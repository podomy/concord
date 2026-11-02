// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/clock"
	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/workload"
)

// A drifted workload inside its cooldown waits instead of restarting:
// drift drops the restart policy but rides the same backoff as normal
// restarts, so a crash loop that also drifted cannot bypass it.
func TestDriftRestartHonorsBackoff(t *testing.T) {
	old := workload.Spec{ID: uuid.New(), Image: "app:v1"}
	updated := old
	updated.Image = "app:v2"

	entry := &ContainerAndProcess{
		Spec:         old,
		ExitStatus:   &cr.ExitStatus{Code: 1},
		restartCount: 1,
		restartAfter: clock.Now().Add(time.Hour),
	}
	running := map[uuid.UUID]*ContainerAndProcess{old.ID: entry}

	if handleExitedEntry(entry, updated, running, nil) {
		t.Fatal("drifted entry restarted inside its cooldown")
	}
	if _, ok := running[old.ID]; !ok {
		t.Fatal("cooling-down drifted entry dropped from running map")
	}
}

// Past the cooldown the same drifted workload restarts immediately:
// the operator ordered a new spec, so policy stays dropped.
func TestDriftRestartAfterCooldown(t *testing.T) {
	old := workload.Spec{ID: uuid.New(), Image: "app:v1"}
	updated := old
	updated.Image = "app:v2"

	entry := &ContainerAndProcess{
		Spec:         old,
		ExitStatus:   &cr.ExitStatus{Code: 1},
		restartAfter: clock.Now().Add(-time.Second),
	}
	running := map[uuid.UUID]*ContainerAndProcess{old.ID: entry}

	if !handleExitedEntry(entry, updated, running, nil) {
		t.Fatal("drifted entry did not restart past its cooldown")
	}
	if _, ok := running[old.ID]; ok {
		t.Fatal("restarted drifted entry left in running map")
	}
}

// Identical specs never drift, so a no-op update neither bumps nor
// restarts through the drift path.
func TestNoOpUpdateSkipsDriftRestart(t *testing.T) {
	spec := workload.Spec{ID: uuid.New(), Image: "app:v1"}
	entry := &ContainerAndProcess{Spec: spec, ExitStatus: &cr.ExitStatus{Code: 0}}
	running := map[uuid.UUID]*ContainerAndProcess{spec.ID: entry}

	if handleExitedEntry(entry, spec, running, nil) {
		t.Fatal("identical spec restarted through drift path")
	}
	if _, ok := running[spec.ID]; !ok {
		t.Fatal("identical-spec entry dropped from running map")
	}
}
