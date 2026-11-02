// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package workload

import (
	"testing"

	"github.com/google/uuid"
)

// Tombstoned specs are excluded from the cycle graph, matching
// depsReady which never waits on them: a removed spec in the middle of
// a dependency chain breaks the cycle.
func TestCheckNoCycleIgnoresTombstones(t *testing.T) {
	t.Parallel()

	a, b := uuid.New(), uuid.New()
	live := []Spec{
		{ID: a, DependsOn: []uuid.UUID{b}},
		{ID: b, Removed: true, DependsOn: []uuid.UUID{a}},
	}

	if err := CheckNoCycle(live, Spec{ID: uuid.New(), DependsOn: []uuid.UUID{a}}); err != nil {
		t.Fatalf("tombstoned dep in cycle position rejected: %v", err)
	}

	// Without the tombstone the same shape is a cycle.
	liveLive := []Spec{
		{ID: a, DependsOn: []uuid.UUID{b}},
		{ID: b, DependsOn: []uuid.UUID{a}},
	}
	if err := CheckNoCycle(liveLive, Spec{ID: uuid.New(), DependsOn: []uuid.UUID{a}}); err == nil {
		t.Fatal("expected cycle through live specs")
	}
}
