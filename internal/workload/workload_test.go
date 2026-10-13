// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package workload

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Ranks order classes; empty and unknown stay at zero with the
// best-effort they resemble.
func TestQoSRank(t *testing.T) {
	t.Parallel()

	if QoSGuaranteed.Rank() != 2 || QoSBurstable.Rank() != 1 {
		t.Fatal("ranks out of order")
	}
	if QoSBestEffort.Rank() != 0 || QoS("").Rank() != 0 || QoS("ultra").Rank() != 0 {
		t.Fatal("expected zero rank for best-effort, empty, and unknown")
	}
}

// Only the three classes plus empty pass; anything else is rejected at
// submit, never scheduled by surprise.
func TestQoSValid(t *testing.T) {
	t.Parallel()

	for _, q := range []QoS{"", QoSBestEffort, QoSBurstable, QoSGuaranteed} {
		if !q.Valid() {
			t.Errorf("Valid(%q) = false", q)
		}
	}
	if QoS("ultra").Valid() {
		t.Error("Valid(ultra) = true")
	}
}

// Self-loops and two-cycles fail; chains, diamonds, and unknown IDs pass.
func TestCheckNoCycle(t *testing.T) {
	t.Parallel()

	a, b, c := uuid.New(), uuid.New(), uuid.New()
	unknown := uuid.New()
	live := []Spec{
		{ID: a},
		{ID: b, DependsOn: []uuid.UUID{a}},
		{ID: c, DependsOn: []uuid.UUID{a, b}},
	}

	if err := CheckNoCycle(live, Spec{ID: uuid.New(), DependsOn: []uuid.UUID{c}}); err != nil {
		t.Errorf("chain rejected: %v", err)
	}
	if err := CheckNoCycle(live, Spec{ID: uuid.New(), DependsOn: []uuid.UUID{unknown}}); err != nil {
		t.Errorf("unknown dep rejected: %v", err)
	}
	if err := CheckNoCycle(live, Spec{ID: a, DependsOn: []uuid.UUID{c}}); err == nil {
		t.Error("expected cycle through existing spec")
	}
	self := uuid.New()
	if err := CheckNoCycle(nil, Spec{ID: self, DependsOn: []uuid.UUID{self}}); err == nil {
		t.Error("expected self-loop rejection")
	}
	d := uuid.New()
	if err := CheckNoCycle([]Spec{{ID: d, DependsOn: []uuid.UUID{uuid.New()}}}, Spec{ID: uuid.New()}); err != nil {
		t.Errorf("unrelated specs rejected: %v", err)
	}
}

// Absolute clean paths with valid digests pass; everything else fails
// loudly at submit instead of landing wrong bytes in a container.
func TestFileRefValidate(t *testing.T) {
	t.Parallel()

	good := FileRef{Path: "/etc/app/config.yaml", Digest: "sha256:" + strings.Repeat("a", 64), Mode: 0o644}
	if err := good.Validate(); err != nil {
		t.Errorf("Validate = %v", err)
	}

	bad := []FileRef{
		{Path: "relative.yaml", Digest: "sha256:" + strings.Repeat("a", 64)},
		{Path: "/etc/../escape", Digest: "sha256:" + strings.Repeat("a", 64)},
		{Path: "/etc/./dots", Digest: "sha256:" + strings.Repeat("a", 64)},
		{Path: "/ok", Digest: "md5:abc"},
		{Path: "/ok", Digest: "sha256:short"},
		{Path: "/ok", Digest: "sha256:" + strings.Repeat("z", 64)},
		{Path: "/ok", Digest: "sha256:" + strings.Repeat("a", 64), Mode: 0o7777},
	}
	assertInvalidFileRefs(t, bad)
}

// assertInvalidFileRefs fails the test on the first ref that validates.
func assertInvalidFileRefs(t *testing.T, refs []FileRef) {
	t.Helper()

	for _, ref := range refs {
		if err := ref.Validate(); err == nil {
			t.Errorf("Validate(%+v) = nil, want error", ref)
		}
	}
}
