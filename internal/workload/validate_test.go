// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package workload

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

// Valid specs pass: defaults, zero memory as unlimited, zero CPU as
// kernel default, empty probe as HTTP.
func TestSpecValidateAcceptsDefaults(t *testing.T) {
	t.Parallel()

	spec := Spec{
		ID:    uuid.New(),
		Image: "nginx:latest",
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("Validate = %v", err)
	}
}

// Each invalid field fails on its own, so submitters learn exactly
// what the server rejected.
func TestSpecValidateRejects(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	good := Spec{ID: id, Image: "nginx:latest"}
	cases := []struct {
		name   string
		mutate func(*Spec)
	}{
		{"blank image", func(s *Spec) { s.Image = "   " }},
		{"bad restart", func(s *Spec) { s.Restart = "sometimes" }},
		{"negative memory", func(s *Spec) { s.Resources.MemoryMB = -1 }},
		{"bad health action", func(s *Spec) { s.HealthAction = 7 }},
		{"bad health probe", func(s *Spec) { s.HealthProbe = "smoke-signals" }},
		{"exec without command", func(s *Spec) { s.HealthProbe = HealthProbeExec }},
		{"bad qos", func(s *Spec) { s.QoS = "ultra" }},
		{"host port without container port", func(s *Spec) { s.HostPort = 8080 }},
		{"container port without host port", func(s *Spec) { s.ContainerPort = 80 }},
		{"bad file", func(s *Spec) {
			s.Files = []FileRef{{Path: "relative", Digest: "sha256:" + strings.Repeat("a", 64)}}
		}},
		{"self dependency", func(s *Spec) { s.DependsOn = []uuid.UUID{id} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := good
			tc.mutate(&spec)
			if err := spec.Validate(); err == nil {
				t.Fatalf("Validate(%+v) = nil, want error", spec)
			}
		})
	}
}

// Zero memory means unlimited, never negative: it passes validation and
// the scheduler always fits it.
func TestSpecValidateMemoryZeroUnlimited(t *testing.T) {
	t.Parallel()

	spec := Spec{ID: uuid.New(), Image: "x", Resources: Resources{MemoryMB: 0}}
	if err := spec.Validate(); err != nil {
		t.Fatalf("zero memory rejected: %v", err)
	}
}

// Ports pair together or stay unset together: one without the other
// cannot start, so submit rejects it, matching sdk.Builder.
func TestSpecValidatePortsPaired(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	good := Spec{ID: id, Image: "x", HostPort: 8080, ContainerPort: 80}
	if err := good.Validate(); err != nil {
		t.Fatalf("paired ports rejected: %v", err)
	}
	unset := Spec{ID: id, Image: "x"}
	if err := unset.Validate(); err != nil {
		t.Fatalf("unset ports rejected: %v", err)
	}
	half := []Spec{
		{ID: id, Image: "x", HostPort: 8080},
		{ID: id, Image: "x", ContainerPort: 80},
	}
	for _, spec := range half {
		if err := spec.Validate(); err == nil {
			t.Errorf("Validate(%+v) = nil, want error", spec)
		}
	}
}
