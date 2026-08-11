// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk_test

import (
	"errors"
	"testing"
	"time"

	"github.com/podomy/concord/sdk"
)

func TestBuilder_Defaults(t *testing.T) {
	t.Parallel()

	w, err := sdk.NewWorkload().
		Image("docker.io/library/nginx:alpine").
		Build()
	if err != nil {
		t.Fatalf("expected valid build, got error: %v", err)
	}

	if w.Image != "docker.io/library/nginx:alpine" {
		t.Errorf("expected image 'docker.io/library/nginx:alpine', got %q", w.Image)
	}
	if w.Resources.CPUShares != 1024 {
		t.Errorf("expected default CPUShares 1024, got %d", w.Resources.CPUShares)
	}
	if w.Resources.MemoryMB != 0 {
		t.Errorf("expected default MemoryMB 0, got %d", w.Resources.MemoryMB)
	}
	if w.Restart != sdk.RestartAlways {
		t.Errorf("expected default Restart %q, got %q", sdk.RestartAlways, w.Restart)
	}
	if w.StopTimeoutSeconds != 60 {
		t.Errorf("expected default StopTimeoutSeconds 60, got %d", w.StopTimeoutSeconds)
	}
	if w.HealthAction != sdk.HealthActionRestart {
		t.Errorf("expected default HealthAction %d, got %d", sdk.HealthActionRestart, w.HealthAction)
	}
	if w.HealthPath != "/health" {
		t.Errorf("expected default HealthPath '/health', got %q", w.HealthPath)
	}
	if w.Env == nil {
		t.Error("expected non-nil Env map")
	}
}

func TestBuilder_FluentChaining(t *testing.T) {
	t.Parallel()

	cmdArgs := []string{"/bin/sh", "-c", "echo hello"}
	envMap := map[string]string{"ENV": "prod", "REGION": "us-east"}

	w, err := sdk.NewWorkload().
		Image("  alpine:latest  ").
		Command(cmdArgs...).
		Env("EXTRA_VAR", "value").
		Envs(envMap).
		Port(8080, 80).
		MemoryMB(512).
		CPUShares(2048).
		Restart(sdk.RestartOnFailure).
		HealthCheck("/livez", sdk.HealthActionSignal).
		StopTimeout(15 * time.Second).
		Build()
	if err != nil {
		t.Fatalf("expected valid build, got error: %v", err)
	}

	assertChainingIdentity(t, w, cmdArgs)
	assertChainingConfig(t, w)
}

func assertChainingIdentity(t *testing.T, w sdk.Workload, cmdArgs []string) {
	t.Helper()

	if w.Image != "alpine:latest" {
		t.Errorf("expected trimmed image 'alpine:latest', got %q", w.Image)
	}
	if len(w.Command) != 3 || w.Command[0] != "/bin/sh" {
		t.Errorf("expected command %v, got %v", cmdArgs, w.Command)
	}
	cmdArgs[0] = "mutated"
	if w.Command[0] == "mutated" {
		t.Error("expected Command slice to be defensively cloned, but it was mutated by caller")
	}
	if w.Env["EXTRA_VAR"] != "value" || w.Env["ENV"] != "prod" || w.Env["REGION"] != "us-east" {
		t.Errorf("unexpected env map: %v", w.Env)
	}
}

func assertChainingConfig(t *testing.T, w sdk.Workload) {
	t.Helper()

	if w.HostPort != 8080 || w.ContainerPort != 80 {
		t.Errorf("expected ports 8080:80, got %d:%d", w.HostPort, w.ContainerPort)
	}
	if w.Resources.MemoryMB != 512 || w.Resources.CPUShares != 2048 {
		t.Errorf("unexpected resources: %+v", w.Resources)
	}
	if w.Restart != sdk.RestartOnFailure {
		t.Errorf("expected restart %q, got %q", sdk.RestartOnFailure, w.Restart)
	}
	if w.HealthPath != "/livez" || w.HealthAction != sdk.HealthActionSignal {
		t.Errorf("unexpected health check settings: %s, %d", w.HealthPath, w.HealthAction)
	}
	if w.StopTimeoutSeconds != 15 {
		t.Errorf("expected stop timeout 15s, got %d", w.StopTimeoutSeconds)
	}
}

func TestBuilder_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		build       func() error
		expectedErr error
	}{
		{
			name: "empty image",
			build: func() error {
				_, err := sdk.NewWorkload().Image("   ").Build()
				return err //nolint:wrapcheck // testing raw sentinel error
			},
			expectedErr: sdk.ErrEmptyImage,
		},
		{
			name: "host port without container port",
			build: func() error {
				_, err := sdk.NewWorkload().
					Image("nginx").
					Port(8080, 0).
					Build()
				return err //nolint:wrapcheck // testing raw sentinel error
			},
			expectedErr: sdk.ErrInvalidPortMapping,
		},
		{
			name: "container port without host port",
			build: func() error {
				_, err := sdk.NewWorkload().
					Image("nginx").
					Port(0, 80).
					Build()
				return err //nolint:wrapcheck // testing raw sentinel error
			},
			expectedErr: sdk.ErrInvalidPortMapping,
		},
		{
			name: "negative memory",
			build: func() error {
				_, err := sdk.NewWorkload().
					Image("nginx").
					MemoryMB(-10).
					Build()
				return err //nolint:wrapcheck // testing raw sentinel error
			},
			expectedErr: sdk.ErrNegativeMemory,
		},
		{
			name: "invalid restart policy",
			build: func() error {
				_, err := sdk.NewWorkload().
					Image("nginx").
					Restart(sdk.RestartPolicy("sometimes")).
					Build()
				return err //nolint:wrapcheck // testing raw sentinel error
			},
			expectedErr: sdk.ErrInvalidRestartPolicy,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.build()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, tc.expectedErr) {
				t.Fatalf("expected error %v, got %v", tc.expectedErr, err)
			}
		})
	}
}

func TestBuilder_MustBuild(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("unexpected panic: %v", r)
			}
		}()

		w := sdk.NewWorkload().Image("redis:alpine").MustBuild()
		if w.Image != "redis:alpine" {
			t.Errorf("expected image 'redis:alpine', got %q", w.Image)
		}
	})

	t.Run("panics on invalid specification", func(t *testing.T) {
		t.Parallel()
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected MustBuild to panic on empty image, but it didn't")
			}
		}()

		_ = sdk.NewWorkload().Image("").MustBuild()
	})
}
