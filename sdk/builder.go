// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

var (
	// ErrEmptyImage is returned when a workload specification has no container image.
	ErrEmptyImage = errors.New("workload image cannot be empty")

	// ErrInvalidPortMapping is returned when one port is configured without the other.
	ErrInvalidPortMapping = errors.New("both host_port and container_port must be specified together")

	// ErrNegativeMemory is returned when memory limit is negative.
	ErrNegativeMemory = errors.New("memory limit cannot be negative")

	// ErrInvalidRestartPolicy is returned when an unrecognized restart policy is provided.
	ErrInvalidRestartPolicy = errors.New("invalid restart policy; must be 'always', 'never', or 'on_failure'")
)

// Builder provides a fluent API for constructing and validating a Workload specification.
type Builder struct {
	workload Workload
}

// NewWorkload initializes a new Workload builder populated with safe default values.
func NewWorkload() *Builder {
	return &Builder{
		workload: Workload{
			Resources: Resources{
				// Standard Linux CFS weight (1 core equivalent).
				CPUShares: 1024,
				// 0 means unlimited memory.
				MemoryMB: 0,
			},
			Restart:            RestartAlways,
			StopTimeoutSeconds: 60,
			HealthAction:       HealthActionRestart,
			HealthPath:         "/health",
			Env:                make(map[string]string),
		},
	}
}

// Image sets the OCI container image reference (e.g. "docker.io/library/nginx:alpine").
func (b *Builder) Image(image string) *Builder {
	b.workload.Image = strings.TrimSpace(image)

	return b
}

// Command sets the entrypoint and argument list override for the container process.
func (b *Builder) Command(cmd ...string) *Builder {
	b.workload.Command = slices.Clone(cmd)

	return b
}

// Env sets an individual environment variable key-value pair.
func (b *Builder) Env(key, value string) *Builder {
	if b.workload.Env == nil {
		b.workload.Env = make(map[string]string)
	}
	b.workload.Env[key] = value

	return b
}

// Envs copies multiple environment variables into the workload.
func (b *Builder) Envs(env map[string]string) *Builder {
	if b.workload.Env == nil {
		b.workload.Env = make(map[string]string)
	}
	maps.Copy(b.workload.Env, env)

	return b
}

// Port configures port forwarding between host and container.
func (b *Builder) Port(hostPort, containerPort uint16) *Builder {
	b.workload.HostPort = hostPort
	b.workload.ContainerPort = containerPort

	return b
}

// Resources sets compute constraints (CPU shares and memory in MB).
func (b *Builder) Resources(res Resources) *Builder {
	b.workload.Resources = res

	return b
}

// MemoryMB sets the maximum memory limit in megabytes (0 = unlimited).
func (b *Builder) MemoryMB(mb int64) *Builder {
	b.workload.Resources.MemoryMB = mb

	return b
}

// CPUShares sets the relative CPU weight (default is 1024).
func (b *Builder) CPUShares(shares uint64) *Builder {
	b.workload.Resources.CPUShares = shares

	return b
}

// Restart sets the container restart policy.
func (b *Builder) Restart(policy RestartPolicy) *Builder {
	b.workload.Restart = policy

	return b
}

// HealthCheck sets the HTTP path and action for container health monitoring.
func (b *Builder) HealthCheck(path string, action HealthAction) *Builder {
	b.workload.HealthPath = path
	b.workload.HealthAction = action

	return b
}

// StopTimeout sets the graceful shutdown grace period before SIGKILL.
func (b *Builder) StopTimeout(d time.Duration) *Builder {
	secs := int(d.Seconds())
	if secs <= 0 {
		secs = 1
	}
	b.workload.StopTimeoutSeconds = secs

	return b
}

// StopTimeoutSeconds sets the graceful shutdown grace period in seconds.
func (b *Builder) StopTimeoutSeconds(seconds int) *Builder {
	b.workload.StopTimeoutSeconds = seconds

	return b
}

// Validate verifies that the workload configuration satisfies all invariants.
func (b *Builder) Validate() error {
	if b.workload.Image == "" {
		return ErrEmptyImage //nolint:wrapcheck // sentinel error
	}

	if (b.workload.HostPort > 0 && b.workload.ContainerPort == 0) ||
		(b.workload.ContainerPort > 0 && b.workload.HostPort == 0) {
		return ErrInvalidPortMapping //nolint:wrapcheck // sentinel error
	}

	if b.workload.Resources.MemoryMB < 0 {
		return ErrNegativeMemory //nolint:wrapcheck // sentinel error
	}

	switch b.workload.Restart {
	case "", RestartAlways, RestartNever, RestartOnFailure:
		// valid.
	default:
		return fmt.Errorf("%w: %q", ErrInvalidRestartPolicy, b.workload.Restart)
	}

	return nil
}

// Build validates and returns the final immutable Workload struct.
func (b *Builder) Build() (Workload, error) {
	err := b.Validate()
	if err != nil {
		return Workload{}, err
	}
	if b.workload.Restart == "" {
		b.workload.Restart = RestartAlways
	}

	return b.workload, nil
}

// MustBuild validates and returns the Workload struct, panicking if invalid.
func (b *Builder) MustBuild() Workload {
	w, err := b.Build()
	if err != nil {
		panic(fmt.Sprintf("sdk: invalid workload specification: %v", err))
	}

	return w
}
