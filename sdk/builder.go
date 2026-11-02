// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
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
// Empty keeps the image default: entrypoint plus cmd resolve at start time.
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

// HealthProbe selects the check type with an optional exec command.
// An empty probe with no command keeps the default HTTP check.
func (b *Builder) HealthProbe(probe HealthProbe, command ...string) *Builder {
	b.workload.HealthProbe = probe
	b.workload.HealthCommand = command

	return b
}

// DependsOn waits for the listed workloads to run on the same node
// before this one starts. Unknown, removed, or elsewhere-assigned IDs
// are ignored, never waited on.
func (b *Builder) DependsOn(ids ...uuid.UUID) *Builder {
	b.workload.DependsOn = append(b.workload.DependsOn, ids...)

	return b
}

// QoS sets the compute allocation class: best-effort gives way to
// everything, guaranteed gives way to nothing.
func (b *Builder) QoS(qos QoS) *Builder {
	b.workload.QoS = qos

	return b
}

// File stages a registry artifact into the container: the digest must
// already be pushed (see fileart.Client.Push); the spec carries only
// the pointer. Mode 0 means 0600.
func (b *Builder) File(containerPath, digest string, mode uint32) *Builder {
	b.workload.Files = append(b.workload.Files, FileRef{Path: containerPath, Digest: digest, Mode: mode})

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
	err := validateBuilderPlacement(b.workload)
	if err != nil {
		return err
	}

	err = validateBuilderHealth(b.workload.HealthAction, b.workload.HealthProbe, b.workload.HealthCommand)
	if err != nil {
		return err
	}

	for _, file := range b.workload.Files {
		err = validateBuilderFile(file)
		if err != nil {
			return err
		}
	}

	if slices.Contains(b.workload.DependsOn, b.workload.ID) {
		return fmt.Errorf("workload cannot depend on itself: %s", b.workload.ID)
	}

	return nil
}

// validateBuilderPlacement checks image, ports, memory, restart, and
// QoS so Build fails before server 400.
func validateBuilderPlacement(w Workload) error {
	if w.Image == "" {
		return ErrEmptyImage //nolint:wrapcheck // sentinel error
	}

	if (w.HostPort > 0 && w.ContainerPort == 0) ||
		(w.ContainerPort > 0 && w.HostPort == 0) {
		return ErrInvalidPortMapping //nolint:wrapcheck // sentinel error
	}

	if w.Resources.MemoryMB < 0 {
		return ErrNegativeMemory //nolint:wrapcheck // sentinel error
	}

	switch w.Restart {
	case "", RestartAlways, RestartNever, RestartOnFailure:
		// valid.
	default:
		return fmt.Errorf("%w: %q", ErrInvalidRestartPolicy, w.Restart)
	}

	switch w.QoS {
	case "", QoSBestEffort, QoSBurstable, QoSGuaranteed:
		// valid.
	default:
		return fmt.Errorf("invalid QoS class %q; must be 'besteffort', 'burstable', or 'guaranteed'", string(w.QoS))
	}

	return nil
}

// validateBuilderHealth mirrors the server health gate so bad probes
// fail at Build instead of server 400.
func validateBuilderHealth(action HealthAction, probe HealthProbe, command []string) error {
	switch action {
	case HealthActionRestart, HealthActionSignal:
	default:
		return fmt.Errorf("invalid health action %d; must be 0 (restart) or 1 (signal)", int(action))
	}
	switch probe {
	case "", HealthProbeHTTP, HealthProbeTCP:
	case HealthProbeExec:
		if len(command) == 0 {
			return fmt.Errorf("exec probe requires a health command") //nolint:perfsprint // plain error, no wrap target
		}
	default:
		return fmt.Errorf("invalid health probe %q; must be 'http', 'tcp', or 'exec'", string(probe))
	}
	return nil
}

// validateBuilderFile mirrors the server file gate: absolute clean
// paths, sha256 digests, modes within 0777.
func validateBuilderFile(file FileRef) error {
	if !filepath.IsAbs(file.Path) {
		return fmt.Errorf("file path must be absolute: %q", file.Path)
	}
	if filepath.Clean(file.Path) != file.Path {
		return fmt.Errorf("file path must be clean: %q", file.Path)
	}
	if slices.Contains(strings.Split(file.Path, string(filepath.Separator)), "..") {
		return fmt.Errorf("file path must not escape: %q", file.Path)
	}
	hexPart, ok := strings.CutPrefix(file.Digest, "sha256:")
	if !ok || len(hexPart) != 64 {
		return fmt.Errorf("file digest must be sha256:<64 hex>: %q", file.Digest)
	}
	for _, c := range hexPart {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return fmt.Errorf("file digest must be sha256:<64 hex>: %q", file.Digest)
		}
	}
	if file.Mode > 0o777 {
		return fmt.Errorf("file mode %o exceeds 0777: %s", file.Mode, file.Path)
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
