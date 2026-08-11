// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"github.com/google/uuid"
)

// RestartPolicy defines the container restart behavior on process exit.
type RestartPolicy string

const (
	// RestartNever indicates the container should never restart once terminated.
	RestartNever RestartPolicy = "never"

	// RestartAlways indicates the container should always be restarted upon termination.
	RestartAlways RestartPolicy = "always"

	// RestartOnFailure indicates the container should be restarted only on non-zero exit codes.
	RestartOnFailure RestartPolicy = "on_failure"
)

// HealthAction defines what action to take when a container fails health checks.
type HealthAction int

const (
	// HealthActionRestart restarts the container process upon failed health checks.
	HealthActionRestart HealthAction = 0

	// HealthActionSignal sends a notification signal when health check fails.
	HealthActionSignal HealthAction = 1
)

// Resources specifies compute limits for a container workload.
type Resources struct {
	CPUShares uint64 `json:"cpu_shares"` // Relative CPU weight (default 1024).
	MemoryMB  int64  `json:"memory_mb"`  // Maximum memory in MB (0 = unlimited).
}

// Workload defines the complete specification of a container workload in Concord.
type Workload struct {
	ID                 uuid.UUID         `json:"id,omitempty"`
	Image              string            `json:"image"`
	Command            []string          `json:"command,omitempty"`
	Env                map[string]string `json:"env,omitempty"`
	Resources          Resources         `json:"resources"`
	Restart            RestartPolicy     `json:"restart"`
	HostPort           uint16            `json:"host_port,omitempty"`
	ContainerPort      uint16            `json:"container_port,omitempty"`
	StopTimeoutSeconds int               `json:"stop_timeout_seconds,omitempty"`
	HealthAction       HealthAction      `json:"health_action,omitempty"`
	HealthPath         string            `json:"health_path,omitempty"`
}

// Node represents a cluster member node and its current health state.
type Node struct {
	ID                 uuid.UUID `json:"id"`
	Address            string    `json:"address"`
	State              string    `json:"state"`
	WireGuardPublicKey string    `json:"wireguard_public_key,omitempty"`
}
