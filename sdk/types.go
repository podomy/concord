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

// WorkloadStats reports live utilization for one running workload.
// These are sampled readings, not desired state: they never enter the
// journal and never appear on submit. Node carries the local node's
// pressure trio as context: stats exist only where the workload runs,
// so the local node is always the relevant one.
type WorkloadStats struct {
	CPUPercent uint8        `json:"cpu_percent"`
	MemPercent uint8        `json:"mem_percent"`
	MemUsageMB uint64       `json:"mem_usage_mb"`
	Node       NodePressure `json:"node"`
}

// NodePressure is CPU, memory, and disk utilization as 0-100 percents.
type NodePressure struct {
	CPUPercent  uint8 `json:"cpu_percent"`
	MemPercent  uint8 `json:"mem_percent"`
	DiskPercent uint8 `json:"disk_percent"`
}

// Node represents a cluster member node and its current health state.
type Node struct {
	ID                 uuid.UUID `json:"id"`
	Address            string    `json:"address"`
	State              string    `json:"state"`
	WireGuardPublicKey string    `json:"wireguard_public_key,omitempty"`
	CPUPercent         uint8     `json:"cpu_percent,omitempty"`
	MemPercent         uint8     `json:"mem_percent,omitempty"`
	DiskPercent        uint8     `json:"disk_percent,omitempty"`
}
