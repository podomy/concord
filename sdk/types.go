// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"time"

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

// HealthProbe selects how the fast beat checks a workload. Empty means
// HTTP, preserving every old spec's behavior.
type HealthProbe string

const (
	// HealthProbeHTTP checks an HTTP endpoint on the mapped host port.
	HealthProbeHTTP HealthProbe = "http"
	// HealthProbeTCP dials the mapped host port and checks the connect.
	HealthProbeTCP HealthProbe = "tcp"
	// HealthProbeExec runs a command inside the container and checks for
	// exit code zero.
	HealthProbeExec HealthProbe = "exec"
)

// QoS is a workload's compute allocation class: who places first and
// who gives way. Empty means best-effort. This ranks compute, not
// packets: ROS-style DDS QoS governs message delivery, this governs
// placement. See docs/qos.md.
type QoS string

const (
	// QoSBestEffort runs when room allows and gives way to everything.
	QoSBestEffort QoS = "besteffort"
	// QoSBurstable preempts best-effort and yields to guaranteed.
	QoSBurstable QoS = "burstable"
	// QoSGuaranteed places first and is never preempted.
	QoSGuaranteed QoS = "guaranteed"
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
	HealthProbe        HealthProbe       `json:"health_probe,omitempty"`
	HealthCommand      []string          `json:"health_command,omitempty"`
	DependsOn          []uuid.UUID       `json:"depends_on,omitempty"`
	QoS                QoS               `json:"qos,omitempty"`
}

// WorkloadStats reports live utilization for one running workload.
// These are sampled readings, not desired state: they never enter the
// journal and never appear on submit. MemLimitMB is 0 when unlimited.
// Node carries the local node's identity and pressure trio as context:
// stats exist only where the workload runs, so the local node is always
// the relevant one.
type WorkloadStats struct {
	CPUPercent    uint8      `json:"cpu_percent"`
	MemPercent    uint8      `json:"mem_percent"`
	MemUsageMB    uint64     `json:"mem_usage_mb"`
	MemLimitMB    uint64     `json:"mem_limit_mb"`
	AvgCPUPercent uint8      `json:"avg_cpu_percent"`
	AvgMemPercent uint8      `json:"avg_mem_percent"`
	Node          NodeStatus `json:"node"`
}

// NodeStatus is a node's identity plus CPU, memory, and disk utilization
// as 0-100 percents.
type NodeStatus struct {
	ID          uuid.UUID `json:"id"`
	CPUPercent  uint8     `json:"cpu_percent"`
	MemPercent  uint8     `json:"mem_percent"`
	DiskPercent uint8     `json:"disk_percent"`
}

// TrailPoint is one recorded position with its timestamp.
type TrailPoint struct {
	At  time.Time `json:"at"`
	Lat float64   `json:"lat"`
	Lon float64   `json:"lon"`
}

// LogLine is one captured container output line with its timestamp and
// stream ("stdout" or "stderr").
type LogLine struct {
	At     time.Time `json:"at"`
	Stream string    `json:"stream"`
	Line   string    `json:"line"`
}

// Position is one position fix in decimal degrees. It is the wire format
// for the self-position setter, shared by client and server so the
// endpoint body has exactly one definition.
type Position struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
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
	Lat                float64   `json:"lat,omitempty"`
	Lon                float64   `json:"lon,omitempty"`
	Anchor             bool      `json:"anchor,omitempty"`
}
