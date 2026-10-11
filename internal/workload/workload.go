// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package workload

import "github.com/google/uuid"

type Spec struct {
	Image          string // OCI reference, e.g. "docker.io/nginx:latest".
	Restart        RestartPolicy
	Command        []string // entrypoint override.
	Env            map[string]string
	Resources      Resources
	ID             uuid.UUID
	AssignedNodeID uuid.UUID `json:"AssignedNodeID"` // which node is assigned to run this.
	// AssignmentEpoch counts reassignments of one workload ID. Higher wins
	// in the view so an orphan move always supersedes the stored copy.
	AssignmentEpoch uint64
	Removed         bool
	HostPort        uint16
	ContainerPort   uint16
	// StopTimeoutSeconds specifies the maximum time (in seconds) allowed for graceful
	// process shutdown via SIGTERM before falling back to SIGKILL. Defaults to 60 seconds if <= 0.
	StopTimeoutSeconds int
	HealthAction       HealthAction
	HealthPath         string // HTTP path for health check endpoint (defaults to "/health").
	// HealthProbe selects the check type: http, tcp, or exec. Empty
	// means http.
	HealthProbe HealthProbe
	// HealthCommand is the argv run inside the container for exec
	// probes. Required when HealthProbe is exec, ignored otherwise.
	HealthCommand []string
}

type HealthAction int

const (
	HealthActionRestart = iota
	HealthActionSignal
)

// HealthProbe selects how the fast beat checks a workload: an HTTP
// endpoint, a TCP connect, or a command run inside the container.
// Empty means HTTP, so specs written before probes existed behave
// exactly as they always did.
type HealthProbe string

const (
	HealthProbeHTTP HealthProbe = "http"
	HealthProbeTCP  HealthProbe = "tcp"
	HealthProbeExec HealthProbe = "exec"
)

type Resources struct {
	CPUShares uint64 // relative weight (default 1024).
	MemoryMB  int64  // max memory, 0 = unlimited.
}

type RestartPolicy string

const (
	RestartNever     RestartPolicy = "never"
	RestartAlways    RestartPolicy = "always"
	RestartOnFailure RestartPolicy = "on_failure"
)

type Instance struct {
	State  State
	ID     uuid.UUID
	SpecID uuid.UUID
	NodeID uuid.UUID
	PID    int
}

// Unhealthy is the payload of a workload.unhealthy event: the signal
// action's falling edge. It names the workload so the event converges
// fleet-wide with the subject attached; an empty payload could never say
// which workload went sick. Keys stay Go-case like every other journal
// payload (Spec, Instance), not the SDK's snake_case.
type Unhealthy struct {
	WorkloadID uuid.UUID
}

type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateStopped State = "stopped"
)
