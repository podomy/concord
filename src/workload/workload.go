// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package workload

import "github.com/google/uuid"

type Spec struct {
	Image         string // OCI reference, e.g. "docker.io/nginx:latest".
	Restart       RestartPolicy
	Command       []string // entrypoint override.
	Env           map[string]string
	Resources     Resources
	ID            uuid.UUID
	SegmentID     uuid.UUID // which node must run this.
	Removed       bool
	HostPort      uint16
	ContainerPort uint16
	// StopTimeoutSeconds specifies the maximum time (in seconds) allowed for graceful
	// process shutdown via SIGTERM before falling back to SIGKILL. Defaults to 60 seconds if <= 0.
	StopTimeoutSeconds int
	HealthAction       HealthAction
	HealthPath         string // HTTP path for health check endpoint (defaults to "/health").
}

type HealthAction int

const (
	HealthActionRestart = iota
	HealthActionSignal
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

type State string

const (
	StatePending State = "pending"
	StateRunning State = "running"
	StateStopped State = "stopped"
)
