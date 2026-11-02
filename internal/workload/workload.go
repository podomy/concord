// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package workload

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// Spec is the desired state of one workload.
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
	// DependsOn lists workload IDs that must be running on the same node
	// before this workload starts. Unknown, removed, or elsewhere-assigned
	// IDs are ignored, never waited on. Empty means no ordering.
	DependsOn []uuid.UUID
	// Files lists content-addressed files materialized into the container
	// filesystem before start. Blobs live in the registry under the
	// concord-files repo; the journal carries only these pointers.
	Files []FileRef
	// QoS ranks the workload for placement order and preemption:
	// best-effort gives way to everything, guaranteed gives way to
	// nothing. Empty means best-effort. This is compute allocation
	// (who gets the node), not message delivery: ROS-style DDS QoS
	// governs packets, this governs placement. See docs/qos.
	QoS QoS
}

// QoS is a workload's compute allocation class. Higher ranks place
// first and displace lower ones; displacement flows strictly downward,
// so eviction loops cannot form.
type QoS string

const (
	// QoSBestEffort runs when room allows and gives way to everything.
	QoSBestEffort QoS = "besteffort"
	// QoSBurstable preempts best-effort and yields to guaranteed.
	QoSBurstable QoS = "burstable"
	// QoSGuaranteed places first and is never preempted.
	QoSGuaranteed QoS = "guaranteed"
)

// Rank orders classes for placement and preemption. Empty and unknown
// values rank zero: unspecified work stays best-effort, and a misspelled
// class fails open instead of failing placed.
func (q QoS) Rank() int {
	switch q {
	case QoSGuaranteed:
		return 2
	case QoSBurstable:
		return 1
	case QoSBestEffort, "":
		return 0
	default:
		return 0
	}
}

// Valid reports whether the class is empty or one of the three known
// values. Submit rejects anything else, loudly.
func (q QoS) Valid() bool {
	switch q {
	case "", QoSBestEffort, QoSBurstable, QoSGuaranteed:
		return true
	default:
		return false
	}
}

// CheckNoCycle rejects a spec whose dependency chain loops back to
// itself through live specs. Unknown IDs have no dependencies, so only
// known specs participate; the existing graph is assumed acyclic because
// every submit passed this same check. Self-dependencies are rejected
// here at submit; the reconciler's depsReady also skips a self edge at
// runtime, so both layers agree and a hand-written journal line can
// stall nothing. Defense in depth, either layer alone suffices.
func CheckNoCycle(live []Spec, newSpec Spec) error {
	byID := make(map[uuid.UUID][]uuid.UUID, len(live)+1)
	for _, spec := range live {
		byID[spec.ID] = spec.DependsOn
	}
	byID[newSpec.ID] = newSpec.DependsOn

	visiting := make(map[uuid.UUID]bool)
	var visit func(id uuid.UUID) error
	visit = func(id uuid.UUID) error {
		if visiting[id] {
			return fmt.Errorf("dependency cycle at workload %s", id)
		}
		visiting[id] = true
		for _, dep := range byID[id] {
			if _, ok := byID[dep]; !ok {
				continue
			}
			if err := visit(dep); err != nil {
				return err
			}
		}
		delete(visiting, id)
		return nil
	}
	return visit(newSpec.ID)
}

// HealthAction selects what the supervisor does when a workload turns unhealthy.
type HealthAction int

const (
	// HealthActionRestart restarts an unhealthy workload.
	HealthActionRestart = iota
	// HealthActionSignal emits a workload.unhealthy event without restarting.
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

// Resources caps CPU shares and memory for one workload.
type Resources struct {
	CPUShares uint64 // relative weight (default 1024).
	MemoryMB  int64  // max memory, 0 = unlimited.
}

// FileRef points at one file's content in the registry: the container
// path it materializes to, the blob digest holding its bytes, and the
// file mode (0 means 0600).
type FileRef struct {
	Path   string
	Digest string
	Mode   uint32
}

// Validate rejects non-absolute, unclean, or escaping paths, malformed
// digests, and modes above 0777. Submit answers 400 on any of these;
// the materializer re-checks paths defensively because journal lines
// can be hand-written.
func (f FileRef) Validate() error {
	if !filepath.IsAbs(f.Path) {
		return fmt.Errorf("file path must be absolute: %q", f.Path)
	}
	if filepath.Clean(f.Path) != f.Path {
		return fmt.Errorf("file path must be clean: %q", f.Path)
	}
	if slices.Contains(strings.Split(f.Path, string(filepath.Separator)), "..") {
		return fmt.Errorf("file path must not escape: %q", f.Path)
	}
	hexPart, ok := strings.CutPrefix(f.Digest, "sha256:")
	if !ok || len(hexPart) != 64 {
		return fmt.Errorf("file digest must be sha256:<64 hex>: %q", f.Digest)
	}
	for _, c := range hexPart {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return fmt.Errorf("file digest must be sha256:<64 hex>: %q", f.Digest)
		}
	}
	if f.Mode > 0o777 {
		return fmt.Errorf("file mode %o exceeds 0777: %s", f.Mode, f.Path)
	}
	return nil
}

// RestartPolicy controls when a stopped workload is restarted.
type RestartPolicy string

const (
	// RestartNever never restarts a stopped workload.
	RestartNever RestartPolicy = "never"
	// RestartAlways always restarts a stopped workload.
	RestartAlways RestartPolicy = "always"
	// RestartOnFailure restarts a stopped workload only on failure.
	RestartOnFailure RestartPolicy = "on_failure"
)

// Instance is the runtime state of one workload on one node.
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

// State is the lifecycle phase of a workload instance.
type State string

const (
	// StatePending marks a workload waiting to start.
	StatePending State = "pending"
	// StateRunning marks a workload currently running.
	StateRunning State = "running"
	// StateStopped marks a workload that has stopped.
	StateStopped State = "stopped"
)
