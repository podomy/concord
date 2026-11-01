// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package workload

import (
	"fmt"
	"slices"
	"strings"
)

// Validate reports whether the spec is submittable. It is the single
// server-side check: the IPC submit handler rejects invalid specs with
// 400, and the journalview Apply path skips invalid synced specs so one
// bad line never fails the apply loop or displaces a tombstone. The SDK
// Builder validates client-side too, but the server never trusts it:
// raw HTTP can send anything.
//
// Memory: negative limits are rejected. Zero or unset means unlimited:
// no cgroup limit is set (see cr.BundleBuilder) and the scheduler's
// fitsMemory always fits a request <= 0, so 0 never strands a workload.
//
// CPU shares are relative weights with no kernel absolute: 0 means the
// kernel default applies (no explicit shares set), any other value is
// passed through. Negative values cannot arrive typed; raw JSON
// negatives fail decoding before they get here.
func (s Spec) Validate() error {
	if strings.TrimSpace(s.Image) == "" {
		return fmt.Errorf("workload image reference is required") //nolint:perfsprint // plain error, no wrap target
	}

	err := validateRestart(s.Restart)
	if err != nil {
		return err
	}

	if s.Resources.MemoryMB < 0 {
		return fmt.Errorf("memory limit cannot be negative: %d", s.Resources.MemoryMB)
	}

	err = validateHealth(s.HealthAction, s.HealthProbe, s.HealthCommand)
	if err != nil {
		return err
	}

	if !s.QoS.Valid() {
		return fmt.Errorf("invalid QoS class: %q", string(s.QoS))
	}

	for _, file := range s.Files {
		err := file.Validate()
		if err != nil {
			return fmt.Errorf("invalid file: %w", err)
		}
	}

	if slices.Contains(s.DependsOn, s.ID) {
		return fmt.Errorf("workload cannot depend on itself: %s", s.ID)
	}

	return nil
}

// validateRestart accepts the three known policies plus empty, which
// means the default (always) for specs written before the field
// existed. Unknown policies fail closed: the reconciler also treats
// unknown as no-restart, so rejection and runtime agree on never
// placing a misspelled policy.
func validateRestart(r RestartPolicy) error {
	switch r {
	case "", RestartNever, RestartAlways, RestartOnFailure:
		return nil
	default:
		return fmt.Errorf("invalid restart policy %q; must be 'always', 'never', or 'on_failure'", string(r))
	}
}

// validateHealth accepts action 0 (restart) or 1 (signal) and probe
// "", http, tcp, or exec. Exec without a command is rejected here, at
// the edge, instead of restart-looping a workload that can never pass.
// Unknown probes are rejected; the runtime treats them as unhealthy,
// so both layers refuse to run a misconfigured check silently.
func validateHealth(action HealthAction, probe HealthProbe, command []string) error {
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
