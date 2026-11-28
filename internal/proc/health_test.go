// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package proc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"
	"github.com/podomy/concord/internal/proc"
	"github.com/podomy/concord/internal/workload"
)

// A true exec probe is healthy.
func TestCheckHealthExecTrue(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{ID: uuid.New(), HealthProbe: workload.HealthProbeExec, HealthCommand: []string{"/bin/true"}}
	if !proc.CheckHealth(context.Background(), logs.NewTest(t), spec) {
		t.Fatal("true probe reported unhealthy")
	}
}

// A false exec probe is unhealthy.
func TestCheckHealthExecFalse(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{ID: uuid.New(), HealthProbe: workload.HealthProbeExec, HealthCommand: []string{"/bin/false"}}
	if proc.CheckHealth(context.Background(), logs.NewTest(t), spec) {
		t.Fatal("false probe reported healthy")
	}
}

// An exec probe without a command is unhealthy, loudly.
func TestCheckHealthExecWithoutCommand(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{ID: uuid.New(), HealthProbe: workload.HealthProbeExec}
	if proc.CheckHealth(context.Background(), logs.NewTest(t), spec) {
		t.Fatal("commandless probe reported healthy")
	}
}

// An unknown probe kind is unhealthy, loudly.
func TestCheckHealthUnknownProbe(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{ID: uuid.New(), HealthProbe: "bogus-probe"}
	if proc.CheckHealth(context.Background(), logs.NewTest(t), spec) {
		t.Fatal("unknown probe reported healthy")
	}
}

// A TCP probe without a host port is unhealthy, loudly.
func TestCheckHealthTCPWithoutPort(t *testing.T) {
	t.Parallel()

	spec := workload.Spec{ID: uuid.New(), HealthProbe: workload.HealthProbeTCP}
	if proc.CheckHealth(context.Background(), logs.NewTest(t), spec) {
		t.Fatal("portless TCP probe reported healthy")
	}
}

// A TCP probe against a live listener is healthy.
func TestCheckHealthTCPListener(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	lc := &net.ListenConfig{}
	l, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() {
		_ = l.Close() //nolint:errcheck // test listener teardown
	}()
	addr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("addr = %T, want *net.TCPAddr", l.Addr())
	}
	if addr.Port <= 0 || addr.Port > 65535 {
		t.Fatalf("port = %d, want 1-65535", addr.Port)
	}
	spec := workload.Spec{ID: uuid.New(), HealthProbe: workload.HealthProbeTCP, HostPort: uint16(addr.Port)} //nolint:gosec // kernel-assigned ephemeral port, range-checked above
	if !proc.CheckHealth(ctx, logs.NewTest(t), spec) {
		t.Fatal("live TCP probe reported unhealthy")
	}
}
