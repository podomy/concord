// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/opencontainers/runc/libcontainer"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/workload"
)

// probeTimeout bounds one health check. The fast beat runs every 500ms
// across all local workloads sequentially, so an unbounded check would
// stall every workload's health behind one hung one.
const probeTimeout = 5 * time.Second

// CheckHealth runs the spec's configured probe and returns true when the
// workload looks healthy. Empty probe means HTTP, preserving every old
// spec's behavior. Unknown probes, TCP without a mapped host port, and
// exec without a command are unhealthy, loudly: silent skips hide
// misconfiguration. The container handle serves exec probes only and may
// be nil for the others.
func CheckHealth(ctx context.Context, logger *zap.Logger, spec workload.Spec, ctr *libcontainer.Container) bool {
	err := ctx.Err()
	if err != nil {
		logger.Error("context cancellation", zap.Error(err))
		return false
	}

	if spec.Removed {
		return false
	}

	switch spec.HealthProbe {
	case "", workload.HealthProbeHTTP:
		return checkHTTP(ctx, logger, spec)
	case workload.HealthProbeTCP:
		return checkTCP(ctx, logger, spec)
	case workload.HealthProbeExec:
		return checkExec(ctx, logger, spec, ctr)
	default:
		logger.Warn("unknown health probe, treating as unhealthy",
			zap.String("probe", string(spec.HealthProbe)), zap.String("id", spec.ID.String()))
		return false
	}
}

// checkHTTP queries the health endpoint on the mapped host port and
// returns true on a 2xx status. Workloads without mapped ports skip the
// check as healthy: specs written before probes existed must not
// suddenly restart-loop.
func checkHTTP(ctx context.Context, logger *zap.Logger, spec workload.Spec) bool {
	if spec.HostPort == 0 || spec.ContainerPort == 0 {
		return true
	}

	path := spec.HealthPath
	if path == "" {
		path = "/health"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	url := fmt.Sprintf("http://127.0.0.1:%d%s", spec.HostPort, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Error("failed to create health check request", zap.Uint16("port", spec.HostPort), zap.Error(err))
		return false
	}

	client := &http.Client{
		Timeout: probeTimeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Warn("health check request failed", zap.Uint16("port", spec.HostPort), zap.Error(err))
		return false
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck // best-effort body drain
		_ = resp.Body.Close()                 //nolint:errcheck // best-effort body close
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Warn("health check returned non-2xx status code", zap.Uint16("port", spec.HostPort), zap.Int("status_code", resp.StatusCode))
		return false
	}

	return true
}

// checkTCP dials the mapped host port and returns true on connect. Unlike
// HTTP there is no legacy skip: a TCP probe without a host port is a
// misconfiguration, and misconfiguration reports unhealthy, loudly.
func checkTCP(ctx context.Context, logger *zap.Logger, spec workload.Spec) bool {
	if spec.HostPort == 0 {
		logger.Warn("tcp probe without host port, treating as unhealthy", zap.String("id", spec.ID.String()))
		return false
	}

	dialer := &net.Dialer{Timeout: probeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", spec.HostPort))
	if err != nil {
		logger.Debug("tcp probe failed", zap.Uint16("port", spec.HostPort), zap.Error(err))
		return false
	}
	if err := conn.Close(); err != nil {
		logger.Debug("tcp probe close failed", zap.Uint16("port", spec.HostPort), zap.Error(err))
	}

	return true
}

// checkExec runs the spec command inside the container and returns true on
// exit code zero. Output discards: probe chatter stays out of log rings.
// The run is bounded by the probe timeout with SIGKILL escalation, so a
// hung check cannot stall the beat behind it.
func checkExec(ctx context.Context, logger *zap.Logger, spec workload.Spec, ctr *libcontainer.Container) bool {
	if len(spec.HealthCommand) == 0 {
		logger.Warn("exec probe without command, treating as unhealthy", zap.String("id", spec.ID.String()))
		return false
	}
	if ctr == nil {
		logger.Warn("exec probe without container, treating as unhealthy", zap.String("id", spec.ID.String()))
		return false
	}

	proc := &libcontainer.Process{
		Args:   spec.HealthCommand,
		Env:    execEnv(spec),
		Cwd:    "/",
		Init:   false,
		Stdin:  nil,
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	if err := ctr.Start(proc); err != nil {
		logger.Debug("exec probe start failed", zap.Strings("command", spec.HealthCommand), zap.Error(err))
		return false
	}

	type outcome struct {
		code int
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		state, err := proc.Wait()
		if err != nil {
			done <- outcome{code: -1, err: err}
			return
		}
		code := -1
		if state != nil {
			code = state.ExitCode()
		}
		done <- outcome{code: code}
	}()

	timeout := time.NewTimer(probeTimeout)
	defer timeout.Stop()
	select {
	case <-ctx.Done():
		_ = proc.Signal(syscall.SIGKILL) //nolint:errcheck // best-effort kill on cancel
		return false
	case out := <-done:
		return out.err == nil && out.code == 0
	case <-timeout.C:
		logger.Debug("exec probe timed out, killing", zap.Strings("command", spec.HealthCommand))
		if err := proc.Signal(syscall.SIGKILL); err != nil {
			logger.Debug("exec probe kill failed", zap.Error(err))
		}
		out := <-done
		return out.err == nil && out.code == 0
	}
}

// execEnv builds the probe environment from spec variables over a default
// PATH, so bare binary names resolve without per-workload boilerplate.
func execEnv(spec workload.Spec) []string {
	env := make([]string, 0, len(spec.Env)+1)
	for k, v := range spec.Env {
		env = append(env, k+"="+v)
	}
	if _, ok := spec.Env["PATH"]; !ok {
		env = append(env, "PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin")
	}
	return env
}
