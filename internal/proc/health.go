// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package proc

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"strings"
	"time"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/workload"
)

// probeTimeout bounds one health check. The fast beat runs every 500ms
// across all local workloads, so an unbounded check would stall every
// workload's health behind one hung one.
const probeTimeout = 5 * time.Second

// CheckHealth runs the spec's configured probe and returns true when the
// workload looks healthy. Empty probe means HTTP, preserving every old
// spec's behavior. Unknown probes, TCP without a mapped host port, and
// exec without a command are unhealthy, loudly: silent skips hide
// misconfiguration. Probes run on the host against the workload's own
// ports and commands; there is no container to enter.
func CheckHealth(ctx context.Context, logger *logs.Logger, spec workload.Spec) bool {
	err := ctx.Err()
	if err != nil {
		logger.Error("context cancellation", logs.Error(err))
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
		return checkExec(ctx, logger, spec)
	default:
		logger.Warn("unknown health probe, treating as unhealthy",
			logs.String("probe", string(spec.HealthProbe)), logs.String("id", spec.ID.String()))
		return false
	}
}

// checkHTTP queries the health endpoint on the workload port and returns
// true on a 2xx status. Workloads without mapped ports skip the check as
// healthy: specs written before probes existed must not suddenly
// restart-loop.
func checkHTTP(ctx context.Context, logger *logs.Logger, spec workload.Spec) bool {
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
		logger.Error("failed to create health check request", logs.Uint16("port", spec.HostPort), logs.Error(err))
		return false
	}

	client := &http.Client{
		Timeout: probeTimeout,
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Warn("health check request failed", logs.Uint16("port", spec.HostPort), logs.Error(err))
		return false
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck // best-effort body drain
		_ = resp.Body.Close()                 //nolint:errcheck // best-effort body close
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Warn("health check returned non-2xx status code", logs.Uint16("port", spec.HostPort), logs.Int("status_code", resp.StatusCode))
		return false
	}

	return true
}

// checkTCP dials the workload port and returns true on connect. Unlike
// HTTP there is no legacy skip: a TCP probe without a host port is a
// misconfiguration, and misconfiguration reports unhealthy, loudly.
func checkTCP(ctx context.Context, logger *logs.Logger, spec workload.Spec) bool {
	if spec.HostPort == 0 {
		logger.Warn("tcp probe without host port, treating as unhealthy", logs.String("id", spec.ID.String()))
		return false
	}

	dialer := &net.Dialer{Timeout: probeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", fmt.Sprintf("127.0.0.1:%d", spec.HostPort))
	if err != nil {
		logger.Debug("tcp probe failed", logs.Uint16("port", spec.HostPort), logs.Error(err))
		return false
	}
	err = conn.Close()
	if err != nil {
		logger.Debug("tcp probe close failed", logs.Uint16("port", spec.HostPort), logs.Error(err))
	}

	return true
}

// outcome carries one exec probe exit across the wait boundary.
type outcome struct {
	code int
	err  error
}

// checkExec runs the spec command on the host and returns true on exit
// code zero. Output discards: probe chatter stays out of log rings. The
// run is bounded by the probe timeout with SIGKILL escalation, so a hung
// check cannot stall the beat behind it.
func checkExec(ctx context.Context, logger *logs.Logger, spec workload.Spec) bool {
	if len(spec.HealthCommand) == 0 {
		logger.Warn("exec probe without command, treating as unhealthy", logs.String("id", spec.ID.String()))
		return false
	}

	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, spec.HealthCommand[0], spec.HealthCommand[1:]...) //nolint:gosec // probe command comes from the workload spec by design
	cmd.Env = execEnv(spec)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	done := make(chan outcome, 1)
	go func() {
		werr := cmd.Run()
		code := 0
		if werr != nil {
			code = -1
		}
		done <- outcome{code: code, err: werr}
	}()

	select {
	case <-ctx.Done():
		return false
	case out := <-done:
		return out.err == nil && out.code == 0
	case <-probeCtx.Done():
		return killAndReap(logger, spec, done)
	}
}

// reapTimeout bounds the post-timeout wait for a timed-out probe. A
// process that ignores SIGKILL must not stall the 500ms beat behind it:
// expiry skips the beat and the reaped exit, if any, is dropped.
const reapTimeout = 5 * time.Second

// killAndReap waits for a timed-out probe's exit up to reapTimeout. The
// probe context already SIGKILLed the process on expiry; this only bounds
// the wait, reporting healthy solely on a zero exit before the bound.
func killAndReap(logger *logs.Logger, spec workload.Spec, done <-chan outcome) bool {
	logger.Debug("exec probe timed out, reaping", logs.Strings("command", spec.HealthCommand))
	select {
	case out := <-done:
		return out.err == nil && out.code == 0
	case <-time.After(reapTimeout):
		logger.Warn("exec probe unreaped after timeout, skipping beat",
			logs.Strings("command", spec.HealthCommand))
		return false
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
