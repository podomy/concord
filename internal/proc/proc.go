// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package proc runs workloads as host binaries.
//
// It owns host process lifecycle only: start from a materialized binary
// path, report the pid and exit, signal on request. No cgroup, no
// namespaces, no networking. Higher layers decide what to run and when
// to restart it; this package only carries out the mechanics.
package proc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"

	"github.com/google/uuid"
)

// ExitStatus is the outcome of one process run: the wait exit code plus
// the wait error when the run itself failed.
type ExitStatus struct {
	// Err is the wait error, nil on a clean wait even for non-zero codes.
	Err error
	// Code is the process exit code, or -1 when no code is available.
	Code int
}

// Handle is one running or finished host process.
type Handle interface {
	// PID reports the host pid started for the workload.
	PID() int
	// Signal delivers a signal to the running process.
	Signal(os.Signal) error
	// Exited reports the process outcome exactly once, then stays ready.
	Exited() <-chan ExitStatus
}

// StartConfig carries everything Launcher.Start needs for one run.
type StartConfig struct {
	// ID names the workload being started, for error context only.
	ID uuid.UUID
	// BinPath is the materialized executable path. It must exist and be
	// executable; Start fails otherwise without spawning anything.
	BinPath string
	// Args is the process argv. Empty means run the binary with no args.
	Args []string
	// Env holds extra KEY=value entries appended to the daemon environment.
	Env []string
	// Dir is the process working directory. Empty means inherit.
	Dir string
	// Stdout and Stderr receive process output. Nil discards that stream.
	Stdout io.Writer
	// Stderr receives process error output. Nil discards that stream.
	Stderr io.Writer
}

// Launcher starts host processes. Implementations must be safe for
// concurrent use: the reconciler starts and stops many workloads.
type Launcher interface {
	// Start runs one process from cfg and reports its handle. A failure
	// to spawn (missing binary, bad directory) returns an error and no
	// handle; a spawned process reports its outcome on Exited.
	Start(ctx context.Context, cfg StartConfig) (Handle, error)
}

// osExecHandle is the production Handle: one exec.Cmd plus a waiter
// goroutine feeding a buffered exit channel, so exactly one outcome is
// ever observed and late readers still get it.
type osExecHandle struct {
	cmd    *exec.Cmd
	pid    int
	exited chan ExitStatus
}

// PID reports the host pid started for the workload.
func (h *osExecHandle) PID() int {
	return h.pid
}

// Signal delivers a signal to the running process.
func (h *osExecHandle) Signal(sig os.Signal) error {
	if h.cmd.Process == nil {
		return fmt.Errorf("signal pid %d: no process", h.pid)
	}
	err := h.cmd.Process.Signal(sig)
	if err != nil {
		return fmt.Errorf("signal pid %d: %w", h.pid, err)
	}
	return nil
}

// Exited reports the process outcome exactly once, then stays ready.
func (h *osExecHandle) Exited() <-chan ExitStatus {
	return h.exited
}

// OsLauncher starts real host processes with os/exec.
type OsLauncher struct{}

// Start runs one process from cfg and reports its handle.
func (OsLauncher) Start(ctx context.Context, cfg StartConfig) (Handle, error) {
	if cfg.BinPath == "" {
		return nil, fmt.Errorf("start workload %s: empty binary path", cfg.ID)
	}
	cmd := exec.CommandContext(ctx, cfg.BinPath, cfg.Args...) //nolint:gosec // BinPath is the staged workload binary from a content-addressed digest, argv from the workload spec
	cmd.Dir = cfg.Dir
	cmd.Dir = cfg.Dir
	cmd.Env = append(os.Environ(), cfg.Env...)
	if cfg.Stdout != nil {
		cmd.Stdout = cfg.Stdout
	}
	if cfg.Stderr != nil {
		cmd.Stderr = cfg.Stderr
	}
	err := cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("start workload %s: %w", cfg.ID, err)
	}
	h := &osExecHandle{
		cmd:    cmd,
		pid:    cmd.Process.Pid,
		exited: make(chan ExitStatus, 1),
	}
	go func() {
		werr := cmd.Wait()
		code := 0
		if werr != nil {
			code = -1
			var exitErr *exec.ExitError
			if errors.As(werr, &exitErr) {
				code = exitErr.ExitCode()
			}
		}
		h.exited <- ExitStatus{Err: werr, Code: code}
	}()
	return h, nil
}
