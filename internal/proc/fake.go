// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package proc

import (
	"context"
	"fmt"
	"os"
	"sync"
)

// FakeLauncher is a scripted Launcher for unit tests. No processes spawn:
// Start records its config and returns scripted outcomes. It is safe for
// concurrent use.
type FakeLauncher struct {
	mu sync.Mutex
	// Cfgs records every Start call in order.
	Cfgs []StartConfig
	// StartErr, when non-nil, fails every Start with this error.
	StartErr error
	// PIDs assigns one pid per Start in call order; short lists reuse 1000.
	PIDs []int
	// Status is delivered on every handle's Exited channel.
	Status ExitStatus
	// started counts Start calls for pid assignment.
	started int
}

// Start records cfg and returns the scripted handle or error.
func (f *FakeLauncher) Start(_ context.Context, cfg StartConfig) (Handle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Cfgs = append(f.Cfgs, cfg)
	if f.StartErr != nil {
		return nil, f.StartErr
	}
	pid := 1000 + f.started
	if f.started < len(f.PIDs) {
		pid = f.PIDs[f.started]
	}
	f.started++
	exited := make(chan ExitStatus, 1)
	exited <- f.Status
	return &fakeHandle{pid: pid, exited: exited}, nil
}

// Calls reports how many times Start ran.
func (f *FakeLauncher) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Cfgs)
}

// fakeHandle is the FakeLauncher process handle: fixed pid, preloaded exit.
type fakeHandle struct {
	pid     int
	exited  chan ExitStatus
	signals []os.Signal
}

// PID reports the scripted pid.
func (h *fakeHandle) PID() int {
	return h.pid
}

// Signal records the signal and reports success.
func (h *fakeHandle) Signal(sig os.Signal) error {
	if sig == nil {
		return fmt.Errorf("signal pid %d: nil signal", h.pid)
	}
	h.signals = append(h.signals, sig)
	return nil
}

// Exited reports the scripted outcome.
func (h *fakeHandle) Exited() <-chan ExitStatus {
	return h.exited
}
