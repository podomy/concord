// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"fmt"
	"os"

	"github.com/opencontainers/runc/libcontainer"
)

// Exit represents the outcome of process execution.
type Exit struct {
	Err  error
	Code int
}

// ProcessHandle defines the interface for interacting with a running container process.
type ProcessHandle interface {
	NamespacePID() int
	Signal(os.Signal) error
	Exited() <-chan Exit
}

// Process implements ProcessHandle for libcontainer process management.
type Process struct {
	process      *libcontainer.Process
	exited       chan Exit
	namespacePID int
}

// NewProcess creates a Process handle and starts monitoring process exit asynchronously.
func NewProcess(process *libcontainer.Process, namespacePID int) *Process {
	p := &Process{
		process:      process,
		namespacePID: namespacePID,
		exited:       make(chan Exit, 1),
	}

	go p.waitForExit()

	return p
}

// NamespacePID returns the host PID of the container process.
func (p *Process) NamespacePID() int {
	return p.namespacePID
}

// Signal sends an OS signal to the container process.
func (p *Process) Signal(signal os.Signal) error {
	err := p.process.Signal(signal)
	if err != nil {
		return fmt.Errorf("process signal: %w", err)
	}

	return nil
}

// waitForExit waits for the process exactly once.
func (p *Process) waitForExit() {
	state, err := p.process.Wait()
	if err != nil {
		p.exited <- Exit{
			Err:  fmt.Errorf("process wait failure: %w", err),
			Code: -1,
		}
		return
	}

	var code int
	if state != nil {
		code = state.ExitCode()
	}

	p.exited <- Exit{
		Err:  nil,
		Code: code,
	}
}

// Exited returns a channel that receives process exit notification.
func (p *Process) Exited() <-chan Exit {
	return p.exited
}
