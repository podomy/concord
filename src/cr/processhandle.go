// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"fmt"
	"os"

	"github.com/opencontainers/runc/libcontainer"
)

type Exit struct {
	Err  error
	Code int
}

type ProcessHandle interface {
	NamespacePID() int
	Wait() (*os.ProcessState, error)
	Signal(os.Signal) error
}

type Process struct {
	process      *libcontainer.Process
	namespacePID int
}

func NewProcess(process *libcontainer.Process, namespacePID int) *Process {
	return &Process{
		process:      process,
		namespacePID: namespacePID,
	}
}

func (p *Process) NamespacePID() int {
	return p.namespacePID
}

func (p *Process) Signal(signal os.Signal) error {
	err := p.process.Signal(signal)
	if err != nil {
		return fmt.Errorf("process signal: %w", err)
	}

	return nil
}

func (p *Process) Wait() (*os.ProcessState, error) {
	// Waiting for the process to exit.
	state, err := p.process.Wait()
	if err != nil {
		return nil, fmt.Errorf("process wait: %w", err)
	}

	return state, nil
}
