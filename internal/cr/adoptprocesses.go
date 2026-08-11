// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"fmt"
	"os"
)

// AdoptedProcess implements ProcessHandle for containers that survived a Concord daemon
// restart or crash recovery.
//
// Background:
// When Concord launches a fresh container, libcontainer constructs an in-memory *libcontainer.Process
// struct containing internal pipes, FIFOs, and synchronization state. cr.Process wraps that struct
// to route Wait() and Signal() through libcontainer's process manager.
//
// However, when the Concord daemon restarts, containers created in previous runs may still be
// actively running in their Linux cgroups and network namespaces. Calling libcontainer.Load(stateDir, id)
// deserializes the container metadata from disk, but does NOT restore the original in-memory
// *libcontainer.Process handle or its launch FIFOs. Instead, ctr.State() provides only the host OS PID
// (InitProcessPid).
//
// AdoptedProcess bridges this gap: it implements ProcessHandle directly against the host OS
// process primitives (os.FindProcess, os.Process.Signal, os.Process.Wait) without requiring an in-memory
// *libcontainer.Process. This allows the reconciler and monitoring loops to treat freshly started and
// adopted containers uniformly through the ProcessHandle interface.
type AdoptedProcess struct {
	exited chan ExitStatus
	pid    int
}

// AdoptProcess wraps an existing host OS process PID in an AdoptedProcess handle
// that monitors its exit state in the background.
func AdoptProcess(pid int) ProcessHandle {
	p := AdoptedProcess{
		exited: make(chan ExitStatus, 1),
		pid:    pid,
	}

	go p.waitForExit()

	return &p
}

// NamespacePID returns the host PID of the adopted container process.
func (p *AdoptedProcess) NamespacePID() int {
	return p.pid
}

// Signal sends an OS signal directly to the host process using os.FindProcess.
func (p *AdoptedProcess) Signal(signal os.Signal) error {
	proc, err := os.FindProcess(p.pid)
	if err != nil {
		return fmt.Errorf("find process: %w", err)
	}

	err = proc.Signal(signal)
	if err != nil {
		return fmt.Errorf("process signal: %w", err)
	}

	return nil
}

// Exited returns a channel that receives the process exit status when terminated.
func (p *AdoptedProcess) Exited() <-chan ExitStatus {
	return p.exited
}

// waitForExit waits for the host process to terminate and delivers its exit code to Exited().
func (p *AdoptedProcess) waitForExit() {
	proc, err := os.FindProcess(p.pid)
	if err != nil {
		p.exited <- ExitStatus{
			Err:  fmt.Errorf("process find failure: %w", err),
			Code: -1,
		}
		return
	}

	state, err := proc.Wait()
	if err != nil {
		p.exited <- ExitStatus{
			Err:  fmt.Errorf("process wait failure: %w", err),
			Code: -1,
		}
		return
	}

	var code int
	if state != nil {
		code = state.ExitCode()
	}

	p.exited <- ExitStatus{
		Err:  nil,
		Code: code,
	}
}
