// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/opencontainers/runc/libcontainer"
	"github.com/opencontainers/runc/libcontainer/configs"
)

// ContainerRuntime manages the lifecycle of libcontainer-backed workloads.
// Each container gets a subdirectory under stateDir where libcontainer stores
// its checkpoint data, FIFOs, and config state.
type ContainerRuntime struct {
	stateDir string
}

// stateDirPath returns the directory libcontainer uses to store container
// state. It lives under the user config directory at ~/.config/concord/cr/.
func stateDirPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord", "cr")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return "", fmt.Errorf("create container runtime directory: %w", err)
	}

	return appDir, nil
}

// NewRuntime creates a ContainerRuntime with its state directory under
// ~/.config/concord/cr/. Multiple containers share this state directory;
// each is keyed by its unique ID.
func NewRuntime() (*ContainerRuntime, error) {
	stateDir, err := stateDirPath()
	if err != nil {
		return nil, err
	}
	return &ContainerRuntime{stateDir: stateDir}, nil
}

// Create sets up a new container with the given configs.Config but does not
// start any process inside it. Call Start to run the init process.
func (r *ContainerRuntime) Create(id string, cfg *configs.Config) (*libcontainer.Container, error) {
	container, err := libcontainer.Create(r.stateDir, id, cfg)
	if err != nil {
		return nil, fmt.Errorf("libcontainer create: %w", err)
	}
	return container, nil
}

// Start runs the init process inside the container and returns once it is
// running (non-blocking). The container must have been created with Create
// first. Use ctr.Run instead to wait for the process to exit.
func (r *ContainerRuntime) Start(ctr *libcontainer.Container, proc *libcontainer.Process) error {
	err := ctr.Start(proc)
	if err != nil {
		return fmt.Errorf("libcontainer start: %w", err)
	}
	return nil
}
