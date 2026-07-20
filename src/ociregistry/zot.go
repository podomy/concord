// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package ociregistry wraps an embedded zot OCI distribution registry.
//
// Each Concord node runs a local zot instance so workloads can pull
// container images from localhost. Images are reconciled between nodes
// via the OCI distribution protocol — when two nodes meet, they exchange
// manifests and blobs they do not yet have.
//
// The registry storage lives under the user config directory at
// ~/.config/concord/zot/ and listens on the port defined by Port.
package ociregistry

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"zotregistry.dev/zot/v2/pkg/api"
	"zotregistry.dev/zot/v2/pkg/api/config"
)

// Port is the TCP port the embedded zot registry listens on.
// It is a variable so tests and deployments can override it.
var Port = 8444

// Registry wraps a zot controller and manages its lifecycle.
type Registry struct {
	controller *api.Controller
}

// rootDir returns the on-disk storage directory for the local zot
// registry, creating it if it does not exist.
func rootDir() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord", "zot")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return "", fmt.Errorf("create zot directory: %w", err)
	}

	return appDir, nil
}

// New creates a new Registry with storage and HTTP configuration wired
// to the default data directory and port. It does not start serving.
func New() (*Registry, error) {
	root, err := rootDir()
	if err != nil {
		return nil, err
	}

	cfg := config.New()
	cfg.Storage.RootDirectory = root
	cfg.HTTP.Address = "0.0.0.0"
	cfg.HTTP.Port = strconv.Itoa(Port)
	cfg.Log.Level = "error"
	cfg.Extensions = nil

	return &Registry{
		controller: api.NewController(cfg),
	}, nil
}

// Start initialises the storage backend and begins serving the OCI
// distribution API in the background. When ctx is cancelled the server
// shuts down gracefully.
func (r *Registry) Start(ctx context.Context) error {
	if err := r.controller.Init(); err != nil {
		return fmt.Errorf("zot init: %w", err)
	}

	go func() {
		if err := r.controller.Run(); err != nil {
			return
		}
	}()

	go func() {
		<-ctx.Done()
		r.Stop()
	}()

	return nil
}

// Stop shuts down the registry server and releases its resources.
func (r *Registry) Stop() {
	r.controller.Shutdown()
}
