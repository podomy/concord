// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package or wraps an embedded zot OCI distribution registry.
//
// Each Concord node runs a local zot instance so workloads can pull
// container images from localhost. Images are reconciled between nodes
// via the OCI distribution protocol - when two nodes meet, they exchange
// manifests and blobs they do not yet have.
//
// The registry storage lives under the user config directory at
// ~/.config/concord/zot/ and listens on the port defined by Port.
package or

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"zotregistry.dev/zot/v2/pkg/api"
	"zotregistry.dev/zot/v2/pkg/api/config"
	extconf "zotregistry.dev/zot/v2/pkg/extensions/config"
	synccfg "zotregistry.dev/zot/v2/pkg/extensions/config/sync"

	"github.com/podomy/concord/src/peerdiscovery"
)

// Port is the TCP port the embedded zot registry listens on.
// It is a variable so tests and deployments can override it.
var Port = 8444

// Registry wraps a zot controller and manages its lifecycle.
type Registry struct {
	controller *api.Controller
	nodeID     uuid.UUID
	stopOnce   sync.Once
}

// rootDirPath returns the on-disk storage directory for the local zot
// registry, creating it if it does not exist.
func rootDirPath() string {
	dir, err := os.UserConfigDir()
	if err != nil {
		return ""
	}
	return filepath.Join(dir, "concord", "zot")
}

// baseConfig returns a config.Config with shared non-storage fields preset.
func baseConfig() *config.Config {
	cfg := config.New()
	cfg.HTTP.Address = "0.0.0.0"
	cfg.HTTP.Port = strconv.Itoa(Port)
	cfg.Log.Level = "error"
	return cfg
}

// New creates a new Registry with storage and HTTP configuration wired
// to the default data directory and port. It does not start serving.
func New(nodeID uuid.UUID) (*Registry, error) {
	root := rootDirPath()
	if root == "" {
		return nil, fmt.Errorf("get user config directory") //nolint:perfsprint // plain error, no wrap target
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create zot directory: %w", err)
	}

	cfg := baseConfig()
	cfg.Storage.RootDirectory = root
	cfg.Extensions = nil

	return &Registry{
		controller: api.NewController(cfg),
		nodeID:     nodeID,
	}, nil
}

// Start initialises the storage backend and begins serving the OCI
// distribution API in the background. When ctx is cancelled the server
// shuts down gracefully.
func (r *Registry) Start(ctx context.Context, memberService *peerdiscovery.MemberService, logger *zap.Logger) error {
	if err := r.controller.Init(); err != nil {
		return fmt.Errorf("zot init: %w", err)
	}

	go func() {
		if err := r.controller.Run(); err != nil {
			return
		}
	}()

	// On peer changes we reload the sync extension config with current peer registries.
	go r.monitorPeers(ctx, memberService, logger)

	go func() {
		<-ctx.Done()
		r.Stop()
	}()

	return nil
}

// reloadSyncConfig rebuilds the zot config with the sync extension pointing at current peer registries.
func (r *Registry) reloadSyncConfig(nodes []peerdiscovery.Node) {
	cfg := baseConfig()
	cfg.Storage.RootDirectory = rootDirPath()

	// Build registry list for sync extension (other nodes' zot instances).
	var registryConfigs []synccfg.RegistryConfig
	for _, node := range nodes {
		// Skip ourselves.
		if node.ID == r.nodeID {
			continue
		}
		registryConfigs = append(registryConfigs, synccfg.RegistryConfig{
			URLs:         []string{"http://" + node.Address.String()},
			PollInterval: 5 * time.Second,
			OnDemand:     true,
		})
	}

	if len(registryConfigs) > 0 {
		cfg.Extensions = &extconf.ExtensionConfig{
			Sync: &synccfg.Config{
				Registries: registryConfigs,
			},
		}
	}

	// Load the new config into the running controller.
	r.controller.LoadNewConfig(cfg)
}

// monitorPeers polls the member service for peer changes and reloads the
// zot sync config when the peer list changes.
func (r *Registry) monitorPeers(ctx context.Context, memberService *peerdiscovery.MemberService, logger *zap.Logger) {
	var previousNodes []peerdiscovery.Node

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		currentNodes, err := memberService.Members()
		if err != nil {
			logger.Error("member service", zap.Error(err))
			continue
		}

		if !nodesEqual(previousNodes, currentNodes) {
			logger.Info("peer list changed, reloading zot sync config",
				zap.Int("peers", len(currentNodes)))
			r.reloadSyncConfig(currentNodes)
			previousNodes = currentNodes
		}

		// Wait for the ticker.
		<-ticker.C
	}
}

// nodesEqual compares two node slices for equality (order-independent).
func nodesEqual(a, b []peerdiscovery.Node) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[uuid.UUID]struct{})
	for _, n := range a {
		m[n.ID] = struct{}{}
	}
	for _, n := range b {
		if _, ok := m[n.ID]; !ok {
			return false
		}
	}
	return true
}

// Stop shuts down the registry server and releases its resources.
// Safe to call multiple times.
func (r *Registry) Stop() {
	r.stopOnce.Do(func() {
		r.controller.Shutdown()
	})
}
