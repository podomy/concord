// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"

	"go.uber.org/zap"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/node"
)

// Run performs application startup, blocks for the process lifetime, and handles graceful shutdown.
func Run(ctx context.Context, logger *zap.Logger) error {
	// Load persistent identity for this node, creating one if none exists.
	nodeConfig, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		return fmt.Errorf("load node config: %w", err)
	}

	// The ip address and port
	if !nodeConfig.PeerAddress.IsValid() {
		nodeConfig.PeerAddress = netip.MustParseAddrPort("0.0.0.0:7946")
	}

	st, err := openStores()
	if err != nil {
		return err
	}
	defer func() {
		if err = st.kv.Close(); err != nil {
			logger.Error("close kv store", zap.Error(err))
		}
		if err = st.journal.Close(); err != nil {
			logger.Error("close journal", zap.Error(err))
		}
	}()

	// Initializing all of the views.
	eventsByID, eventsByNode, eventsByType := newViews(st.kv)
	views := viewList(eventsByID, eventsByNode, eventsByType)

	// Rebuilding all of the views.
	err = rebuildViews(ctx, views)
	if err != nil {
		return err
	}

	// Create a startup event and persist it before announcing readiness.
	event := journal.NewEvent(nodeConfig.ID, "node.started", json.RawMessage(`{}`))
	if err = recordEventAndLog(ctx, logger, st.journal, views, event, "node runtime started"); err != nil {
		return fmt.Errorf("append startup event: %w", err)
	}

	// Initialize the peer service.
	peerService, err := startPeerService(logger, nodeConfig)
	if err != nil {
		return err
	}
	defer func() {
		err := peerService.Shutdown()
		if err != nil {
			logger.Error("shutdown peer service", zap.Error(err))
		}
	}()

	// Start the discovery for the peer service.
	go observePeers(ctx, logger, nodeConfig.ID, peerService, st.journal, views)

	// Block until the OS delivers a shutdown signal.
	<-ctx.Done()
	logger.Info("shutting down", zap.String("node_id", nodeConfig.ID.String()))

	return nil
}
