// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"github.com/podomy/hive/src/journal"
	"github.com/podomy/hive/src/node"
)

// Run performs application startup, blocks for the process lifetime, and handles graceful shutdown.
func Run(ctx context.Context, logger *zap.Logger) error {
	// Load persistent identity for this node, creating one if none exists.
	nodeConfig, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		return fmt.Errorf("load node config: %w", err)
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
	if err := recordEvent(ctx, st.journal, views, event); err != nil {
		return fmt.Errorf("append startup event: %w", err)
	}
	logger.Info("node runtime started",
		zap.String("node_id", nodeConfig.ID.String()),
		zap.String("event_id", event.ID.String()),
	)

	// Block until the OS delivers a shutdown signal.
	<-ctx.Done()
	logger.Info("shutting down", zap.String("node_id", nodeConfig.ID.String()))

	return nil
}
