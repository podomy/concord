// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/podomy/hive/src/journal"
	"github.com/podomy/hive/src/logs"
	"github.com/podomy/hive/src/node"
)

// main initialises the logger, sets up signal-based shutdown, and runs the node runtime.
func main() {
	logger, syncLogs, err := logs.Init()
	if err != nil {
		// Logger has not been initialized here; this is the only case where log is acceptable.
		log.Fatal(err)
	}
	defer func() {
		if err := syncLogs(); err != nil {
			logger.Warn("log sync failed", zap.Error(err))
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger); err != nil {
		logger.Fatal("runtime error", zap.Error(err))
	}
}

// run performs application startup, blocks for the process lifetime, and handles graceful shutdown.
func run(ctx context.Context, logger *zap.Logger) error {
	// Load persistent identity for this node, creating one if none exists.
	nodeConfig, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		return fmt.Errorf("load node config: %w", err)
	}

	// Open the JSONL journal file for appending runtime events.
	journalJSONLStore, err := journal.OpenJSONL()
	if err != nil {
		return fmt.Errorf("open journal: %w", err)
	}
	defer func() {
		if err := journalJSONLStore.Close(); err != nil {
			logger.Error("close journal", zap.Error(err))
		}
	}()

	// Create a startup event and persist it to the journal before announcing readiness.
	event := journal.NewEvent(nodeConfig.ID, "node.started", json.RawMessage(`{}`))
	if err := journalJSONLStore.Append(ctx, event); err != nil {
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
