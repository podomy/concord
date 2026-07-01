// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/podomy/hive/src/logs"
	hiveruntime "github.com/podomy/hive/src/runtime"
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

	if err := hiveruntime.Run(ctx, logger); err != nil {
		logger.Fatal("runtime error", zap.Error(err))
	}
}
