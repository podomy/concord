// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/podomy/concord/internal/cli"
	"github.com/podomy/concord/internal/logs"
	concordruntime "github.com/podomy/concord/internal/runtime"
)

func startDaemon(ctx context.Context) error {
	logger, syncLogs, err := logs.Init()
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer func() {
		if err := syncLogs(); err != nil {
			logger.Warn("log sync failed", zap.Error(err))
		}
	}()

	if err := concordruntime.Run(ctx, logger); err != nil {
		return fmt.Errorf("runtime: %w", err)
	}

	return nil
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// If cli arguments were specified we run our cli instead
	// of running the daemon.
	if len(os.Args) > 1 && os.Args[1] != "daemon" {
		if err := cli.Execute(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
			return fmt.Errorf("cli: %w", err)
		}

		return nil
	}

	return startDaemon(ctx)
}

func main() {
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err) //nolint:errcheck // CLI fatal output
		os.Exit(1)
	}
}
