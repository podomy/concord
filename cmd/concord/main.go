// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/podomy/concord/internal/cli"
	"github.com/podomy/concord/internal/logs"
	concordruntime "github.com/podomy/concord/internal/runtime"
)

func startDaemon(ctx context.Context, level logs.Level) error {
	logger, syncLogs, err := logs.Init(level)
	if err != nil {
		return fmt.Errorf("init logger: %w", err)
	}
	defer func() {
		if err := syncLogs(); err != nil {
			logger.Warn("log sync failed", logs.Error(err))
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

	// Leading flags belong to the daemon (currently only --anchor, which
	// marks this node as a rendezvous anchor). Anything else goes
	// to the CLI, including --help.
	if len(os.Args) == 1 || (strings.HasPrefix(os.Args[1], "-") && os.Args[1] != "-h" && os.Args[1] != "--help") {
		return runDaemon(ctx, os.Args[1:])
	}

	if err := cli.Execute(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		return fmt.Errorf("cli: %w", err)
	}

	return nil
}

// runDaemon provisions anchor mode when flagged, then starts the node.
func runDaemon(ctx context.Context, args []string) error {
	flags, err := parseDaemonFlags(args)
	if err != nil {
		return err
	}
	level, err := logs.ParseLevel(flags.logLevel)
	if err != nil {
		return fmt.Errorf("parse log level: %w", err)
	}
	if flags.anchor {
		changed, err := provisionAnchor()
		if err != nil {
			return err
		}
		if changed {
			_, _ = fmt.Fprintln(os.Stdout, "provisioned as rendezvous anchor") //nolint:errcheck // startup output
		}
	}

	return startDaemon(ctx, level)
}

func main() {
	maybeInitContainer()
	if err := run(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err) //nolint:errcheck // CLI fatal output
		os.Exit(1)
	}
}
