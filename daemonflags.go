// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"flag"
	"fmt"
	"io"

	"github.com/podomy/concord/internal/node"
)

// daemonFlags holds daemon-only startup options.
type daemonFlags struct {
	// anchor marks this node as a rendezvous anchor in node config.
	anchor bool
	// logLevel selects the daemon log verbosity.
	logLevel string
}

// parseDaemonFlags parses daemon-only flags. Unknown flags fail;
// --help is handled by the caller routing to the CLI instead.
func parseDaemonFlags(args []string) (daemonFlags, error) {
	var flags daemonFlags
	fs := flag.NewFlagSet("concord", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.BoolVar(&flags.anchor, "anchor", false, "Serve as a rendezvous anchor")
	fs.StringVar(&flags.logLevel, "log-level", "info", "Log verbosity: debug, info, warn, or error")
	err := fs.Parse(args)
	if err != nil {
		return daemonFlags{}, fmt.Errorf("parse daemon flags: %w", err)
	}
	if fs.NArg() > 0 {
		return daemonFlags{}, fmt.Errorf("unexpected daemon arguments: %v", fs.Args())
	}
	return flags, nil
}

// provisionAnchor marks this node as a rendezvous anchor in node config
// and persists the result. It reports whether the flag changed anything.
func provisionAnchor() (bool, error) {
	config, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		return false, fmt.Errorf("load node config: %w", err)
	}
	if config.Anchor {
		return false, nil
	}
	config.Anchor = true

	_, err = node.UpdateNodeConfig(config)
	if err != nil {
		return false, fmt.Errorf("persist node config: %w", err)
	}
	return true, nil
}
