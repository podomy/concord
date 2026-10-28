// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build linux

package main

import (
	"os"

	"github.com/opencontainers/runc/libcontainer"
	_ "github.com/opencontainers/runc/libcontainer/nsenter"
)

// maybeInitContainer handles libcontainer's re-exec: Container.Start
// re-executes /proc/self/exe with argv "init", and the re-executed
// process must become the container init instead of running the daemon.
// The nsenter import above runs its C constructor before Go main and
// joins the namespaces; Init reads the parent handshake and never
// returns on success. Without both halves, every container init dies
// as an unknown CLI command. Call before anything else in main: the
// init process must not open journals, bind sockets, or log.
func maybeInitContainer() {
	if isContainerInit(os.Args) {
		libcontainer.Init()
	}
}

// isContainerInit reports whether argv is libcontainer's re-exec for
// container init rather than a daemon or CLI invocation.
func isContainerInit(args []string) bool {
	return len(args) > 1 && args[1] == "init"
}
