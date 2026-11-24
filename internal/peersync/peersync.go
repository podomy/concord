// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: BUSL-1.1

// Package peersync pulls journal state from peer nodes over the node
// transport and applies it locally (data sync reconciler).
//
// This is distinct from the workload reconciler (src/reconciler/), which
// reads local journal events and drives container lifecycle. peersync
// feeds the journal; the workload reconciler consumes it.
package peersync

import (
	"context"
	"net/netip"

	"github.com/podomy/concord/internal/transport"
)

// PeerSync pulls journal state from a peer over the node transport.
// The concrete client lives in transport.Client (Noise IK unary).
type PeerSync interface {
	Sync(ctx context.Context, peer netip.AddrPort, expect transport.Peer, req transport.SyncRequest) (transport.SyncResponse, error)
}
