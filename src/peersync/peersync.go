// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

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

	"github.com/podomy/concord/src/transport"
)

// PeerSync pulls journal state from a peer over the node transport.
// The concrete client lives in transport.Client (HTTPS/2 mTLS unary).
type PeerSync interface {
	Sync(ctx context.Context, peer netip.AddrPort, req transport.SyncRequest) (transport.SyncResponse, error)
}
