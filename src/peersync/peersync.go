// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

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
