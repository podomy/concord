// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"net/netip"

	"github.com/podomy/concord/src/transport"
)

type PeerSync interface {
	Sync(ctx context.Context, peer netip.AddrPort, req transport.SyncRequest) (transport.SyncResponse, error)
}

// We must enforce tls 1.3 and http/2 only
