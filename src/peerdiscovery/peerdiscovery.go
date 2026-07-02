// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"net/netip"
	"time"

	"github.com/google/uuid"
)

type PeerDiscovery struct {
	ID       uuid.UUID
	Address  netip.AddrPort
	SeenAt   time.Time
	Protocol string
}
