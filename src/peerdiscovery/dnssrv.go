// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"net/netip"
	"time"
)

type DNSSRVResolver struct {
	Timeout time.Duration
}

func (d *DNSSRVResolver) Resolve(ctx context.Context) ([]netip.AddrPort, error) {
	return nil, nil
}
