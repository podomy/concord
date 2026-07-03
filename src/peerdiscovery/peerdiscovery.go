// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"net/netip"

	"github.com/google/uuid"
)

// NodeState describes memberlist's current liveness observation for a node.
type NodeState string

const (
	NodeStateAlive   NodeState = "alive"
	NodeStateSuspect NodeState = "suspect"
	NodeStateDead    NodeState = "dead"
	NodeStateLeft    NodeState = "left"
	NodeStateUnknown NodeState = "unknown"
)

// Node identifies a Concord node as it appears in peer discovery.
//
// ID is the stable Concord node identity. Address is the current network
// endpoint used by memberlist for peer membership traffic. The address can
// change over time; the ID is the durable identity. State is memberlist's
// current liveness observation for the node.
type Node struct {
	ID      uuid.UUID
	Address netip.AddrPort
	State   NodeState
}
