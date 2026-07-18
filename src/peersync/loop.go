// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"net/netip"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/src/peerdiscovery"
	"github.com/podomy/concord/src/transport"
)

const (
	// defaultPullInterval is how often we re-read membership and pull peers.
	defaultPullInterval = 5 * time.Second
	// defaultSyncLimit is the max events per Sync page (keeps transfers small).
	defaultSyncLimit = 100
)

// MemberSource lists current membership (memberlist).
// *peerdiscovery.MemberService implements it.
type MemberSource interface {
	Members() ([]peerdiscovery.Node, error)
}

// RunPullLoop is the peer journal reconciliation loop.
//
// It does not push our events. Each tick it discovers who is alive via
// memberlist and pulls a page of their journal over HTTPS mTLS:
//
//   - On meet: peer is newly seen or has become alive again → Sync once now.
//   - Periodic: every defaultPullInterval, Sync all still-alive peers
//     (skipped if already synced this tick for a meet).
//
// Progress per peer is a watermark (opaque cursor, often last event id).
// Watermarks live only in process memory: a process restart starts again
// from an empty watermark (full pull from the peer's start). Idempotent
// apply on event id is what prevents duplicate journal rows after replay.
//
// Individual Sync failures are soft-fail (log and continue). The loop
// blocks until ctx is cancelled.
func RunPullLoop(
	ctx context.Context,
	logger *zap.Logger,
	selfID uuid.UUID,
	members MemberSource,
	syncer PeerSync,
) {
	// previous: last membership snapshot (for meet / re-alive detection).
	previous := map[uuid.UUID]peerdiscovery.Node{}
	// watermarks: peer ID → last successful NextWatermark from that peer.
	watermarks := map[uuid.UUID]string{}
	ticker := time.NewTicker(defaultPullInterval)
	defer ticker.Stop()

	port := parseTransportPort(transport.Port)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			previous = pullTick(ctx, logger, selfID, members, syncer, port, previous, watermarks)
		}
	}
}

// pullTick runs one reconciliation pass: list members, meet pulls, periodic pulls.
// Returns the new membership snapshot for the next tick's diff.
// On Members() error, returns previous unchanged and does not Sync.
func pullTick(
	ctx context.Context,
	logger *zap.Logger,
	selfID uuid.UUID,
	members MemberSource,
	syncer PeerSync,
	port uint16,
	previous map[uuid.UUID]peerdiscovery.Node,
	watermarks map[uuid.UUID]string,
) map[uuid.UUID]peerdiscovery.Node {
	list, err := members.Members()
	if err != nil {
		logger.Error("list members", zap.Error(err))
		return previous
	}

	current := membersExceptSelf(selfID, list)
	synced := pullMeet(ctx, logger, syncer, port, previous, current, watermarks)
	pullPeriodic(ctx, logger, syncer, port, current, synced, watermarks)
	return current
}

// membersExceptSelf builds a map of peers, dropping the local node.
func membersExceptSelf(selfID uuid.UUID, members []peerdiscovery.Node) map[uuid.UUID]peerdiscovery.Node {
	current := make(map[uuid.UUID]peerdiscovery.Node, len(members))
	for _, member := range members {
		if member.ID == selfID {
			continue
		}
		current[member.ID] = member
	}
	return current
}

// pullMeet Syncs peers that became alive this tick (first sight or dead→alive).
// Returns the set of peer IDs successfully synced so periodic can skip them.
func pullMeet(
	ctx context.Context,
	logger *zap.Logger,
	syncer PeerSync,
	port uint16,
	previous, current map[uuid.UUID]peerdiscovery.Node,
	watermarks map[uuid.UUID]string,
) map[uuid.UUID]struct{} {
	synced := map[uuid.UUID]struct{}{}
	for id, member := range current {
		if !becameAlive(previous, id, member) {
			continue
		}
		if syncOne(ctx, logger, syncer, member, port, watermarks) {
			synced[id] = struct{}{}
		}
	}
	return synced
}

// becameAlive reports a meet edge: member is alive now and was not alive before
// (missing from previous, or previous state was not alive).
func becameAlive(previous map[uuid.UUID]peerdiscovery.Node, id uuid.UUID, member peerdiscovery.Node) bool {
	if member.State != peerdiscovery.NodeStateAlive {
		return false
	}
	old, seen := previous[id]
	return !seen || old.State != peerdiscovery.NodeStateAlive
}

// pullPeriodic Syncs every currently alive peer that was not already synced
// this tick (avoids double Sync on the meet tick).
func pullPeriodic(
	ctx context.Context,
	logger *zap.Logger,
	syncer PeerSync,
	port uint16,
	current map[uuid.UUID]peerdiscovery.Node,
	syncedThisTick map[uuid.UUID]struct{},
	watermarks map[uuid.UUID]string,
) {
	for id, member := range current {
		if member.State != peerdiscovery.NodeStateAlive {
			continue
		}
		if _, ok := syncedThisTick[id]; ok {
			continue
		}
		syncOne(ctx, logger, syncer, member, port, watermarks)
	}
}

// syncOne pulls one page from member over the HTTPS transport port (not gossip).
// On success, stores NextWatermark when non-empty. On failure, leaves the
// watermark unchanged so the next attempt retries the same cursor.
func syncOne(
	ctx context.Context,
	logger *zap.Logger,
	syncer PeerSync,
	member peerdiscovery.Node,
	port uint16,
	watermarks map[uuid.UUID]string,
) bool {
	// Member.Address is the memberlist endpoint; Sync uses the same IP + transport port.
	addr := netip.AddrPortFrom(member.Address.Addr(), port)
	req := transport.SyncRequest{
		// Missing map key → "" → peer starts from the beginning of its journal.
		Watermark: watermarks[member.ID],
		Limit:     defaultSyncLimit,
	}

	res, err := syncer.Sync(ctx, addr, req)
	if err != nil {
		logger.Warn("peer sync failed",
			zap.String("peer_id", member.ID.String()),
			zap.String("addr", addr.String()),
			zap.Error(err),
		)
		return false
	}

	// Only advance the cursor when the peer gives a new mark.
	if res.NextWatermark != "" {
		watermarks[member.ID] = res.NextWatermark
	}

	logger.Info("peer sync ok",
		zap.String("peer_id", member.ID.String()),
		zap.String("addr", addr.String()),
		zap.Int("events", len(res.Events)),
		zap.String("next_watermark", res.NextWatermark),
	)
	return true
}

// parseTransportPort parses transport.Port; invalid or zero falls back to 8443.
func parseTransportPort(port string) uint16 {
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return 8443
	}
	return uint16(n)
}
