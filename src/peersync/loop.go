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

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
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
// After each successful Sync, events are applied idempotently (skip known ids)
// into the local journal and views. The watermark advances only if apply succeeds.
//
// Watermarks live only in process memory: a process restart starts again
// from an empty watermark (full pull from the peer's start). Idempotent
// apply on event id is what prevents duplicate journal rows after replay.
//
// Individual Sync/apply failures are soft-fail (log and continue). The loop
// blocks until ctx is cancelled.
func RunPullLoop(
	ctx context.Context,
	logger *zap.Logger,
	selfID uuid.UUID,
	members MemberSource,
	syncer PeerSync,
	j journal.Journal,
	views []journalview.View,
	byID EventByID,
) {
	previous := map[uuid.UUID]peerdiscovery.Node{}
	watermarks := map[uuid.UUID]string{}
	ticker := time.NewTicker(defaultPullInterval)
	defer ticker.Stop()

	port := parseTransportPort(transport.Port)
	state := pullState{
		syncer:     syncer,
		journal:    j,
		views:      views,
		byID:       byID,
		port:       port,
		watermarks: watermarks,
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			previous = pullTick(ctx, logger, selfID, members, state, previous)
		}
	}
}

// pullState is the per-loop dependencies shared by meet/periodic/syncOne.
type pullState struct {
	syncer     PeerSync
	journal    journal.Journal
	views      []journalview.View
	byID       EventByID
	port       uint16
	watermarks map[uuid.UUID]string
}

// pullTick runs one reconciliation pass: list members, meet pulls, periodic pulls.
// Returns the new membership snapshot for the next tick's diff.
// On Members() error, returns previous unchanged and does not Sync.
func pullTick(
	ctx context.Context,
	logger *zap.Logger,
	selfID uuid.UUID,
	members MemberSource,
	state pullState,
	previous map[uuid.UUID]peerdiscovery.Node,
) map[uuid.UUID]peerdiscovery.Node {
	list, err := members.Members()
	if err != nil {
		logger.Error("list members", zap.Error(err))
		return previous
	}

	current := membersExceptSelf(selfID, list)
	synced := pullMeet(ctx, logger, state, previous, current)
	pullPeriodic(ctx, logger, state, current, synced)
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
	state pullState,
	previous, current map[uuid.UUID]peerdiscovery.Node,
) map[uuid.UUID]struct{} {
	synced := map[uuid.UUID]struct{}{}
	for id, member := range current {
		if !becameAlive(previous, id, member) {
			continue
		}
		if syncOne(ctx, logger, state, member) {
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
	state pullState,
	current map[uuid.UUID]peerdiscovery.Node,
	syncedThisTick map[uuid.UUID]struct{},
) {
	for id, member := range current {
		if member.State != peerdiscovery.NodeStateAlive {
			continue
		}
		if _, ok := syncedThisTick[id]; ok {
			continue
		}
		syncOne(ctx, logger, state, member)
	}
}

// syncOne pulls one page from member, applies events idempotently, then advances
// the watermark only if apply succeeded.
func syncOne(
	ctx context.Context,
	logger *zap.Logger,
	state pullState,
	member peerdiscovery.Node,
) bool {
	addr := netip.AddrPortFrom(member.Address.Addr(), state.port)
	req := transport.SyncRequest{
		Watermark: state.watermarks[member.ID],
		Limit:     defaultSyncLimit,
	}

	res, err := state.syncer.Sync(ctx, addr, req)
	if err != nil {
		logger.Warn("peer sync failed",
			zap.String("peer_id", member.ID.String()),
			zap.String("addr", addr.String()),
			zap.Error(err),
		)
		return false
	}

	applied, err := ApplyEvents(ctx, state.journal, state.views, state.byID, res.Events)
	if err != nil {
		logger.Warn("peer sync apply failed",
			zap.String("peer_id", member.ID.String()),
			zap.String("addr", addr.String()),
			zap.Error(err),
		)
		return false
	}

	if res.NextWatermark != "" {
		state.watermarks[member.ID] = res.NextWatermark
	}

	logger.Info("peer sync ok",
		zap.String("peer_id", member.ID.String()),
		zap.String("addr", addr.String()),
		zap.Int("events", len(res.Events)),
		zap.Int("applied", applied),
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
