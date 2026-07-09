// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
)

// ObservePeers polls the memberlist every 5 seconds, diffs the result against
// the previous snapshot, and records peer.seen, peer.updated, and peer.lost
// events. It blocks until the context is cancelled.
func ObservePeers(
	ctx context.Context,
	logger *zap.Logger,
	localNodeID uuid.UUID,
	memberService *MemberService,
	j journal.Journal,
	views []journalview.View,
) {
	previous := map[uuid.UUID]Node{}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Error("context cancelation", zap.Error(ctx.Err()))
			return
		case <-ticker.C:
			current, err := pollPeerChanges(ctx, logger, localNodeID, memberService, j, views, previous)
			if err != nil {
				logger.Error("poll peer changes", zap.Error(err))
				continue
			}

			previous = current
		}
	}
}

func pollPeerChanges(
	ctx context.Context,
	logger *zap.Logger,
	localNodeID uuid.UUID,
	memberService *MemberService,
	j journal.Journal,
	views []journalview.View,
	previous map[uuid.UUID]Node,
) (map[uuid.UUID]Node, error) {
	currentMembers, err := memberService.Members()
	if err != nil {
		return nil, fmt.Errorf("list peer members: %w", err)
	}

	current := currentPeerMap(localNodeID, currentMembers)
	recordSeenOrUpdatedPeers(ctx, logger, j, views, localNodeID, previous, current)
	recordLostPeers(ctx, logger, j, views, localNodeID, previous, current)

	return current, nil
}

func currentPeerMap(localNodeID uuid.UUID, members []Node) map[uuid.UUID]Node {
	current := map[uuid.UUID]Node{}
	for _, member := range members {
		if member.ID == localNodeID {
			continue
		}

		current[member.ID] = member
	}

	return current
}

func recordSeenOrUpdatedPeers(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	views []journalview.View,
	localNodeID uuid.UUID,
	previous map[uuid.UUID]Node,
	current map[uuid.UUID]Node,
) {
	for id, member := range current {
		old, exists := previous[id]
		if !exists {
			if err := recordPeerEvent(ctx, logger, j, views, localNodeID, "peer.seen", member); err != nil {
				logger.Error("record peer.seen", zap.Error(err))
			}
			continue
		}

		if old.Address != member.Address || old.State != member.State {
			if err := recordPeerEvent(ctx, logger, j, views, localNodeID, "peer.updated", member); err != nil {
				logger.Error("record peer.updated", zap.Error(err))
			}
		}
	}
}

func recordLostPeers(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	views []journalview.View,
	localNodeID uuid.UUID,
	previous map[uuid.UUID]Node,
	current map[uuid.UUID]Node,
) {
	for id, old := range previous {
		if _, exists := current[id]; !exists {
			if err := recordPeerEvent(ctx, logger, j, views, localNodeID, "peer.lost", old); err != nil {
				logger.Error("record peer.lost", zap.Error(err))
			}
		}
	}
}

type peerEventPayload struct {
	Address string    `json:"address"`
	State   NodeState `json:"state"`
	PeerID  uuid.UUID `json:"peer_id"`
}

func recordPeerEvent(ctx context.Context, logger *zap.Logger, j journal.Journal, views []journalview.View, localNodeID uuid.UUID, eventType string, peer Node) error {
	payload, err := json.Marshal(peerEventPayload{
		PeerID:  peer.ID,
		Address: peer.Address.String(),
		State:   peer.State,
	})
	if err != nil {
		return fmt.Errorf("marshal peer event payload: %w", err)
	}

	event := journal.NewEvent(localNodeID, eventType, payload)
	if err := journalview.RecordEventAndLog(ctx, logger, j, views, event, eventType,
		zap.String("peer_id", peer.ID.String()),
		zap.String("peer_address", peer.Address.String()),
		zap.String("peer_state", string(peer.State)),
	); err != nil {
		return fmt.Errorf("record peer event: %w", err)
	}

	return nil
}
