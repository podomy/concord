// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/peerdiscovery"
)

// ErrNoMembers indicates that the cluster members list is empty.
var ErrNoMembers = errors.New("not enough members in the members list")

// getLeader determines the cluster leader by finding the member with the
// lowest lexicographical UUID string.
func getLeader(peerService *peerdiscovery.MemberService) (uuid.UUID, error) {
	if peerService == nil {
		return uuid.Nil, ErrNoMembers //nolint:wrapcheck // sentinel error
	}
	members, err := peerService.Members()
	if err != nil {
		return uuid.Nil, fmt.Errorf("member service: %w", err)
	}
	if len(members) == 0 {
		return uuid.Nil, ErrNoMembers //nolint:wrapcheck // sentinel error
	}

	minID := members[0].ID.String()
	for _, member := range members[1:] {
		if member.ID.String() < minID {
			minID = member.ID.String()
		}
	}

	leader, err := uuid.Parse(minID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse leader uuid: %w", err)
	}

	return leader, nil
}

// isLeader checks whether the given nodeID is currently the leader of the cluster.
func isLeader(myID uuid.UUID, peerService *peerdiscovery.MemberService) bool {
	leader, err := getLeader(peerService)
	if err != nil {
		return false
	}

	return leader == myID
}

// scheduleWorkloads inspects unassigned workload specs (SegmentID == uuid.Nil)
// and assigns them to the cluster node with the lowest active workload count.
func scheduleWorkloads(
	ctx context.Context,
	logger *zap.Logger,
	j journal.Journal,
	workloads *journalview.Workloads,
	peerService *peerdiscovery.MemberService,
	nodeID uuid.UUID,
	views []journalview.View,
) {
	if peerService == nil {
		return
	}
	members, err := peerService.Members()
	if err != nil {
		logger.Error("peerservice members", zap.Error(err))
		return
	}
	if len(members) == 0 {
		return
	}

	specs, err := workloads.List(ctx)
	if err != nil {
		logger.Error("workloads list", zap.Error(err))
		return
	}

	for _, spec := range specs {
		if spec.SegmentID != uuid.Nil || spec.Removed {
			continue
		}

		chosenMember := pickNode(members)
		spec.SegmentID = chosenMember.ID

		payload, err := json.Marshal(spec)
		if err != nil {
			logger.Error("json marshal", zap.Error(err))
			continue
		}

		event := journal.NewEvent(nodeID, "workload.spec", payload)
		if err := journalview.RecordEventAndLog(ctx, logger, j, views, event, "workload.spec"); err != nil {
			logger.Error("record workload.spec event", zap.Error(err))
		}
	}
}

// pickNode selects the peer discovery node with the fewest running workloads,
// prioritizing healthy (NodeStateAlive) peers over inactive/dead nodes.
func pickNode(members []peerdiscovery.Node) peerdiscovery.Node {
	best := members[0]

	for _, member := range members[1:] {
		// Prefer alive nodes over non-alive nodes.
		if member.State == peerdiscovery.NodeStateAlive && best.State != peerdiscovery.NodeStateAlive {
			best = member
			continue
		}
		// If both share the same liveness state, select the one with fewer workloads.
		if member.State == best.State && member.Metadata.Workloads < best.Metadata.Workloads {
			best = member
		}
	}

	return best
}
