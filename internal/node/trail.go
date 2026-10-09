// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"time"

	"github.com/podomy/concord/internal/geo"
)

// trailSize caps the position trail. Move-triggered appends make 720
// entries cover long roams; memory stays flat forever.
const trailSize = 720

// trailMoveKM is how far the node must move before the trail records.
// Ten meters keeps depot jitter out while catching real roams.
const trailMoveKM = 0.01

// trailPoint is one timestamped position.
type trailPoint struct {
	At  time.Time
	Pos geo.Point
}

// trail is a fixed-size round-robin of positions: appends overwrite the
// oldest once full. Not safe for concurrent use; the Sampler guards it.
type trail struct {
	pts  []trailPoint
	next int
	full bool
}

// newTrail creates an empty trail holding up to trailSize points.
func newTrail() *trail {
	return &trail{pts: make([]trailPoint, 0, trailSize)}
}

// add appends a point, overwriting the oldest once full.
func (t *trail) add(p trailPoint) {
	if len(t.pts) < trailSize {
		t.pts = append(t.pts, p)
		t.next = len(t.pts) % trailSize
		return
	}
	t.pts[t.next] = p
	t.next = (t.next + 1) % trailSize
	t.full = true
}

// points returns the held points oldest-first.
func (t *trail) points() []trailPoint {
	if !t.full {
		out := make([]trailPoint, len(t.pts))
		copy(out, t.pts)
		return out
	}
	out := make([]trailPoint, 0, trailSize)
	out = append(out, t.pts[t.next:]...)
	out = append(out, t.pts[:t.next]...)
	return out
}

// last returns the newest point, or false when the trail is empty.
func (t *trail) last() (trailPoint, bool) {
	if len(t.pts) == 0 {
		return trailPoint{}, false
	}
	if !t.full {
		return t.pts[len(t.pts)-1], true
	}
	return t.pts[(t.next+trailSize-1)%trailSize], true
}
