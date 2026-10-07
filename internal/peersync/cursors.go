// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"github.com/google/uuid"
)

// cursorSet holds one sync cursor per peer: the id of the last journal
// event already pulled from that peer. A missing key means nothing has
// been pulled yet, so the next pull starts from the peer's first event.
//
// The set lives in process memory only. A restart starts from empty
// cursors and re-pulls from event zero; idempotent apply by event id
// discards the overlap. See docs/cursors.md for the full mechanics.
type cursorSet map[uuid.UUID]string

// newCursorSet creates an empty per-peer cursor set.
func newCursorSet() cursorSet {
	return cursorSet{}
}

// lookup returns the stored cursor for peer, or "" when nothing has been
// pulled from them yet (first contact).
func (c cursorSet) lookup(peer uuid.UUID) string {
	return c[peer]
}

// advance moves peer's cursor to next, unless next is empty. An empty
// NextWatermark means the peer had nothing new, so the stored cursor
// must be kept, never cleared.
func (c cursorSet) advance(peer uuid.UUID, next string) {
	if next == "" {
		return
	}
	c[peer] = next
}
