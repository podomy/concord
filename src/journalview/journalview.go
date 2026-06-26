// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"

	"github.com/podomy/hive/src/journal"
	"github.com/podomy/hive/src/journalreader"
)

// View represents a read-optimised projection of journal events.
// Views are kept in sync by applying events as they are appended to the journal.
type View interface {
	// Apply processes a single event to keep the view up to date.
	Apply(ctx context.Context, event journal.Event) error

	// Rebuild reconstructs the entire view by replaying the journal from scratch.
	Rebuild(ctx context.Context, jr journalreader.Reader) error
}
