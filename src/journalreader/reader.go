// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalreader

import (
	"context"

	"github.com/podomy/hive/src/journal"
)

// Reader reads events from the journal sequentially.
type Reader interface {
	// Read reads the next event from the journal.
	// Returns io.EOF when all events have been consumed.
	Read(ctx context.Context) (*journal.Event, error)

	// Close closes the underlying journal file.
	Close() error
}
