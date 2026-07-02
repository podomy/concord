// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"fmt"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalview"
)

// recordEvent appends an event to the journal and applies it to every configured view.
func recordEvent(ctx context.Context, j journal.Journal, views []journalview.View, event journal.Event) error {
	if err := j.Append(ctx, event); err != nil {
		return fmt.Errorf("append event: %w", err)
	}

	for _, view := range views {
		if err := view.Apply(ctx, event); err != nil {
			return fmt.Errorf("apply event to view: %w", err)
		}
	}

	return nil
}
