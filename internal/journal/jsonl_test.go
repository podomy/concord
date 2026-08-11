// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journal

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestJSONLAppendCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	j, err := OpenJSONLPath(t.TempDir() + "/journal.jsonl")
	if err != nil {
		t.Fatalf("open jsonl: %v", err)
	}
	t.Cleanup(func() {
		if err := j.Close(); err != nil {
			t.Fatalf("close journal: %v", err)
		}
	})

	event := NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))
	if err := j.Append(ctx, event); err == nil {
		t.Fatalf("expected append cancellation error")
	}
}
