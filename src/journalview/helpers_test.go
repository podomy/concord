// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/podomy/concord/src/kvstore"
)

func testKVStore(t *testing.T) *kvstore.KVStore {
	t.Helper()

	kv, err := kvstore.OpenDBPath(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open db path: %v", err)
	}

	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Fatalf("test close db: %v", err)
		}
	})

	return kv
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
