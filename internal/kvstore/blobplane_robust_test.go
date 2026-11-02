// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package kvstore

import (
	"path/filepath"
	"sync"
	"testing"
)

// Concurrent Close calls are idempotent: exactly one closes and every
// caller reports nil, with no race on the closed flag.
func TestRobustConcurrentClose(t *testing.T) {
	t.Parallel()

	kv, err := OpenDBPath(filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatal(err)
	}
	if kv.DB() == nil {
		t.Fatal("DB is nil after open")
	}

	const closers = 8
	errs := make([]error, closers)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = kv.Close()
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("close %d = %v, want nil", i, err)
		}
	}
	if err := kv.Close(); err != nil {
		t.Fatalf("close after concurrent closes = %v, want nil", err)
	}
}
