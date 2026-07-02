// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"fmt"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/kvstore"
)

// stores groups the runtime's persistent storage handles.
type stores struct {
	kv      *kvstore.KVStore
	journal *journal.JSONL
}

// openStores initialises the bbolt key-value store and the JSONL journal.
func openStores() (*stores, error) {
	kvStore, err := kvstore.Open()
	if err != nil {
		return nil, fmt.Errorf("load kv store: %w", err)
	}

	journalStore, err := journal.OpenJSONL()
	if err != nil {
		if closeErr := kvStore.Close(); closeErr != nil {
			return nil, fmt.Errorf("open journal: %w; close kv store: %w", err, closeErr)
		}
		return nil, fmt.Errorf("open journal: %w", err)
	}

	return &stores{kv: kvStore, journal: journalStore}, nil
}
