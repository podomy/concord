// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package kvstore

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// KVStore wraps a bbolt database and provides access to the underlying DB.
type KVStore struct {
	mu sync.Mutex
	db *bolt.DB
	// closeOnce makes Close idempotent: only the first call closes.
	closeOnce sync.Once
	// closed records that Close ran. DB must not be used after Close;
	// callers must quiesce all View and Update calls before closing
	// because concurrent database use during Close is not safe.
	closed bool
}

// openDB opens or creates the bbolt database at the auto-determined path.
func openDB() (*bolt.DB, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return nil, fmt.Errorf("create config directory: %w", err)
	}

	db, err := bolt.Open(filepath.Join(appDir, "bbolt.db"), 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}

	return db, nil
}

// OpenDBPath opens a bbolt database at the provided path.
// The caller is responsible for closing the returned store.
func OpenDBPath(path string) (*KVStore, error) {
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}

	return &KVStore{db: db}, nil
}

// Open opens the bbolt database at the auto-determined path.
// The caller is responsible for calling Close when done.
func Open() (*KVStore, error) {
	db, err := openDB()
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}

	return &KVStore{db: db}, nil
}

// isClosed reports whether Close ran. The early check keeps repeat
// closes cheap; closeOnce below guarantees exactly one close.
func (s *KVStore) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Close closes the bbolt database. It is idempotent: only the first call
// closes, later calls return nil. Callers must quiesce all DB use before
// Close; concurrent View or Update during Close is not safe. DB stays
// valid until the first Close returns.
func (s *KVStore) Close() error {
	if s.isClosed() {
		return nil
	}
	var err error
	s.closeOnce.Do(func() {
		if cerr := s.db.Close(); cerr != nil {
			err = fmt.Errorf("close bbolt store: %w", cerr)
		}
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	})
	return err
}

// DB returns the underlying bbolt database for direct use.
// The caller must not close the returned DB; use Close instead. The DB
// must not be used after Close; concurrent use with Close is not safe.
func (s *KVStore) DB() *bolt.DB {
	return s.db
}
