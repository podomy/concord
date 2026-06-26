// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package kvstore

import (
	"fmt"
	"os"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

// KVStore wraps a bbolt database and provides access to the underlying DB.
type KVStore struct {
	db *bolt.DB
}

// openDB opens or creates the bbolt database at the auto-determined path.
func openDB() (*bolt.DB, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "hive")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return nil, fmt.Errorf("create config directory: %w", err)
	}

	db, err := bolt.Open(filepath.Join(appDir, "bbolt.db"), 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("open state database: %w", err)
	}

	return db, nil
}

// Open opens the bbolt database at the auto-determined path.
// The caller is responsible for calling Close when done.
func Open() (*KVStore, error) {
	db, err := openDB()
	if err != nil {
		return nil, err
	}

	return &KVStore{db: db}, nil
}

// Close closes the bbolt database.
func (s *KVStore) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close bbolt store: %w", err)
	}

	return nil
}

// DB returns the underlying bbolt database for direct use.
// The caller must not close the returned DB; use Close instead.
func (s *KVStore) DB() *bolt.DB {
	return s.db
}
