// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package kvstore

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// openTempStore opens an isolated store for one test and registers Close.
func openTempStore(t *testing.T, name string) *KVStore {
	t.Helper()
	kv, err := OpenDBPath(filepath.Join(t.TempDir(), name))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cerr := kv.Close()
		if cerr != nil {
			t.Errorf("close kv: %v", cerr)
		}
	})
	return kv
}

// testOpenPath opens path and checks the wantErr outcome.
func testOpenPath(t *testing.T, path string, wantErr bool) {
	t.Helper()
	kv, err := OpenDBPath(path)
	if wantErr {
		if err == nil {
			t.Fatal("OpenDBPath succeeded, want error")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if kv.DB() == nil {
		t.Fatal("DB is nil after open")
	}
	cerr := kv.Close()
	if cerr != nil {
		t.Fatal(cerr)
	}
}

// TestOpenDBPath opens a fresh file and rejects a directory path.
func TestOpenDBPath(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{"fresh file", filepath.Join(dir, "kv.db"), false},
		{"path is a directory", dir, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testOpenPath(t, tc.path, tc.wantErr)
		})
	}
}

// putEntry stores one key in bucket.
func putEntry(t *testing.T, kv *KVStore, bucket, key, value string) {
	t.Helper()
	putErr := kv.DB().Update(func(tx *bolt.Tx) error {
		b, berr := tx.CreateBucketIfNotExists([]byte(bucket))
		if berr != nil {
			return fmt.Errorf("create bucket: %w", berr)
		}
		if perr := b.Put([]byte(key), []byte(value)); perr != nil {
			return fmt.Errorf("put key: %w", perr)
		}
		return nil
	})
	if putErr != nil {
		t.Fatal(putErr)
	}
}

// checkEntry asserts bucket holds key with value.
func checkEntry(t *testing.T, kv *KVStore, bucket, key, value string) {
	t.Helper()
	viewErr := kv.DB().View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			t.Errorf("bucket %q missing after put", bucket)
			return nil
		}
		if got := b.Get([]byte(key)); string(got) != value {
			t.Errorf("get(%q) = %q, want %q", key, got, value)
		}
		return nil
	})
	if viewErr != nil {
		t.Fatal(viewErr)
	}
}

// TestBucketPutGetRoundTrip stores several keys and reads them back.
func TestBucketPutGetRoundTrip(t *testing.T) {
	t.Parallel()

	kv := openTempStore(t, "roundtrip.db")
	entries := []struct {
		bucket, key, value string
	}{
		{"alpha", "k1", "v1"},
		{"alpha", "k2", "v2"},
		{"beta", "k1", "other"},
	}

	for _, e := range entries {
		t.Run(e.bucket+"/"+e.key, func(t *testing.T) {
			putEntry(t, kv, e.bucket, e.key, e.value)
			checkEntry(t, kv, e.bucket, e.key, e.value)
		})
	}
}

// TestMissingKeyNil pins that absent buckets and keys read back nil.
func TestMissingKeyNil(t *testing.T) {
	t.Parallel()

	kv := openTempStore(t, "missing.db")

	viewErr := kv.DB().View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte("absent")); b != nil {
			t.Error("absent bucket is not nil")
		}
		return nil
	})
	if viewErr != nil {
		t.Fatal(viewErr)
	}

	putEntry(t, kv, "present", "k", "v")

	missErr := kv.DB().View(func(tx *bolt.Tx) error {
		if got := tx.Bucket([]byte("present")).Get([]byte("absent")); got != nil {
			t.Errorf("absent key = %q, want nil", got)
		}
		return nil
	})
	if missErr != nil {
		t.Fatal(missErr)
	}
}

// TestCancelledContext pins that the store takes no context: a cancelled
// context stays observable to the caller while open and read still work.
func TestCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if cerr := ctx.Err(); !errors.Is(cerr, context.Canceled) {
		t.Fatalf("ctx.Err() = %v, want context.Canceled", cerr)
	}

	kv := openTempStore(t, "cancel.db")
	viewErr := kv.DB().View(func(_ *bolt.Tx) error {
		return nil
	})
	if viewErr != nil {
		t.Fatal(viewErr)
	}
}

// TestDoubleClose pins that Close is idempotent: closing twice returns nil.
func TestDoubleClose(t *testing.T) {
	t.Parallel()

	kv, err := OpenDBPath(filepath.Join(t.TempDir(), "twice.db"))
	if err != nil {
		t.Fatal(err)
	}
	err = kv.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = kv.Close()
	if err != nil {
		t.Fatalf("second close = %v, want nil", err)
	}
}
