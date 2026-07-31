// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package integration_test

import (
	"context"
	"encoding/pem"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalreader"
	"github.com/podomy/concord/src/journalview"
	"github.com/podomy/concord/src/kvstore"
	"github.com/podomy/concord/src/peerdiscovery"
)

func writePEM(t *testing.T, path, blockType string, der []byte) {
	t.Helper()

	// #nosec G304 — test only, paths are under t.TempDir.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer func() { _ = f.Close() }() //nolint:errcheck // best-effort

	if err := pem.Encode(f, &pem.Block{Type: blockType, Bytes: der}); err != nil {
		t.Fatalf("encode %s: %v", path, err)
	}
}

func openKV(t *testing.T, path string) *kvstore.KVStore {
	t.Helper()

	kv, err := kvstore.OpenDBPath(path)
	if err != nil {
		t.Fatalf("open kv %s: %v", path, err)
	}
	return kv
}

func openJSONL(t *testing.T, path string) *journal.JSONL {
	t.Helper()

	j, err := journal.OpenJSONLPath(path)
	if err != nil {
		t.Fatalf("open journal %s: %v", path, err)
	}
	return j
}

func initViews(t *testing.T, kv *kvstore.KVStore, journalPath string) (*journalview.EventsByID, []journalview.View) {
	t.Helper()

	eventsByID := journalview.NewEventsByID(kv)
	eventsByNode := journalview.NewEventsByNode(kv)
	eventsByType := journalview.NewEventsByType(kv)
	views := []journalview.View{eventsByID, eventsByNode, eventsByType}

	ctx := context.Background()

	jr, err := journalreader.OpenJSONLReaderPath(journalPath)
	if err != nil {
		t.Fatalf("open journal reader: %v", err)
	}
	defer func() {
		if err := jr.Close(); err != nil {
			t.Errorf("close journal reader: %v", err)
		}
	}()

	for _, view := range views {
		if err := view.Rebuild(ctx, jr); err != nil {
			t.Fatalf("rebuild view: %v", err)
		}
	}

	return eventsByID, views
}

func startMemberlist(t *testing.T, logger *zap.Logger, id uuid.UUID, bind netip.AddrPort, join []netip.AddrPort, advertise netip.Addr) *peerdiscovery.MemberService {
	t.Helper()

	node := peerdiscovery.Node{
		ID:      id,
		Address: bind,
	}

	ms, err := peerdiscovery.Start(logger, node, join, advertise)
	if err != nil {
		t.Fatalf("memberlist start: %v", err)
	}
	return ms
}

func shutDown(t *testing.T, ms *peerdiscovery.MemberService) {
	t.Helper()

	if err := ms.Shutdown(); err != nil {
		t.Logf("memberlist shutdown: %v", err)
	}
}

func waitForEvent(t *testing.T, byID *journalview.EventsByID, id uuid.UUID, timeout time.Duration) *journal.Event {
	t.Helper()

	pollCtx, pollCancel := context.WithTimeout(t.Context(), timeout)
	defer pollCancel()

	for {
		if pollCtx.Err() != nil {
			t.Fatalf("timed out waiting for event %s", id)
		}
		e, err := byID.Get(pollCtx, id)
		if err == nil && e != nil {
			return e
		}
		time.Sleep(200 * time.Millisecond)
	}
}
