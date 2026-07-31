// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package integration_test

import (
	"context"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"

	"github.com/podomy/concord/src/certs"
	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalreader"
	"github.com/podomy/concord/src/journalview"
	"github.com/podomy/concord/src/kvstore"
	"github.com/podomy/concord/src/peerdiscovery"
	"github.com/podomy/concord/src/peersync"
	"github.com/podomy/concord/src/transport"
)

// TestTwoNodeEventSync validates that two nodes discover each other via
// memberlist and sync a journal event from A to B through the pull loop.
//
// This test only exercises the event sync layer (peersync). Container
// lifecycle, networking (bridge/veth/nat), and image pulling require
// root privileges and a running zot registry, those belong in a
// separate integration test.
func TestTwoNodeEventSync(t *testing.T) {
	dirA, dirB := t.TempDir(), t.TempDir()
	idA, idB := uuid.New(), uuid.New()

	provisionNodes(t, idA, idB, dirA, dirB)

	kvA := openKV(t, filepath.Join(dirA, "concord", "bbolt.db"))
	jA := openJSONL(t, filepath.Join(dirA, "concord", "journal.jsonl"))
	_, viewsA := initViews(t, kvA, filepath.Join(dirA, "concord", "journal.jsonl"))

	kvB := openKV(t, filepath.Join(dirB, "concord", "bbolt.db"))
	jB := openJSONL(t, filepath.Join(dirB, "concord", "journal.jsonl"))
	eventsByIDB, viewsB := initViews(t, kvB, filepath.Join(dirB, "concord", "journal.jsonl"))

	logger := zaptest.NewLogger(t)
	t.Setenv("XDG_CONFIG_HOME", dirA)

	// Node A
	ctxA, cancelA := context.WithCancel(t.Context())
	t.Cleanup(cancelA)

	peerA := startMemberlist(t, logger, idA, netip.MustParseAddrPort("127.0.0.1:17946"), nil)
	t.Cleanup(func() { shutDown(t, peerA) })

	origPort := transport.Port
	transport.Port = "18443"
	t.Cleanup(func() { transport.Port = origPort })

	caPathA := filepath.Join(dirA, "concord", "certs", "ca.crt")
	certPathA := filepath.Join(dirA, "concord", "certs", "node.crt")
	keyPathA := filepath.Join(dirA, "concord", "certs", "node.key")
	if err := transport.Start(ctxA, logger, caPathA, certPathA, keyPathA); err != nil {
		t.Fatalf("A transport: %v", err)
	}

	if err := journalview.RecordNodeStarted(ctxA, logger, jA, viewsA, idA, netip.MustParseAddrPort("127.0.0.1:17946")); err != nil {
		t.Fatalf("A record started: %v", err)
	}

	// Node B
	ctxB, cancelB := context.WithCancel(t.Context())
	t.Cleanup(cancelB)

	peerB := startMemberlist(t, logger, idB, netip.MustParseAddrPort("127.0.0.1:17947"),
		[]netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:17946")})
	t.Cleanup(func() { shutDown(t, peerB) })

	caPathB := filepath.Join(dirB, "concord", "certs", "ca.crt")
	certPathB := filepath.Join(dirB, "concord", "certs", "node.crt")
	keyPathB := filepath.Join(dirB, "concord", "certs", "node.key")
	clientB, err := transport.NewClient(caPathB, certPathB, keyPathB)
	if err != nil {
		t.Fatalf("B client: %v", err)
	}

	go peersync.RunPullLoop(ctxB, logger, idB, peerB, clientB, jB, viewsB, eventsByIDB)

	time.Sleep(2 * time.Second) // let memberlist gossip propagate

	testEvent := journal.NewEvent(idA, "sync.test", json.RawMessage(`{}`))
	if err := journalview.RecordEvent(ctxA, jA, viewsA, testEvent); err != nil {
		t.Fatalf("record event: %v", err)
	}

	got := waitForEvent(t, eventsByIDB, testEvent.ID, 20*time.Second)
	if got.NodeID != idA {
		t.Fatalf("event node_id = %s, want %s", got.NodeID, idA)
	}
	if got.Type != "sync.test" {
		t.Fatalf("event type = %s, want sync.test", got.Type)
	}
	t.Log("event sync test PASSED: event synced from A to B via pull loop")
}

func provisionNodes(t *testing.T, idA, idB uuid.UUID, dirA, dirB string) {
	t.Helper()

	// Provision CA and node A certs in dirA
	t.Setenv("XDG_CONFIG_HOME", dirA)
	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write CA: %v", err)
	}
	if _, err := certs.Ensure(idA, netip.Addr{}); err != nil {
		t.Fatalf("ensure node A certs: %v", err)
	}

	// Copy CA to dirB so node B shares the same CA trust root
	certsDirB := filepath.Join(dirB, "concord", "certs")
	if err := os.MkdirAll(certsDirB, 0o700); err != nil {
		t.Fatalf("mkdir certs B: %v", err)
	}
	certsDirA := filepath.Join(dirA, "concord", "certs")
	copyFile(t, filepath.Join(certsDirA, "ca.crt"), filepath.Join(certsDirB, "ca.crt"))
	copyFile(t, filepath.Join(certsDirA, "ca.key"), filepath.Join(certsDirB, "ca.key"))

	// Ensure node B certs using the copied CA
	t.Setenv("XDG_CONFIG_HOME", dirB)
	if _, err := certs.Ensure(idB, netip.Addr{}); err != nil {
		t.Fatalf("ensure node B certs: %v", err)
	}
}

func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	// #nosec G304 - test helper with trusted temp dir paths.
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read %s: %v", src, err)
	}
	// #nosec G703 - test helper with trusted temp dir paths.
	if err := os.WriteFile(dst, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", dst, err)
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
	views := []journalview.View{
		eventsByID,
		journalview.NewEventsByNode(kv),
		journalview.NewEventsByType(kv),
	}
	jr, err := journalreader.OpenJSONLReaderPath(journalPath)
	if err != nil {
		t.Fatalf("open journal reader: %v", err)
	}
	defer jr.Close() //nolint:errcheck // best-effort in test
	for _, view := range views {
		if err := view.Rebuild(context.Background(), jr); err != nil {
			t.Fatalf("rebuild view: %v", err)
		}
	}
	return eventsByID, views
}

func startMemberlist(t *testing.T, logger *zap.Logger, id uuid.UUID, bind netip.AddrPort, join []netip.AddrPort) *peerdiscovery.MemberService {
	t.Helper()
	ms, err := peerdiscovery.Start(logger, peerdiscovery.Node{ID: id, Address: bind}, join, netip.Addr{})
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
	pollCtx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()
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
