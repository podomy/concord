// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/transport"
)

func mustEvent() journal.Event {
	return journal.NewEvent(uuid.New(), "test.event", json.RawMessage(`{}`))
}

func dummyNode(id uuid.UUID) peerdiscovery.Node {
	return keyedNode(id, "192.0.2.10:7946")
}

// testNoiseKey is a fixed 32-byte public key so loop unit tests exercise the
// sync path. Signature checking happens in transport, not here.
var testNoiseKey = bytes.Repeat([]byte{0x07}, 32)

// keyedNode builds an alive peer advertising a Noise static key.
func keyedNode(id uuid.UUID, addr string) peerdiscovery.Node {
	return peerdiscovery.Node{
		ID:      id,
		Address: mustAddrPort(addr),
		State:   peerdiscovery.NodeStateAlive,
		Metadata: peerdiscovery.NodeMetadata{
			NoisePublicKey: testNoiseKey,
		},
	}
}

// testPullState builds pullState with in-memory journal/index for loop unit tests.
func testPullState(syncer PeerSync, cursors map[uuid.UUID]string) pullState {
	j := &memJournal{}
	return pullState{
		syncer:  syncer,
		journal: j,
		views:   nil,
		byID:    &journalIndex{j: j},
		port:    8443,
		cursors: cursors,
	}
}

func TestParseTransportPort(t *testing.T) {
	t.Parallel()

	if got := parseTransportPort("8443"); got != 8443 {
		t.Fatalf("got %d, want 8443", got)
	}
	if got := parseTransportPort("not-a-port"); got != 8443 {
		t.Fatalf("invalid port fallback: got %d", got)
	}
	if got := parseTransportPort("0"); got != 8443 {
		t.Fatalf("zero port fallback: got %d", got)
	}
	if got := parseTransportPort("9000"); got != 9000 {
		t.Fatalf("got %d, want 9000", got)
	}
}

// membersExceptSelf must never Sync the local node.
func TestMembersExceptSelf(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	other := uuid.New()
	members := []peerdiscovery.Node{
		{ID: self, Address: mustAddrPort("10.0.0.1:7946"), State: peerdiscovery.NodeStateAlive},
		{ID: other, Address: mustAddrPort("10.0.0.2:7946"), State: peerdiscovery.NodeStateAlive},
	}

	got := membersExceptSelf(self, members)
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if _, ok := got[other]; !ok {
		t.Fatal("missing other peer")
	}
	if _, ok := got[self]; ok {
		t.Fatal("self should be excluded")
	}
}

// becameAlive is the meet edge used by strategy C.
func TestBecameAlive(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	alive := peerdiscovery.Node{ID: id, State: peerdiscovery.NodeStateAlive}
	dead := peerdiscovery.Node{ID: id, State: peerdiscovery.NodeStateDead}
	suspect := peerdiscovery.Node{ID: id, State: peerdiscovery.NodeStateSuspect}

	if !becameAlive(nil, id, alive) {
		t.Fatal("first sighting alive should be meet")
	}
	if becameAlive(nil, id, dead) {
		t.Fatal("first sighting dead is not meet")
	}
	if !becameAlive(map[uuid.UUID]peerdiscovery.Node{id: dead}, id, alive) {
		t.Fatal("dead → alive should be meet")
	}
	if !becameAlive(map[uuid.UUID]peerdiscovery.Node{id: suspect}, id, alive) {
		t.Fatal("suspect → alive should be meet")
	}
	if becameAlive(map[uuid.UUID]peerdiscovery.Node{id: alive}, id, alive) {
		t.Fatal("still alive is not a new meet")
	}
	if becameAlive(map[uuid.UUID]peerdiscovery.Node{id: alive}, id, dead) {
		t.Fatal("alive → dead is not meet")
	}
}

// syncOne: port, cursor, failure.

// Gossip memberlist port must not be used for Sync; transport port is.
func TestSyncOneUsesTransportPortAndCursor(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := dummyNode(peerID)
	cursors := map[uuid.UUID]string{peerID: "mark-1"}
	fake := &fakeSyncer{
		resp: transport.SyncResponse{NextWatermark: "mark-2", Events: nil},
	}

	if !syncOne(context.Background(), zap.NewNop(), testPullState(fake, cursors), member) {
		t.Fatal("expected success")
	}
	if len(fake.calls) != 1 {
		t.Fatalf("calls = %d, want 1", len(fake.calls))
	}
	call := fake.calls[0]
	if call.peer.String() != "192.0.2.10:8443" {
		t.Fatalf("peer addr = %s, want 192.0.2.10:8443 (transport port, not gossip)", call.peer)
	}
	if call.req.Watermark != "mark-1" {
		t.Fatalf("cursor = %q, want mark-1", call.req.Watermark)
	}
	if call.req.Limit != defaultSyncLimit {
		t.Fatalf("limit = %d, want %d", call.req.Limit, defaultSyncLimit)
	}
	if cursors[peerID] != "mark-2" {
		t.Fatalf("cursor after = %q, want mark-2", cursors[peerID])
	}
}

// Empty next cursor must not wipe an existing bookmark.
func TestSyncOneEmptyNextCursorDoesNotClear(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := dummyNode(peerID)
	cursors := map[uuid.UUID]string{peerID: "keep-me"}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: ""}}

	if !syncOne(context.Background(), zap.NewNop(), testPullState(fake, cursors), member) {
		t.Fatal("expected success")
	}
	if cursors[peerID] != "keep-me" {
		t.Fatalf("cursor = %q, want keep-me", cursors[peerID])
	}
}

// Peers without an advertised Noise key are skipped, never dialed.
func TestSyncOneSkipsPeerWithoutNoiseKey(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := peerdiscovery.Node{
		ID:      peerID,
		Address: mustAddrPort("192.0.2.10:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "w"}}

	if syncOne(context.Background(), zap.NewNop(), testPullState(fake, map[uuid.UUID]string{}), member) {
		t.Fatal("expected skip, got success")
	}
	if len(fake.calls) != 0 {
		t.Fatalf("calls = %d, want 0 (no dial without key)", len(fake.calls))
	}
}

// Failed Sync leaves the cursor so the next attempt retries the same page.
func TestSyncOneFailureDoesNotAdvanceCursor(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := dummyNode(peerID)
	cursors := map[uuid.UUID]string{peerID: "old"}
	fake := &fakeSyncer{err: errors.New("dial failed")}

	if syncOne(context.Background(), zap.NewNop(), testPullState(fake, cursors), member) {
		t.Fatal("expected failure")
	}
	if cursors[peerID] != "old" {
		t.Fatalf("cursor = %q, want old", cursors[peerID])
	}
}

// First contact: missing map key → cursor "" (from start of peer journal).
func TestSyncOneMissingCursorSendsEmpty(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := dummyNode(peerID)
	cursors := map[uuid.UUID]string{}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "first"}}

	syncOne(context.Background(), zap.NewNop(), testPullState(fake, cursors), member)
	if fake.calls[0].req.Watermark != "" {
		t.Fatalf("first pull watermark = %q, want empty", fake.calls[0].req.Watermark)
	}
	if cursors[peerID] != "first" {
		t.Fatalf("stored = %q, want first", cursors[peerID])
	}
}

// Meet tick must Sync once only (not meet + periodic double pull).
func TestPullTickMeetThenPeriodicNoDoubleSync(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	peer := keyedNode(peerID, "198.51.100.1:7946")
	src := &fakeMembers{list: []peerdiscovery.Node{
		{ID: self, Address: mustAddrPort("127.0.0.1:7946"), State: peerdiscovery.NodeStateAlive},
		peer,
	}}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "w1"}}
	cursors := map[uuid.UUID]string{}

	previous := pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), nil)
	if len(fake.calls) != 1 {
		t.Fatalf("meet tick calls = %d, want 1 (no double sync)", len(fake.calls))
	}

	// Still alive: periodic only.
	fake.calls = nil
	_ = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), previous)
	if len(fake.calls) != 1 {
		t.Fatalf("periodic tick calls = %d, want 1", len(fake.calls))
	}
	if cursors[peerID] != "w1" {
		t.Fatalf("cursor = %q", cursors[peerID])
	}
}

func TestPullTickSkipsDeadAndSelf(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	deadID := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{
		{ID: self, Address: mustAddrPort("10.0.0.1:7946"), State: peerdiscovery.NodeStateAlive},
		{ID: deadID, Address: mustAddrPort("10.0.0.2:7946"), State: peerdiscovery.NodeStateDead},
	}}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "x"}}

	_ = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, map[uuid.UUID]string{}), nil)
	if len(fake.calls) != 0 {
		t.Fatalf("calls = %d, want 0", len(fake.calls))
	}
}

// Peer returns to alive: meet pull, keep cursor from before they died.
func TestPullTickDeadToAliveIsMeet(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	addr := mustAddrPort("203.0.113.5:7946")
	previous := map[uuid.UUID]peerdiscovery.Node{
		peerID: {ID: peerID, Address: addr, State: peerdiscovery.NodeStateDead},
	}
	src := &fakeMembers{list: []peerdiscovery.Node{
		keyedNode(peerID, "203.0.113.5:7946"),
	}}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "back"}}
	cursors := map[uuid.UUID]string{peerID: "before-death"}

	_ = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), previous)
	if len(fake.calls) != 1 {
		t.Fatalf("calls = %d, want 1 meet pull", len(fake.calls))
	}
	if fake.calls[0].req.Watermark != "before-death" {
		t.Fatalf("should resume cursor after rejoin, got %q", fake.calls[0].req.Watermark)
	}
}

func TestPullTickMembersErrorKeepsPrevious(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	previous := map[uuid.UUID]peerdiscovery.Node{
		peerID: {ID: peerID, State: peerdiscovery.NodeStateAlive},
	}
	src := &fakeMembers{err: errors.New("memberlist down")}
	fake := &fakeSyncer{}

	got := pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, map[uuid.UUID]string{}), previous)
	if len(fake.calls) != 0 {
		t.Fatal("should not sync on members error")
	}
	if len(got) != 1 || got[peerID].ID != peerID {
		t.Fatalf("should keep previous snapshot, got %#v", got)
	}
}

// Each successful page advances the cursor; next tick sends the prior NextWatermark.
func TestPullTickPagingCursorsAcrossTicks(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{
		keyedNode(peerID, "192.0.2.1:7946"),
	}}
	fake := &fakeSyncer{responses: []transport.SyncResponse{
		{NextWatermark: "page1"},
		{NextWatermark: "page2"},
		{NextWatermark: "page3"},
	}}
	cursors := map[uuid.UUID]string{}

	prev := pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), nil)
	if cursors[peerID] != "page1" {
		t.Fatalf("after tick1: %q", cursors[peerID])
	}
	prev = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), prev)
	if fake.calls[1].req.Watermark != "page1" {
		t.Fatalf("tick2 sent cursor %q, want page1", fake.calls[1].req.Watermark)
	}
	if cursors[peerID] != "page2" {
		t.Fatalf("after tick2: %q", cursors[peerID])
	}
	_ = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), prev)
	if fake.calls[2].req.Watermark != "page2" {
		t.Fatalf("tick3 sent cursor %q, want page2", fake.calls[2].req.Watermark)
	}
	if cursors[peerID] != "page3" {
		t.Fatalf("after tick3: %q", cursors[peerID])
	}
}

// Each peer has its own cursor; they must not share.
func TestPullTickPerPeerCursorsIndependent(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	a := uuid.New()
	b := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{
		keyedNode(a, "10.0.0.1:7946"),
		keyedNode(b, "10.0.0.2:7946"),
	}}
	fake := &fakeSyncer{
		byPeer: map[string]transport.SyncResponse{
			"10.0.0.1:8443": {NextWatermark: "wa"},
			"10.0.0.2:8443": {NextWatermark: "wb"},
		},
	}
	cursors := map[uuid.UUID]string{}

	_ = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), nil)
	if cursors[a] != "wa" || cursors[b] != "wb" {
		t.Fatalf("cursors = %v", cursors)
	}
}

// Peer unreachable mid-sync: cursor stays; when peer returns, resume same cursor
// (no skip ahead, no forced full reset while the process stays up).
func TestPullTickPeerDownThenUpResumesCursor(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{
		keyedNode(peerID, "192.0.2.50:7946"),
	}}
	fake := &fakeSyncer{responses: []transport.SyncResponse{
		{NextWatermark: "got-to-here"},
	}}
	cursors := map[uuid.UUID]string{}

	// Successful page while peer is up.
	prev := pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), nil)
	if cursors[peerID] != "got-to-here" {
		t.Fatalf("cursor after success: %q", cursors[peerID])
	}

	// Peer/server down: Sync fails, cursor must not move.
	fake.err = errors.New("connection refused")
	fake.calls = nil
	prev = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), prev)
	if len(fake.calls) != 1 {
		t.Fatalf("expected a failed sync attempt, calls=%d", len(fake.calls))
	}
	if cursors[peerID] != "got-to-here" {
		t.Fatalf("cursor after failure: %q, want got-to-here", cursors[peerID])
	}

	// Peer back: next pull must send the same cursor (resume, not from scratch).
	fake.err = nil
	fake.resp = transport.SyncResponse{NextWatermark: "after-recovery"}
	fake.calls = nil
	_ = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, cursors), prev)
	if len(fake.calls) != 1 {
		t.Fatalf("calls after recovery = %d", len(fake.calls))
	}
	if fake.calls[0].req.Watermark != "got-to-here" {
		t.Fatalf("resume cursor = %q, want got-to-here", fake.calls[0].req.Watermark)
	}
	if cursors[peerID] != "after-recovery" {
		t.Fatalf("cursor after recovery: %q", cursors[peerID])
	}
}

// Process restart: cursors are in-memory only. A new empty set means the
// next pull sends cursor "" (full resync from peer start). Preventing
// duplicate journal rows is the apply layer's job (event id idempotency),
// not the pull loop's.
func TestPullTickProcessRestartResyncsFromEmptyCursor(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{
		keyedNode(peerID, "192.0.2.60:7946"),
	}}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "progress"}}

	// "Old process" had advanced the cursor.
	oldCursors := map[uuid.UUID]string{}
	_ = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, oldCursors), nil)
	if oldCursors[peerID] != "progress" {
		t.Fatalf("old process cursor: %q", oldCursors[peerID])
	}

	// "New process" after restart: fresh cursor set (RunPullLoop starts empty).
	fake.calls = nil
	fake.resp = transport.SyncResponse{NextWatermark: "progress-again"}
	newCursors := map[uuid.UUID]string{}
	_ = pullTick(context.Background(), zap.NewNop(), self, src, testPullState(fake, newCursors), nil)

	if len(fake.calls) != 1 {
		t.Fatalf("calls = %d", len(fake.calls))
	}
	if fake.calls[0].req.Watermark != "" {
		t.Fatalf("after restart sent watermark %q, want empty (full resync)", fake.calls[0].req.Watermark)
	}
	if newCursors[peerID] != "progress-again" {
		t.Fatalf("new process cursor: %q", newCursors[peerID])
	}
}

// After Sync, events are applied; cursor advances only if apply succeeds.
func TestSyncOneAppliesEventsBeforeCursor(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := keyedNode(peerID, "192.0.2.10:7946")
	ev := mustEvent()
	fake := &fakeSyncer{
		resp: transport.SyncResponse{
			NextWatermark: ev.ID.String(),
			Events:        []journal.Event{ev},
		},
	}
	cursors := map[uuid.UUID]string{}
	j := &memJournal{}
	state := pullState{
		syncer:  fake,
		journal: j,
		byID:    &journalIndex{j: j},
		port:    8443,
		cursors: cursors,
	}

	if !syncOne(context.Background(), zap.NewNop(), state, member) {
		t.Fatal("expected success")
	}
	if len(j.events) != 1 || j.events[0].ID != ev.ID {
		t.Fatalf("applied events = %v", j.events)
	}
	if cursors[peerID] != ev.ID.String() {
		t.Fatalf("cursor = %q", cursors[peerID])
	}

	// Replay same page: idempotent, still one row.
	if !syncOne(context.Background(), zap.NewNop(), state, member) {
		t.Fatal("expected success on replay")
	}
	if len(j.events) != 1 {
		t.Fatalf("idempotent apply failed, len=%d", len(j.events))
	}
}

func TestRunPullLoopStopsOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	src := &fakeMembers{list: nil}
	fake := &fakeSyncer{}

	done := make(chan struct{})
	go func() {
		j := &memJournal{}
		RunPullLoop(ctx, zap.NewNop(), uuid.New(), src, fake, j, nil, &journalIndex{j: j})
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunPullLoop did not stop")
	}
}

type syncCall struct {
	peer   netip.AddrPort
	expect transport.Peer
	req    transport.SyncRequest
}

// fakeSyncer records Sync calls and returns scripted responses.
type fakeSyncer struct {
	err       error
	calls     []syncCall
	responses []transport.SyncResponse
	byPeer    map[string]transport.SyncResponse
	resp      transport.SyncResponse
	mu        sync.Mutex
}

func (f *fakeSyncer) Sync(_ context.Context, peer netip.AddrPort, expect transport.Peer, req transport.SyncRequest) (transport.SyncResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, syncCall{peer: peer, expect: expect, req: req})
	if f.err != nil {
		return transport.SyncResponse{}, f.err
	}
	if f.byPeer != nil {
		if r, ok := f.byPeer[peer.String()]; ok {
			return r, nil
		}
	}
	if len(f.responses) > 0 {
		r := f.responses[0]
		f.responses = f.responses[1:]
		return r, nil
	}
	return f.resp, nil
}

type fakeMembers struct {
	err  error
	list []peerdiscovery.Node
}

func (f *fakeMembers) Members() ([]peerdiscovery.Node, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]peerdiscovery.Node, len(f.list))
	copy(out, f.list)
	return out, nil
}

func mustAddrPort(s string) netip.AddrPort {
	return netip.MustParseAddrPort(s)
}
