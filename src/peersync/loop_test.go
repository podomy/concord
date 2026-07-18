// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"errors"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/src/peerdiscovery"
	"github.com/podomy/concord/src/transport"
)

// --- helpers / port ---

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

// syncOne: port, watermark cursor, failure

// Gossip memberlist port must not be used for Sync; transport port is.
func TestSyncOneUsesTransportPortAndWatermark(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := peerdiscovery.Node{
		ID:      peerID,
		Address: mustAddrPort("192.0.2.10:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}
	watermarks := map[uuid.UUID]string{peerID: "mark-1"}
	fake := &fakeSyncer{
		resp: transport.SyncResponse{NextWatermark: "mark-2", Events: nil},
	}

	ok := syncOne(context.Background(), zap.NewNop(), fake, member, 8443, watermarks)
	if !ok {
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
		t.Fatalf("watermark = %q, want mark-1", call.req.Watermark)
	}
	if call.req.Limit != defaultSyncLimit {
		t.Fatalf("limit = %d, want %d", call.req.Limit, defaultSyncLimit)
	}
	if watermarks[peerID] != "mark-2" {
		t.Fatalf("watermark after = %q, want mark-2", watermarks[peerID])
	}
}

// Empty NextWatermark must not wipe an existing bookmark.
func TestSyncOneEmptyNextWatermarkDoesNotClear(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := peerdiscovery.Node{
		ID:      peerID,
		Address: mustAddrPort("192.0.2.10:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}
	watermarks := map[uuid.UUID]string{peerID: "keep-me"}
	fake := &fakeSyncer{
		resp: transport.SyncResponse{NextWatermark: ""},
	}

	if !syncOne(context.Background(), zap.NewNop(), fake, member, 8443, watermarks) {
		t.Fatal("expected success")
	}
	if watermarks[peerID] != "keep-me" {
		t.Fatalf("watermark = %q, want keep-me", watermarks[peerID])
	}
}

// Failed Sync leaves the cursor so the next attempt retries the same page.
func TestSyncOneFailureDoesNotAdvanceWatermark(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := peerdiscovery.Node{
		ID:      peerID,
		Address: mustAddrPort("192.0.2.10:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}
	watermarks := map[uuid.UUID]string{peerID: "old"}
	fake := &fakeSyncer{err: errors.New("dial failed")}

	if syncOne(context.Background(), zap.NewNop(), fake, member, 8443, watermarks) {
		t.Fatal("expected failure")
	}
	if watermarks[peerID] != "old" {
		t.Fatalf("watermark = %q, want old", watermarks[peerID])
	}
}

// First contact: missing map key → watermark "" (from start of peer journal).
func TestSyncOneMissingWatermarkSendsEmpty(t *testing.T) {
	t.Parallel()

	peerID := uuid.New()
	member := peerdiscovery.Node{
		ID:      peerID,
		Address: mustAddrPort("192.0.2.10:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}
	watermarks := map[uuid.UUID]string{}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "first"}}

	syncOne(context.Background(), zap.NewNop(), fake, member, 8443, watermarks)
	if fake.calls[0].req.Watermark != "" {
		t.Fatalf("first pull watermark = %q, want empty", fake.calls[0].req.Watermark)
	}
	if watermarks[peerID] != "first" {
		t.Fatalf("stored = %q, want first", watermarks[peerID])
	}
}

// --- pullTick: strategy C, membership edges ---

// Meet tick must Sync once only (not meet + periodic double pull).
func TestPullTickMeetThenPeriodicNoDoubleSync(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	peer := peerdiscovery.Node{
		ID:      peerID,
		Address: mustAddrPort("198.51.100.1:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}
	src := &fakeMembers{list: []peerdiscovery.Node{
		{ID: self, Address: mustAddrPort("127.0.0.1:7946"), State: peerdiscovery.NodeStateAlive},
		peer,
	}}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "w1"}}
	watermarks := map[uuid.UUID]string{}

	previous := pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, nil, watermarks)
	if len(fake.calls) != 1 {
		t.Fatalf("meet tick calls = %d, want 1 (no double sync)", len(fake.calls))
	}

	// Still alive: periodic only.
	fake.calls = nil
	_ = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, previous, watermarks)
	if len(fake.calls) != 1 {
		t.Fatalf("periodic tick calls = %d, want 1", len(fake.calls))
	}
	if watermarks[peerID] != "w1" {
		t.Fatalf("watermark = %q", watermarks[peerID])
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

	_ = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, nil, map[uuid.UUID]string{})
	if len(fake.calls) != 0 {
		t.Fatalf("calls = %d, want 0", len(fake.calls))
	}
}

// Peer returns to alive: meet pull, keep watermark from before they died.
func TestPullTickDeadToAliveIsMeet(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	addr := mustAddrPort("203.0.113.5:7946")
	previous := map[uuid.UUID]peerdiscovery.Node{
		peerID: {ID: peerID, Address: addr, State: peerdiscovery.NodeStateDead},
	}
	src := &fakeMembers{list: []peerdiscovery.Node{
		{ID: peerID, Address: addr, State: peerdiscovery.NodeStateAlive},
	}}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "back"}}
	watermarks := map[uuid.UUID]string{peerID: "before-death"}

	_ = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, previous, watermarks)
	if len(fake.calls) != 1 {
		t.Fatalf("calls = %d, want 1 meet pull", len(fake.calls))
	}
	if fake.calls[0].req.Watermark != "before-death" {
		t.Fatalf("should resume watermark after rejoin, got %q", fake.calls[0].req.Watermark)
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

	got := pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, previous, map[uuid.UUID]string{})
	if len(fake.calls) != 0 {
		t.Fatal("should not sync on members error")
	}
	if len(got) != 1 || got[peerID].ID != peerID {
		t.Fatalf("should keep previous snapshot, got %#v", got)
	}
}

// --- paging and multi-peer watermarks ---

// Each successful page advances the cursor; next tick sends the prior NextWatermark.
func TestPullTickPagingWatermarksAcrossTicks(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{{
		ID:      peerID,
		Address: mustAddrPort("192.0.2.1:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}}}
	fake := &fakeSyncer{responses: []transport.SyncResponse{
		{NextWatermark: "page1"},
		{NextWatermark: "page2"},
		{NextWatermark: "page3"},
	}}
	watermarks := map[uuid.UUID]string{}

	prev := pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, nil, watermarks)
	if watermarks[peerID] != "page1" {
		t.Fatalf("after tick1: %q", watermarks[peerID])
	}
	prev = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, prev, watermarks)
	if fake.calls[1].req.Watermark != "page1" {
		t.Fatalf("tick2 sent watermark %q, want page1", fake.calls[1].req.Watermark)
	}
	if watermarks[peerID] != "page2" {
		t.Fatalf("after tick2: %q", watermarks[peerID])
	}
	_ = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, prev, watermarks)
	if fake.calls[2].req.Watermark != "page2" {
		t.Fatalf("tick3 sent watermark %q, want page2", fake.calls[2].req.Watermark)
	}
	if watermarks[peerID] != "page3" {
		t.Fatalf("after tick3: %q", watermarks[peerID])
	}
}

// Each peer has its own cursor; they must not share one watermark.
func TestPullTickPerPeerWatermarksIndependent(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	a := uuid.New()
	b := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{
		{ID: a, Address: mustAddrPort("10.0.0.1:7946"), State: peerdiscovery.NodeStateAlive},
		{ID: b, Address: mustAddrPort("10.0.0.2:7946"), State: peerdiscovery.NodeStateAlive},
	}}
	fake := &fakeSyncer{
		byPeer: map[string]transport.SyncResponse{
			"10.0.0.1:8443": {NextWatermark: "wa"},
			"10.0.0.2:8443": {NextWatermark: "wb"},
		},
	}
	watermarks := map[uuid.UUID]string{}

	_ = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, nil, watermarks)
	if watermarks[a] != "wa" || watermarks[b] != "wb" {
		t.Fatalf("watermarks = %v", watermarks)
	}
}

// peer/server down then back; process restart

// Peer unreachable mid-sync: cursor stays; when peer returns, resume same watermark
// (no skip ahead, no forced full reset while the process stays up).
func TestPullTickPeerDownThenUpResumesWatermark(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{{
		ID:      peerID,
		Address: mustAddrPort("192.0.2.50:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}}}
	fake := &fakeSyncer{responses: []transport.SyncResponse{
		{NextWatermark: "got-to-here"},
	}}
	watermarks := map[uuid.UUID]string{}

	// Successful page while peer is up.
	prev := pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, nil, watermarks)
	if watermarks[peerID] != "got-to-here" {
		t.Fatalf("watermark after success: %q", watermarks[peerID])
	}

	// Peer/server down: Sync fails, watermark must not move.
	fake.err = errors.New("connection refused")
	fake.calls = nil
	prev = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, prev, watermarks)
	if len(fake.calls) != 1 {
		t.Fatalf("expected a failed sync attempt, calls=%d", len(fake.calls))
	}
	if watermarks[peerID] != "got-to-here" {
		t.Fatalf("watermark after failure: %q, want got-to-here", watermarks[peerID])
	}

	// Peer back: next pull must send the same cursor (resume, not from scratch).
	fake.err = nil
	fake.resp = transport.SyncResponse{NextWatermark: "after-recovery"}
	fake.calls = nil
	_ = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, prev, watermarks)
	if len(fake.calls) != 1 {
		t.Fatalf("calls after recovery = %d", len(fake.calls))
	}
	if fake.calls[0].req.Watermark != "got-to-here" {
		t.Fatalf("resume watermark = %q, want got-to-here", fake.calls[0].req.Watermark)
	}
	if watermarks[peerID] != "after-recovery" {
		t.Fatalf("watermark after recovery: %q", watermarks[peerID])
	}
}

// Process restart: watermarks are in-memory only. A new empty map means the
// next pull sends watermark "" (full resync from peer start). Preventing
// duplicate journal rows is the apply layer's job (event id idempotency),
// not the pull loop's.
func TestPullTickProcessRestartResyncsFromEmptyWatermark(t *testing.T) {
	t.Parallel()

	self := uuid.New()
	peerID := uuid.New()
	src := &fakeMembers{list: []peerdiscovery.Node{{
		ID:      peerID,
		Address: mustAddrPort("192.0.2.60:7946"),
		State:   peerdiscovery.NodeStateAlive,
	}}}
	fake := &fakeSyncer{resp: transport.SyncResponse{NextWatermark: "progress"}}

	// "Old process" had advanced the cursor.
	oldWatermarks := map[uuid.UUID]string{}
	_ = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, nil, oldWatermarks)
	if oldWatermarks[peerID] != "progress" {
		t.Fatalf("old process watermark: %q", oldWatermarks[peerID])
	}

	// "New process" after restart: fresh watermarks map (RunPullLoop starts empty).
	fake.calls = nil
	fake.resp = transport.SyncResponse{NextWatermark: "progress-again"}
	newWatermarks := map[uuid.UUID]string{}
	_ = pullTick(context.Background(), zap.NewNop(), self, src, fake, 8443, nil, newWatermarks)

	if len(fake.calls) != 1 {
		t.Fatalf("calls = %d", len(fake.calls))
	}
	if fake.calls[0].req.Watermark != "" {
		t.Fatalf("after restart sent watermark %q, want empty (full resync)", fake.calls[0].req.Watermark)
	}
	if newWatermarks[peerID] != "progress-again" {
		t.Fatalf("new process watermark: %q", newWatermarks[peerID])
	}
}

func TestRunPullLoopStopsOnCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	src := &fakeMembers{list: nil}
	fake := &fakeSyncer{}

	done := make(chan struct{})
	go func() {
		RunPullLoop(ctx, zap.NewNop(), uuid.New(), src, fake)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunPullLoop did not stop")
	}
}

// fakes

type syncCall struct {
	peer netip.AddrPort
	req  transport.SyncRequest
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

func (f *fakeSyncer) Sync(_ context.Context, peer netip.AddrPort, req transport.SyncRequest) (transport.SyncResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, syncCall{peer: peer, req: req})
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
