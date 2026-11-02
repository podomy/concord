// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/blob"
	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/reconciler"
	"github.com/podomy/concord/internal/registry"
)

// fakeSyncer stands in for the blob syncer: Run blocks until its ctx is
// cancelled, so a closed runReturned proves the startup path cancelled.
type fakeSyncer struct {
	runReturned chan struct{}
}

// Run blocks until cancellation, mirroring the production syncer.
func (f *fakeSyncer) Run(ctx context.Context) {
	<-ctx.Done()
	close(f.runReturned)
}

// Ensure never misses in tests.
func (f *fakeSyncer) Ensure(_ context.Context, _ []string) error { return nil }

// AttachCursorStore is a no-op in tests.
func (f *fakeSyncer) AttachCursorStore(_ *kvstore.KVStore) {}

// A failing container runtime after a started registry must release
// both: no leaked listener (the port re-binds) and no leaked sync
// goroutine (Run returned).
func TestStartupFailureReleasesRegistryAndSyncer(t *testing.T) {
	svc, sampler := startPositionedService(t, 47.6, 8.9)

	lc := &net.ListenConfig{}
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr = %v, want TCP", ln.Addr())
	}
	port := addr.Port
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	oldPort := registry.Port
	registry.Port = port
	t.Cleanup(func() { registry.Port = oldPort })

	oldRuntime := newContainerRuntime
	newContainerRuntime = func() (*cr.ContainerRuntime, error) {
		return nil, errors.New("no runtime in test")
	}
	t.Cleanup(func() { newContainerRuntime = oldRuntime })

	fake := &fakeSyncer{runReturned: make(chan struct{})}
	oldSyncer := newBlobSyncer
	newBlobSyncer = func(_ *blob.Store, _ *peerdiscovery.MemberService, _ *logs.Logger) syncerIface {
		return fake
	}
	t.Cleanup(func() {
		newBlobSyncer = oldSyncer
		reconciler.SetBlobEnsurer(nil)
	})

	var bg sync.WaitGroup
	_, err = startWorkloadInfrastructure(context.Background(), uuid.New(), svc, logs.NewTest(t), nil, nil, nil, sampler, &bg)
	if err == nil {
		t.Fatal("expected container runtime failure, got nil")
	}

	select {
	case <-fake.runReturned:
	case <-time.After(5 * time.Second):
		t.Fatal("blob syncer goroutine leaked after startup failure")
	}

	probe, err := lc.Listen(context.Background(), "tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("registry listener leaked after startup failure: %v", err)
	}
	if err := probe.Close(); err != nil {
		t.Errorf("close probe listener: %v", err)
	}
}
