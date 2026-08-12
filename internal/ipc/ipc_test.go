// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ipc_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/ipc"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/sdk"
)

type testHarness struct {
	server     *ipc.Server
	client     sdk.Client
	socketPath string
	kv         *kvstore.KVStore
	journal    *journal.JSONL
	workloads  *journalview.Workloads
}

func setupTestServer(t *testing.T) *testHarness {
	t.Helper()

	tempDir := t.TempDir()
	socketPath := filepath.Join(tempDir, "concord.sock")
	dbPath := filepath.Join(tempDir, "test.db")
	journalPath := filepath.Join(tempDir, "journal.jsonl")

	kv, err := kvstore.OpenDBPath(dbPath)
	if err != nil {
		t.Fatalf("open kv store: %v", err)
	}

	j, err := journal.OpenJSONLPath(journalPath)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}

	workloads := journalview.NewWorkloads(kv)
	views := []journalview.View{workloads}

	nodeID := uuid.New()
	server := ipc.NewServer(nodeID, j, views, workloads, nil, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())

	err = server.Start(ctx, socketPath)
	if err != nil {
		t.Fatalf("start ipc server: %v", err)
	}

	client, err := sdk.Dial(socketPath)
	if err != nil {
		t.Fatalf("dial ipc server: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		if err := client.Close(); err != nil {
			t.Logf("cleanup client close error: %v", err)
		}
		if err := server.Shutdown(context.Background()); err != nil {
			t.Logf("cleanup server shutdown error: %v", err)
		}
		if err := j.Close(); err != nil {
			t.Logf("cleanup journal close error: %v", err)
		}
		if err := kv.Close(); err != nil {
			t.Logf("cleanup kv close error: %v", err)
		}
	})

	return &testHarness{
		server:     server,
		client:     client,
		socketPath: socketPath,
		kv:         kv,
		journal:    j,
		workloads:  workloads,
	}
}

func mustBuild(t *testing.T, b *sdk.Builder) sdk.Workload {
	t.Helper()
	w, err := b.Build()
	if err != nil {
		t.Fatalf("build workload: %v", err)
	}
	return w
}

func mustSubmit(t *testing.T, client sdk.Client, ctx context.Context, w sdk.Workload) uuid.UUID {
	t.Helper()
	id, err := client.Submit(ctx, w)
	if err != nil {
		t.Fatalf("submit workload: %v", err)
	}
	return id
}

func TestIPCSubmitAndGet(t *testing.T) {
	t.Parallel()

	h := setupTestServer(t)
	ctx := context.Background()

	w := mustBuild(t, sdk.NewWorkload().
		Image("docker.io/library/nginx:alpine").
		Port(8080, 80).
		Env("PORT", "80").
		Restart(sdk.RestartAlways).
		HealthCheck("/healthz", sdk.HealthActionRestart))

	id := mustSubmit(t, h.client, ctx, w)

	got, err := h.client.Get(ctx, id)
	if err != nil {
		t.Fatalf("get workload: %v", err)
	}

	diff := cmp.Diff(id, got.ID)
	if diff != "" {
		t.Fatalf("workload ID mismatch (-want +got):\n%s", diff)
	}
}

func TestIPCList(t *testing.T) {
	t.Parallel()

	h := setupTestServer(t)
	ctx := context.Background()

	w1 := mustBuild(t, sdk.NewWorkload().Image("app1:latest").Port(80, 80))
	w2 := mustBuild(t, sdk.NewWorkload().Image("app2:latest").Port(81, 81))

	mustSubmit(t, h.client, ctx, w1)
	mustSubmit(t, h.client, ctx, w2)

	list, err := h.client.List(ctx)
	if err != nil {
		t.Fatalf("list workloads: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("expected 2 workloads, got %d", len(list))
	}
}

func TestIPCStop(t *testing.T) {
	t.Parallel()

	h := setupTestServer(t)
	ctx := context.Background()

	w := mustBuild(t, sdk.NewWorkload().Image("app:latest").Port(80, 80))
	id := mustSubmit(t, h.client, ctx, w)

	if err := h.client.Stop(ctx, id); err != nil {
		t.Fatalf("stop workload: %v", err)
	}

	_, err := h.client.Get(ctx, id)
	if !errors.Is(err, sdk.ErrNotFound) {
		t.Fatalf("expected ErrNotFound after stop, got %v", err)
	}
}

func TestIPCNodesEmpty(t *testing.T) {
	t.Parallel()

	h := setupTestServer(t)
	ctx := context.Background()

	nodes, err := h.client.Nodes(ctx)
	if err != nil {
		t.Fatalf("query nodes: %v", err)
	}
	if len(nodes) != 0 {
		t.Fatalf("expected empty nodes, got %d", len(nodes))
	}
}

func TestIPCValidationErrors(t *testing.T) {
	t.Parallel()

	h := setupTestServer(t)
	ctx := context.Background()

	_, err := h.client.Submit(ctx, sdk.Workload{})
	if err == nil {
		t.Fatalf("expected error submitting empty workload")
	}

	_, err = h.client.Get(ctx, uuid.New())
	if !errors.Is(err, sdk.ErrNotFound) {
		t.Fatalf("expected ErrNotFound for missing workload, got %v", err)
	}
}

func TestIPCExplicitWorkloadID(t *testing.T) {
	t.Parallel()

	h := setupTestServer(t)
	ctx := context.Background()

	customID := uuid.New()
	w := mustBuild(t, sdk.NewWorkload().
		Image("docker.io/library/alpine:latest").
		Command("/bin/sh", "-c", "echo hello"))
	w.ID = customID

	submittedID := mustSubmit(t, h.client, ctx, w)
	if submittedID != customID {
		t.Fatalf("expected submitted ID %s, got %s", customID, submittedID)
	}

	got, err := h.client.Get(ctx, customID)
	if err != nil {
		t.Fatalf("get custom ID workload: %v", err)
	}
	if got.ID != customID {
		t.Fatalf("expected got ID %s, got %s", customID, got.ID)
	}
}

func TestIPCServerDoubleStart(t *testing.T) {
	t.Parallel()

	h := setupTestServer(t)

	err := h.server.Start(context.Background(), h.socketPath)
	if err == nil {
		t.Fatalf("expected error on double start")
	}
}

func TestIPCServerGracefulShutdown(t *testing.T) {
	t.Parallel()

	tempDir := t.TempDir()
	socketPath := filepath.Join(tempDir, "shutdown.sock")
	dbPath := filepath.Join(tempDir, "test.db")
	journalPath := filepath.Join(tempDir, "journal.jsonl")

	kv, err := kvstore.OpenDBPath(dbPath)
	if err != nil {
		t.Fatalf("open kv store: %v", err)
	}
	defer func() {
		if err := kv.Close(); err != nil {
			t.Logf("close kv error: %v", err)
		}
	}()

	j, err := journal.OpenJSONLPath(journalPath)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	defer func() {
		if err := j.Close(); err != nil {
			t.Logf("close journal error: %v", err)
		}
	}()

	workloads := journalview.NewWorkloads(kv)
	views := []journalview.View{workloads}

	server := ipc.NewServer(uuid.New(), j, views, workloads, nil, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	if err := server.Start(ctx, socketPath); err != nil {
		t.Fatalf("start server: %v", err)
	}

	cancel()
	time.Sleep(50 * time.Millisecond)

	if err := server.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown already closed server: %v", err)
	}
}
