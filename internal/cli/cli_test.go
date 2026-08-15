// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/cli"
	"github.com/podomy/concord/internal/ipc"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
)

func setupCLITest(t *testing.T) {
	t.Helper()

	tempDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tempDir)

	socketDir := filepath.Join(tempDir, "concord")
	if err := os.MkdirAll(socketDir, 0o750); err != nil {
		t.Fatalf("create socket dir: %v", err)
	}

	socketPath := filepath.Join(socketDir, "concord.sock")
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

	if err := server.Start(ctx, socketPath); err != nil {
		t.Fatalf("start ipc server: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		if err := server.Shutdown(context.Background()); err != nil {
			t.Logf("shutdown server: %v", err)
		}
		if err := j.Close(); err != nil {
			t.Logf("close journal: %v", err)
		}
		if err := kv.Close(); err != nil {
			t.Logf("close kv: %v", err)
		}
	})
}

func TestCLIMainHelp(t *testing.T) {
	t.Parallel()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(context.Background(), []string{"--help"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("run help: %v", err)
	}

	out := stdout.String()
	if !strings.Contains(out, "coordination layer for distributed systems") || !strings.Contains(out, "Available Commands") {
		t.Fatalf("expected main usage in stdout, got:\n%s", out)
	}
}

func testSubmitWorkload(t *testing.T, ctx context.Context) string {
	t.Helper()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{
		"workload", "run",
		"-p", "8080:80",
		"-e", "ENV=production",
		"--restart", "always",
		"--health-path", "/healthz",
		"docker.io/library/nginx:alpine",
	}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("workload run failed: %v, stderr: %s", err, stderr.String())
	}

	runOut := stdout.String()
	fields := strings.Fields(runOut)
	if len(fields) < 3 {
		t.Fatalf("expected workload UUID in output: %s", runOut)
	}

	return fields[2]
}

func testVerifyList(t *testing.T, ctx context.Context, shortID string) {
	t.Helper()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{"workload", "list"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("workload list failed: %v", err)
	}

	listOut := stdout.String()
	if !strings.Contains(listOut, shortID) || !strings.Contains(listOut, "8080:80") {
		t.Fatalf("workload not found in list output:\n%s", listOut)
	}
}

func testVerifyInspect(t *testing.T, ctx context.Context, shortID, fullID string) {
	t.Helper()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{"workload", "inspect", shortID}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("workload inspect failed: %v", err)
	}

	inspectOut := stdout.String()
	if !strings.Contains(inspectOut, fullID) || !strings.Contains(inspectOut, "docker.io/library/nginx:alpine") {
		t.Fatalf("unexpected inspect output:\n%s", inspectOut)
	}
}

func testStopWorkload(t *testing.T, ctx context.Context, shortID string) {
	t.Helper()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{"workload", "stop", shortID}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("workload stop failed: %v", err)
	}

	stopOut := stdout.String()
	if !strings.Contains(stopOut, "Stopped workload") {
		t.Fatalf("unexpected stop output:\n%s", stopOut)
	}

	stdout.Reset()
	stderr.Reset()
	err = cli.Execute(ctx, []string{"workload", "list"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("workload list after stop failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "No active workloads found") {
		t.Fatalf("expected empty workload list, got:\n%s", stdout.String())
	}
}

func TestCLIWorkloadLifecycle(t *testing.T) {
	setupCLITest(t)
	ctx := context.Background()

	workloadID := testSubmitWorkload(t, ctx)
	shortID := workloadID[:8]

	testVerifyList(t, ctx, shortID)
	testVerifyInspect(t, ctx, shortID, workloadID)
	testStopWorkload(t, ctx, shortID)
}

func TestCLINodeListEmpty(t *testing.T) {
	setupCLITest(t)
	ctx := context.Background()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{"node", "list"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("node list failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "No cluster nodes found") {
		t.Fatalf("expected empty nodes message, got:\n%s", stdout.String())
	}
}

func TestCLIValidationErrors(t *testing.T) {
	setupCLITest(t)
	ctx := context.Background()

	var stdout, stderr bytes.Buffer

	// Unknown command
	err := cli.Execute(ctx, []string{"unknowncmd"}, &stdout, &stderr)
	if err == nil {
		t.Fatalf("expected error on unknown command")
	}

	// Workload run missing image
	stdout.Reset()
	stderr.Reset()
	err = cli.Execute(ctx, []string{"workload", "run"}, &stdout, &stderr)
	if err == nil {
		t.Fatalf("expected error on workload run without image")
	}

	// Workload run invalid port
	stdout.Reset()
	stderr.Reset()
	err = cli.Execute(ctx, []string{"workload", "run", "-p", "invalid", "nginx"}, &stdout, &stderr)
	if err == nil {
		t.Fatalf("expected error on invalid port mapping")
	}

	// Workload inspect non-existent ID
	stdout.Reset()
	stderr.Reset()
	err = cli.Execute(ctx, []string{"workload", "inspect", "nonexistent"}, &stdout, &stderr)
	if err == nil {
		t.Fatalf("expected error inspecting nonexistent workload")
	}
}
