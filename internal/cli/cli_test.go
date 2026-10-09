// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli_test

import (
	"bytes"
	"context"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/cli"
	"github.com/podomy/concord/internal/geo"
	"github.com/podomy/concord/internal/ipc"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/peerdiscovery"
)

// setupCLITest starts the CLI test server without a peer service, so
// handlers degrade to their unavailable branches (empty node list,
// 503 trail). Tests needing gossip use setupCLIPositionedTest.
func setupCLITest(t *testing.T) *node.Sampler {
	t.Helper()

	sampler, _ := setupCLIServer(t, false)
	return sampler
}

// setupCLIPositionedTest starts the CLI test server with a real loopback
// member service, so position tests cover gossip end to end.
func setupCLIPositionedTest(t *testing.T) (*node.Sampler, *peerdiscovery.MemberService) {
	t.Helper()

	return setupCLIServer(t, true)
}

func setupCLIServer(t *testing.T, withPeers bool) (*node.Sampler, *peerdiscovery.MemberService) {
	t.Helper()

	tempDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tempDir)

	socketDir := filepath.Join(tempDir, "concord")
	if err := os.MkdirAll(socketDir, 0o750); err != nil {
		t.Fatalf("create socket dir: %v", err)
	}

	var peerService *peerdiscovery.MemberService
	if withPeers {
		peerService = startCLIPeers(t, socketDir)
	}

	socketPath := filepath.Join(socketDir, "concord.sock")
	dbPath := filepath.Join(tempDir, "test.db")
	journalPath := filepath.Join(tempDir, "journal.jsonl")

	kv, j := openCLITestStores(t, dbPath, journalPath)

	workloads := journalview.NewWorkloads(kv)
	views := []journalview.View{workloads}

	nodeID := uuid.New()
	sampler := node.NewSampler()
	server := ipc.NewServer(nodeID, j, views, workloads, peerService, zap.NewNop(), sampler)

	ctx, cancel := context.WithCancel(context.Background())

	if err := server.Start(ctx, socketPath); err != nil {
		t.Fatalf("start ipc server: %v", err)
	}

	t.Cleanup(func() {
		cancel()
		if peerService != nil {
			if err := peerService.Shutdown(); err != nil {
				t.Logf("shutdown peer service: %v", err)
			}
		}
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

	return sampler, peerService
}

// openCLITestStores opens the kv and journal backing the CLI test server.
func openCLITestStores(t *testing.T, dbPath, journalPath string) (*kvstore.KVStore, *journal.JSONL) {
	t.Helper()

	kv, err := kvstore.OpenDBPath(dbPath)
	if err != nil {
		t.Fatalf("open kv store: %v", err)
	}

	j, err := journal.OpenJSONLPath(journalPath)
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}
	return kv, j
}

// startCLIPeers provisions a gossip key and starts one loopback member
// for CLI tests that cover gossip end to end.
func startCLIPeers(t *testing.T, socketDir string) *peerdiscovery.MemberService {
	t.Helper()

	keyDir := filepath.Join(socketDir, "memberservice")
	if err := os.MkdirAll(keyDir, 0o700); err != nil {
		t.Fatalf("create gossip key dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(keyDir, "secret.key"), bytes.Repeat([]byte{0x07}, 32), 0o600); err != nil {
		t.Fatalf("write gossip key: %v", err)
	}

	peerService, err := peerdiscovery.Start(zap.NewNop(), peerdiscovery.Node{
		ID:      uuid.New(),
		Address: netip.MustParseAddrPort("127.0.0.1:0"),
	}, nil, netip.Addr{}, peerdiscovery.NoiseIdentity{})
	if err != nil {
		t.Fatalf("start peer service: %v", err)
	}
	return peerService
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

func TestCLIWorkloadStats(t *testing.T) {
	sampler := setupCLITest(t)
	ctx := context.Background()

	workloadID := testSubmitWorkload(t, ctx)
	shortID := workloadID[:8]

	id, err := uuid.Parse(workloadID)
	if err != nil {
		t.Fatalf("parse workload id: %v", err)
	}
	sampler.SampleWorkload(id, 0, 128*1024*1024, 0, time.Now())

	var stdout, stderr bytes.Buffer
	err = cli.Execute(ctx, []string{"workload", "stats", shortID}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("workload stats failed: %v, stderr: %s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "128 MB") || !strings.Contains(out, "Disk") {
		t.Fatalf("expected memory usage and node pressure in output, got:\n%s", out)
	}
}

func TestCLIMetrics(t *testing.T) {
	setupCLITest(t)
	ctx := context.Background()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{"metrics"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("metrics failed: %v, stderr: %s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "concord_node_cpu_percent") {
		t.Fatalf("expected exposition output, got:\n%s", out)
	}
}

func TestCLINodeTrail(t *testing.T) {
	sampler := setupCLITest(t)
	ctx := context.Background()

	sampler.RecordPosition(geo.Point{Lat: 47.6, Lon: 8.9})

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{"node", "trail"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("node trail failed: %v, stderr: %s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "47.600000 8.900000") {
		t.Fatalf("expected coordinates in output, got:\n%s", out)
	}
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

// Setting a live fix through the CLI lands in gossip and the trail,
// and persists for the next boot.
func TestCLINodePositionSet(t *testing.T) {
	sampler, peerService := setupCLIPositionedTest(t)
	ctx := context.Background()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{"node", "position", "set", "--lat", "47.6", "--lon", "8.9"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("node position set failed: %v, stderr: %s", err, stderr.String())
	}
	if out := stdout.String(); !strings.Contains(out, "47.600000 8.900000") {
		t.Fatalf("expected confirmation, got:\n%s", out)
	}

	if trail := sampler.Trail(); len(trail) != 1 || trail[0].Pos.Lat != 47.6 {
		t.Fatalf("trail = %+v", trail)
	}
	members, err := peerService.Members()
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(members[0].Metadata.Lat-47.6) > 1e-6 || math.Abs(members[0].Metadata.Lon-8.9) > 1e-6 {
		t.Fatalf("gossip = %+v", members[0].Metadata)
	}

	var out2, err2 bytes.Buffer
	err = cli.Execute(ctx, []string{"node", "position", "set", "--lat", "91", "--lon", "0"}, &out2, &err2)
	if err == nil {
		t.Fatal("expected error for out-of-planet fix")
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

func TestCLINodeRotateKey(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("XDG_CONFIG_HOME", tmpDir)
	ctx := context.Background()

	var stdout, stderr bytes.Buffer
	err := cli.Execute(ctx, []string{"node", "rotate-key"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("node rotate-key failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "generation 1") {
		t.Fatalf("expected generation 1 in output, got:\n%s", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	err = cli.Execute(ctx, []string{"node", "rotate-key"}, &stdout, &stderr)
	if err != nil {
		t.Fatalf("second node rotate-key failed: %v", err)
	}
	if !strings.Contains(stdout.String(), "generation 2") {
		t.Fatalf("expected generation 2 in output, got:\n%s", stdout.String())
	}
}
