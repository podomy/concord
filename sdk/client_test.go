// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/sdk"
)

// setupMockUnixServer starts an HTTP server listening on a temporary Unix socket.
func setupMockUnixServer(t *testing.T, handler http.Handler) (string, func()) {
	t.Helper()

	sockPath := filepath.Join(t.TempDir(), "concord_test.sock")
	var lc net.ListenConfig
	listener, err := lc.Listen(context.Background(), "unix", sockPath)
	if err != nil {
		t.Fatalf("failed to listen on unix socket: %v", err)
	}

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		_ = server.Serve(listener) //nolint:errcheck // test server lifecycle
	}()

	cleanup := func() {
		_ = server.Close()   //nolint:errcheck // best-effort test cleanup
		_ = listener.Close() //nolint:errcheck // best-effort test cleanup
	}

	return sockPath, cleanup
}

func writeJSONResponse(t *testing.T, w http.ResponseWriter, status int, data any) {
	t.Helper()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		t.Fatalf("write JSON response failed: %v", err)
	}
}

func TestClient_Submit(t *testing.T) {
	t.Parallel()

	expectedID := uuid.New()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workloads", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]any{"id": expectedID})
	})

	sockPath, cleanup := setupMockUnixServer(t, mux)
	defer cleanup()

	client, err := sdk.Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial mock server: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	workload := sdk.NewWorkload().Image("nginx:alpine").MustBuild()
	id, err := client.Submit(context.Background(), workload)
	if err != nil {
		t.Fatalf("unexpected submit error: %v", err)
	}
	if id != expectedID {
		t.Fatalf("expected workload ID %s, got %s", expectedID, id)
	}
}

func TestClient_Submit_ServerError(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workloads", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusBadRequest, map[string]string{
			"error": "invalid container configuration",
		})
	})

	sockPath, cleanup := setupMockUnixServer(t, mux)
	defer cleanup()

	client, err := sdk.Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	workload := sdk.NewWorkload().Image("nginx").MustBuild()
	_, err = client.Submit(context.Background(), workload)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestClient_Stop_Success(t *testing.T) {
	t.Parallel()

	targetID := uuid.New()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workloads/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && r.URL.Path == "/v1/workloads/"+targetID.String() {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	sockPath, cleanup := setupMockUnixServer(t, mux)
	defer cleanup()

	client, err := sdk.Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	if err := client.Stop(context.Background(), targetID); err != nil {
		t.Fatalf("unexpected stop error: %v", err)
	}
}

func TestClient_Stop_NotFound(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workloads/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	sockPath, cleanup := setupMockUnixServer(t, mux)
	defer cleanup()

	client, err := sdk.Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	err = client.Stop(context.Background(), uuid.New())
	if !errors.Is(err, sdk.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestClient_Get_Success(t *testing.T) {
	t.Parallel()

	targetID := uuid.New()
	expectedWorkload := sdk.NewWorkload().
		Image("redis:alpine").
		Port(6379, 6379).
		MustBuild()
	expectedWorkload.ID = targetID

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workloads/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/workloads/"+targetID.String() {
			writeJSONResponse(t, w, http.StatusOK, expectedWorkload)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	})

	sockPath, cleanup := setupMockUnixServer(t, mux)
	defer cleanup()

	client, err := sdk.Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	w, err := client.Get(context.Background(), targetID)
	if err != nil {
		t.Fatalf("unexpected get error: %v", err)
	}
	if w.Image != "redis:alpine" || w.HostPort != 6379 {
		t.Fatalf("unexpected retrieved workload: %+v", w)
	}
}

func TestClient_Get_NotFound(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workloads/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	sockPath, cleanup := setupMockUnixServer(t, mux)
	defer cleanup()

	client, err := sdk.Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	_, err = client.Get(context.Background(), uuid.New())
	if !errors.Is(err, sdk.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestClient_List(t *testing.T) {
	t.Parallel()

	expectedList := []sdk.Workload{
		sdk.NewWorkload().Image("nginx:alpine").MustBuild(),
		sdk.NewWorkload().Image("redis:alpine").MustBuild(),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workloads", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]any{
			"workloads": expectedList,
		})
	})

	sockPath, cleanup := setupMockUnixServer(t, mux)
	defer cleanup()

	client, err := sdk.Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	workloads, err := client.List(context.Background())
	if err != nil {
		t.Fatalf("unexpected list error: %v", err)
	}
	if len(workloads) != 2 {
		t.Fatalf("expected 2 workloads, got %d", len(workloads))
	}
}

func TestClient_Nodes(t *testing.T) {
	t.Parallel()

	expectedNodes := []sdk.Node{
		{
			ID:                 uuid.New(),
			Address:            "10.0.0.1:9000",
			State:              "alive",
			WireGuardPublicKey: "mock-pub-key-1",
		},
		{
			ID:                 uuid.New(),
			Address:            "10.0.0.2:9000",
			State:              "alive",
			WireGuardPublicKey: "mock-pub-key-2",
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSONResponse(t, w, http.StatusOK, map[string]any{
			"nodes": expectedNodes,
		})
	})

	sockPath, cleanup := setupMockUnixServer(t, mux)
	defer cleanup()

	client, err := sdk.Dial(sockPath)
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	nodes, err := client.Nodes(context.Background())
	if err != nil {
		t.Fatalf("unexpected nodes error: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0].WireGuardPublicKey != "mock-pub-key-1" {
		t.Errorf("expected wireguard key 'mock-pub-key-1', got %q", nodes[0].WireGuardPublicKey)
	}
}

func TestClient_DaemonUnreachable(t *testing.T) {
	t.Parallel()

	nonExistentSock := filepath.Join(t.TempDir(), "non_existent.sock")
	client, err := sdk.Dial(nonExistentSock)
	if err != nil {
		t.Fatalf("unexpected dial error: %v", err)
	}
	defer client.Close() //nolint:errcheck // best-effort close in test defer

	_, err = client.List(context.Background())
	if err == nil {
		t.Fatal("expected unreachable error, got nil")
	}
	if !errors.Is(err, sdk.ErrDaemonUnreachable) {
		t.Fatalf("expected ErrDaemonUnreachable, got %v", err)
	}
}
