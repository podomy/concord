// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/podomy/concord/internal/workload"
)

func TestCheckHealthContextCancelled(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	spec := workload.Spec{HostPort: 8080, ContainerPort: 80}
	if healthy := CheckHealth(ctx, logger, spec); healthy {
		t.Error("expected false when context is cancelled")
	}
}

func TestCheckHealthSpecRemoved(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{Removed: true, HostPort: 8080, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec); healthy {
		t.Error("expected false when spec is marked as removed")
	}
}

func TestCheckHealthNoPortSpecified(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HostPort: 0, ContainerPort: 0}
	if healthy := CheckHealth(t.Context(), logger, spec); !healthy {
		t.Error("expected true when no port is specified in spec")
	}
}

func TestCheckHealthPartialPortSpecified(t *testing.T) {
	logger := zaptest.NewLogger(t)
	specHostOnly := workload.Spec{HostPort: 8080, ContainerPort: 0}
	if healthy := CheckHealth(t.Context(), logger, specHostOnly); !healthy {
		t.Error("expected true when only HostPort is specified without ContainerPort")
	}

	specContainerOnly := workload.Spec{HostPort: 0, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, specContainerOnly); !healthy {
		t.Error("expected true when only ContainerPort is specified without HostPort")
	}
}

func TestCheckHealthHostPortSuccess(t *testing.T) {
	logger := zaptest.NewLogger(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("failed to parse server URL: %v", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("failed to parse port: %v", err)
	}

	spec := workload.Spec{HostPort: uint16(port), ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec); !healthy {
		t.Error("expected true for healthy HTTP 200 endpoint")
	}
}

func TestCheckHealthCustomHealthPath(t *testing.T) {
	logger := zaptest.NewLogger(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/custom/ping" {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)

	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("failed to parse server URL: %v", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("failed to parse port: %v", err)
	}

	spec := workload.Spec{HostPort: uint16(port), ContainerPort: 80, HealthPath: "custom/ping"}
	if healthy := CheckHealth(t.Context(), logger, spec); !healthy {
		t.Error("expected true for custom health path")
	}
}

func TestCheckHealthNon2xxStatus(t *testing.T) {
	logger := zaptest.NewLogger(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("failed to parse server URL: %v", err)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		t.Fatalf("failed to parse port: %v", err)
	}

	spec := workload.Spec{HostPort: uint16(port), ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec); healthy {
		t.Error("expected false when endpoint returns 500 status")
	}
}

func TestCheckHealthConnectionRefused(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HostPort: 59999, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec); healthy {
		t.Error("expected false when connection is refused")
	}
}
