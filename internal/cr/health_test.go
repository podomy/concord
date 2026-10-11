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
	if healthy := CheckHealth(ctx, logger, spec, nil); healthy {
		t.Error("expected false when context is cancelled")
	}
}

func TestCheckHealthSpecRemoved(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{Removed: true, HostPort: 8080, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); healthy {
		t.Error("expected false when spec is marked as removed")
	}
}

func TestCheckHealthNoPortSpecified(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HostPort: 0, ContainerPort: 0}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); !healthy {
		t.Error("expected true when no port is specified in spec")
	}
}

func TestCheckHealthPartialPortSpecified(t *testing.T) {
	logger := zaptest.NewLogger(t)
	specHostOnly := workload.Spec{HostPort: 8080, ContainerPort: 0}
	if healthy := CheckHealth(t.Context(), logger, specHostOnly, nil); !healthy {
		t.Error("expected true when only HostPort is specified without ContainerPort")
	}

	specContainerOnly := workload.Spec{HostPort: 0, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, specContainerOnly, nil); !healthy {
		t.Error("expected true when only ContainerPort is specified without HostPort")
	}
}

func TestCheckHealthHostPortSuccess(t *testing.T) {
	logger := zaptest.NewLogger(t)
	port := startProbeHTTPServer(t, "/health")

	spec := workload.Spec{HostPort: port, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); !healthy {
		t.Error("expected true for healthy HTTP 200 endpoint")
	}
}

func TestCheckHealthCustomHealthPath(t *testing.T) {
	logger := zaptest.NewLogger(t)
	port := startProbeHTTPServer(t, "/custom/ping")

	spec := workload.Spec{HostPort: port, ContainerPort: 80, HealthPath: "custom/ping"}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); !healthy {
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
	if healthy := CheckHealth(t.Context(), logger, spec, nil); healthy {
		t.Error("expected false when endpoint returns 500 status")
	}
}

func TestCheckHealthConnectionRefused(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HostPort: 59999, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); healthy {
		t.Error("expected false when connection is refused")
	}
}

func TestCheckHealthTCPOpen(t *testing.T) {
	logger := zaptest.NewLogger(t)
	// Any open TCP socket answers the dial; the HTTP server is just a
	// convenient listener.
	port := startProbeHTTPServer(t, "/health")

	spec := workload.Spec{HealthProbe: workload.HealthProbeTCP, HostPort: port, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); !healthy {
		t.Error("expected true for open TCP port")
	}
}

func TestCheckHealthTCPClosed(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HealthProbe: workload.HealthProbeTCP, HostPort: 59999, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); healthy {
		t.Error("expected false for closed TCP port")
	}
}

func TestCheckHealthTCPWithoutPort(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HealthProbe: workload.HealthProbeTCP}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); healthy {
		t.Error("expected false for TCP probe without host port")
	}
}

func TestCheckHealthExecWithoutCommand(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HealthProbe: workload.HealthProbeExec, HostPort: 8080, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); healthy {
		t.Error("expected false for exec probe without command")
	}
}

func TestCheckHealthExecWithoutContainer(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HealthProbe: workload.HealthProbeExec, HealthCommand: []string{"/bin/true"}}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); healthy {
		t.Error("expected false for exec probe without container")
	}
}

func TestCheckHealthUnknownProbe(t *testing.T) {
	logger := zaptest.NewLogger(t)
	spec := workload.Spec{HealthProbe: workload.HealthProbe("smoke-signals"), HostPort: 8080, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); healthy {
		t.Error("expected false for unknown probe")
	}
}

func TestCheckHealthEmptyProbeStaysHTTP(t *testing.T) {
	logger := zaptest.NewLogger(t)
	port := startProbeHTTPServer(t, "/health")

	spec := workload.Spec{HostPort: port, ContainerPort: 80}
	if healthy := CheckHealth(t.Context(), logger, spec, nil); !healthy {
		t.Error("expected empty probe to behave as HTTP")
	}
}

// startHTTPHealthServer serves 200 on path and 404 elsewhere, returning
// the bound port.
func startProbeHTTPServer(t *testing.T, path string) uint16 {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == path {
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
	return uint16(port)
}
