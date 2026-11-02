// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/podomy/concord/sdk"
)

// 503 from Metrics and Trail maps to ErrUnavailable with the server
// message preserved.
func TestClient_UnavailableMapping(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
	})
	mux.HandleFunc("/v1/nodes/self/trail", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusServiceUnavailable, map[string]string{
			"error": "trail unavailable",
		})
	})

	client := dialMockClient(t, mux)

	if _, err := client.Metrics(context.Background()); !errors.Is(err, sdk.ErrUnavailable) {
		t.Fatalf("metrics 503 = %v, want ErrUnavailable", err)
	}
	if _, err := client.Trail(context.Background()); !errors.Is(err, sdk.ErrUnavailable) {
		t.Fatalf("trail 503 = %v, want ErrUnavailable", err)
	}
}

// List and Nodes map any 4xx to ErrValidation and any 5xx to
// ErrUnavailable.
func TestClient_ListNodesStatusMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		path   string
		status int
		want   error
	}{
		{"list 400", "/v1/workloads", http.StatusBadRequest, sdk.ErrValidation},
		{"list 403", "/v1/workloads", http.StatusForbidden, sdk.ErrValidation},
		{"list 500", "/v1/workloads", http.StatusInternalServerError, sdk.ErrUnavailable},
		{"list 503", "/v1/workloads", http.StatusServiceUnavailable, sdk.ErrUnavailable},
		{"nodes 400", "/v1/nodes", http.StatusBadRequest, sdk.ErrValidation},
		{"nodes 503", "/v1/nodes", http.StatusServiceUnavailable, sdk.ErrUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mux := http.NewServeMux()
			mux.HandleFunc(tc.path, func(w http.ResponseWriter, _ *http.Request) {
				writeJSONResponse(t, w, tc.status, map[string]string{"error": "boom"})
			})
			client := dialMockClient(t, mux)
			var err error
			if tc.path == "/v1/nodes" {
				_, err = client.Nodes(context.Background())
			} else {
				_, err = client.List(context.Background())
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("%s %d = %v, want %v", tc.path, tc.status, err, tc.want)
			}
		})
	}
}
