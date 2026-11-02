// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/sdk"
)

// Mutating calls route through the shared mapping: 400 arrives as
// ErrValidation with the server message, 503 and every other 5xx as
// ErrUnavailable, 404 as ErrNotFound where the endpoint defines it.
func TestClient_SubmitStatusMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		status int
		want   error
	}{
		{"submit 400", http.StatusBadRequest, sdk.ErrValidation},
		{"submit 503", http.StatusServiceUnavailable, sdk.ErrUnavailable},
		{"submit 500", http.StatusInternalServerError, sdk.ErrUnavailable},
		{"submit 502", http.StatusBadGateway, sdk.ErrUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/workloads", func(w http.ResponseWriter, _ *http.Request) {
				writeJSONResponse(t, w, tc.status, map[string]string{"error": "boom"})
			})
			client := dialMockClient(t, mux)
			_, err := client.Submit(context.Background(), sdk.NewWorkload().Image("nginx").MustBuild())
			if !errors.Is(err, tc.want) {
				t.Fatalf("submit %d = %v, want %v", tc.status, err, tc.want)
			}
		})
	}
}

// UpdateImage routes through the same shared mapping, with 404 keeping
// its ErrNotFound and unknown codes staying unstructured.
func TestClient_UpdateImageStatusMapping(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		status int
		want   error
	}{
		{"update 400", http.StatusBadRequest, sdk.ErrValidation},
		{"update 503", http.StatusServiceUnavailable, sdk.ErrUnavailable},
		{"update 500", http.StatusInternalServerError, sdk.ErrUnavailable},
		{"update 429", http.StatusTooManyRequests, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertUpdateImageStatus(t, tc.status, tc.want)
		})
	}
}

// assertUpdateImageStatus serves one status from the image endpoint and
// checks the client mapping.
func assertUpdateImageStatus(t *testing.T, status int, want error) {
	t.Helper()
	id := uuid.New()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workloads/"+id.String()+"/image", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, status, map[string]string{"error": "boom"})
	})
	client := dialMockClient(t, mux)
	err := client.UpdateImage(context.Background(), id, "app:v2")
	if want == nil {
		if err == nil {
			t.Fatal("expected unstructured error, got nil")
		}
		if errors.Is(err, sdk.ErrValidation) || errors.Is(err, sdk.ErrUnavailable) || errors.Is(err, sdk.ErrNotFound) {
			t.Fatalf("status %d mapped to sentinel, want unstructured: %v", status, err)
		}
		return
	}
	if !errors.Is(err, want) {
		t.Fatalf("update %d = %v, want %v", status, err, want)
	}
}

// Non-4xx/5xx statuses stay unstructured on read paths: 418 via
// Metrics and Trail is neither validation nor unavailable.
func TestClient_NonStandardStatusUnstructured(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusTeapot, map[string]string{"error": "teapot"})
	})
	mux.HandleFunc("/v1/nodes/self/trail", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusTeapot, map[string]string{"error": "teapot"})
	})
	mux.HandleFunc("/v1/workloads", func(w http.ResponseWriter, _ *http.Request) {
		writeJSONResponse(t, w, http.StatusTeapot, map[string]string{"error": "teapot"})
	})

	client := dialMockClient(t, mux)

	if _, err := client.Metrics(context.Background()); err == nil {
		t.Fatal("metrics 418: expected error, got nil")
	} else if errors.Is(err, sdk.ErrValidation) || errors.Is(err, sdk.ErrUnavailable) {
		t.Fatalf("metrics 418 mapped to sentinel: %v", err)
	}

	if _, err := client.Trail(context.Background()); err == nil {
		t.Fatal("trail 418: expected error, got nil")
	} else if errors.Is(err, sdk.ErrValidation) || errors.Is(err, sdk.ErrUnavailable) {
		t.Fatalf("trail 418 mapped to sentinel: %v", err)
	}

	// List leaves 418 unstructured, matching Metrics and Trail: only
	// 400 and 422 map to ErrValidation.
	if _, err := client.List(context.Background()); err == nil {
		t.Fatal("list 418: expected error, got nil")
	} else if errors.Is(err, sdk.ErrValidation) || errors.Is(err, sdk.ErrUnavailable) {
		t.Fatalf("list 418 mapped to sentinel: %v", err)
	}
}
