// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ipc_test

import (
	"net/http"
	"testing"
)

// Trailing data after the first JSON value is a 400, never a silent
// second value.
func TestIPCSubmitRejectsTrailingGarbage(t *testing.T) {
	h := setupTestServer(t)

	status, body := postRawJSON(t, h, http.MethodPost, "/v1/workloads", `{"image":"app:latest"} {"image":"evil:latest"}`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body %s", status, body)
	}

	status, _ = postRawJSON(t, h, http.MethodPost, "/v1/workloads", `{"image":"app:latest"} trailing`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
}

// Trailing data on the image bump path is a 400 before any lookup.
func TestIPCUpdateImageRejectsTrailingGarbage(t *testing.T) {
	h := setupTestServer(t)
	id := "11111111-1111-1111-1111-111111111111"

	status, _ := postRawJSON(t, h, http.MethodPut, "/v1/workloads/"+id+"/image", `{"image":"app:v2"} trailing`)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", status)
	}
}
