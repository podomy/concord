// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ipc_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// Each HELP/TYPE header appears once per scrape no matter how many
// workloads report; the per-workload value lines stay identical.
func TestIPCMetricsSingleHelpBlock(t *testing.T) {
	h := setupTestServer(t)

	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	for _, id := range ids {
		h.sampler.SampleWorkload(id, 100_000_000, 64*1024*1024, 0, time.Now())
	}

	body, err := h.client.Metrics(context.Background())
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	for _, name := range []string{
		"concord_workload_cpu_percent",
		"concord_workload_mem_percent",
		"concord_workload_mem_usage_mb",
	} {
		if got := strings.Count(body, "# HELP "+name+" "); got != 1 {
			t.Fatalf("# HELP %s appears %d times, want 1:\n%s", name, got, body)
		}
		if got := strings.Count(body, "# TYPE "+name+" gauge"); got != 1 {
			t.Fatalf("# TYPE %s appears %d times, want 1:\n%s", name, got, body)
		}
		for _, id := range ids {
			want := name + `{id="` + id.String() + `"}`
			if !strings.Contains(body, want) {
				t.Fatalf("missing series %q in:\n%s", want, body)
			}
		}
	}
}
