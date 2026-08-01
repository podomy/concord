// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package journalview

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/workload"
)

func TestWorkloadsApplyAndGet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewWorkloads(kv)

	spec := workload.Spec{
		ID:        uuid.New(),
		Image:     "nginx:latest",
		SegmentID: uuid.New(),
		HostPort:  8080,
	}
	payload, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}

	event := journal.NewEvent(uuid.New(), "workload.spec", payload)

	if err := view.Apply(ctx, event); err != nil {
		t.Fatalf("apply event: %v", err)
	}

	got, err := view.Get(ctx, spec.ID)
	if err != nil {
		t.Fatalf("get spec: %v", err)
	}
	if got == nil {
		t.Fatalf("expected spec, got nil")
	}
	if diff := cmp.Diff(spec, *got); diff != "" {
		t.Fatalf("spec mismatch (-want +got):\n%s", diff)
	}
}

func TestWorkloadsIgnoreNonWorkloadEvents(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewWorkloads(kv)

	event := journal.NewEvent(uuid.New(), "node.started", json.RawMessage(`{}`))

	if err := view.Apply(ctx, event); err != nil {
		t.Fatalf("apply non-workload event: %v", err)
	}

	specs, err := view.List(ctx)
	if err != nil {
		t.Fatalf("list specs: %v", err)
	}
	if len(specs) != 0 {
		t.Fatalf("expected 0 specs, got %d", len(specs))
	}
}

func TestWorkloadsUpdateAndTombstone(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewWorkloads(kv)

	specID := uuid.New()
	spec1 := workload.Spec{
		ID:    specID,
		Image: "redis:6",
	}
	payload1, err := json.Marshal(spec1)
	if err != nil {
		t.Fatalf("marshal spec1: %v", err)
	}
	if err := view.Apply(ctx, journal.NewEvent(uuid.New(), "workload.spec", payload1)); err != nil {
		t.Fatalf("apply spec1: %v", err)
	}

	spec2 := workload.Spec{
		ID:      specID,
		Image:   "redis:6",
		Removed: true,
	}
	payload2, err := json.Marshal(spec2)
	if err != nil {
		t.Fatalf("marshal spec2: %v", err)
	}
	if err := view.Apply(ctx, journal.NewEvent(uuid.New(), "workload.spec", payload2)); err != nil {
		t.Fatalf("apply spec2: %v", err)
	}

	got, err := view.Get(ctx, specID)
	if err != nil {
		t.Fatalf("get spec: %v", err)
	}
	if got == nil || !got.Removed {
		t.Fatalf("expected tombstoned spec with Removed=true, got %+v", got)
	}
}

func TestWorkloadsList(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewWorkloads(kv)

	spec1 := workload.Spec{ID: uuid.New(), Image: "app:v1"}
	spec2 := workload.Spec{ID: uuid.New(), Image: "app:v2"}

	for _, spec := range []workload.Spec{spec1, spec2} {
		payload, err := json.Marshal(spec)
		if err != nil {
			t.Fatalf("marshal spec: %v", err)
		}
		if err := view.Apply(ctx, journal.NewEvent(uuid.New(), "workload.spec", payload)); err != nil {
			t.Fatalf("apply spec: %v", err)
		}
	}

	specs, err := view.List(ctx)
	if err != nil {
		t.Fatalf("list specs: %v", err)
	}
	if len(specs) != 2 {
		t.Fatalf("expected 2 specs, got %d", len(specs))
	}

	sortByID := cmpopts.SortSlices(func(a, b workload.Spec) bool {
		return a.ID.String() < b.ID.String()
	})
	expected := []workload.Spec{spec1, spec2}
	if diff := cmp.Diff(expected, specs, sortByID); diff != "" {
		t.Fatalf("list mismatch (-want +got):\n%s", diff)
	}
}

func TestWorkloadsApplyMalformedPayload(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewWorkloads(kv)

	event := journal.NewEvent(uuid.New(), "workload.spec", []byte("invalid json"))

	if err := view.Apply(ctx, event); err == nil {
		t.Fatalf("expected error for malformed payload")
	}
}

func TestWorkloadsCancelledContext(t *testing.T) {
	t.Parallel()

	ctx := cancelledContext()
	kv := testKVStore(t)
	view := NewWorkloads(kv)

	event := journal.NewEvent(uuid.New(), "workload.spec", json.RawMessage(`{}`))

	if err := view.Apply(ctx, event); err == nil {
		t.Fatalf("expected apply cancellation error")
	}
	if _, err := view.Get(ctx, uuid.New()); err == nil {
		t.Fatalf("expected get cancellation error")
	}
	if _, err := view.List(ctx); err == nil {
		t.Fatalf("expected list cancellation error")
	}
}

func TestWorkloadsMissingKeyAndBucket(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	kv := testKVStore(t)
	view := NewWorkloads(kv)

	got, err := view.Get(ctx, uuid.New())
	if err != nil {
		t.Fatalf("get non-existent spec: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil, got %v", got)
	}

	specs, err := view.List(ctx)
	if err != nil {
		t.Fatalf("list empty bucket: %v", err)
	}
	if len(specs) != 0 {
		t.Fatalf("expected empty list, got %d items", len(specs))
	}
}
