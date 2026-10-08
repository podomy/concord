// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"encoding/json"
	"testing"

	"github.com/hashicorp/memberlist"
)

func TestMemberState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   memberlist.NodeStateType
		want NodeState
	}{
		{memberlist.StateAlive, NodeStateAlive},
		{memberlist.StateSuspect, NodeStateSuspect},
		{memberlist.StateDead, NodeStateDead},
		{memberlist.StateLeft, NodeStateLeft},
		{memberlist.NodeStateType(99), NodeStateUnknown},
	}
	for _, tc := range cases {
		if got := memberState(tc.in); got != tc.want {
			t.Fatalf("memberState(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSetWorkloadCount(t *testing.T) {
	t.Parallel()

	delegate := &nodeMetadataDelegate{}
	delegate.meta = func() NodeMetadata {
		return NodeMetadata{
			CPUMHz:    2400.0,
			MemoryMB:  8192,
			Workloads: int(delegate.workloads.Load()),
		}
	}

	ms := &MemberService{delegate: delegate}

	ms.SetWorkloadCount(5)
	metaBytes := delegate.NodeMeta(512)

	if got := delegate.workloads.Load(); got != 5 {
		t.Fatalf("expected workload count 5, got %d", got)
	}

	if !testing.Short() {
		metaStr := string(metaBytes)
		if metaStr == "" {
			t.Fatal("expected non-empty NodeMeta JSON bytes")
		}
	}

	// Test nil MemberService does not panic.
	var nilService *MemberService
	nilService.SetWorkloadCount(10)
}

// SetPressure lands in gossiped metadata next to the workload count.
func TestSetPressure(t *testing.T) {
	t.Parallel()

	delegate := &nodeMetadataDelegate{}
	delegate.meta = func() NodeMetadata {
		return NodeMetadata{
			Workloads:   int(delegate.workloads.Load()),
			CPUPercent:  clampPressure(delegate.cpu.Load()),
			MemPercent:  clampPressure(delegate.mem.Load()),
			DiskPercent: clampPressure(delegate.disk.Load()),
		}
	}

	ms := &MemberService{delegate: delegate}

	ms.SetPressure(30, 40, 50)
	metaBytes := delegate.NodeMeta(512)

	var meta NodeMetadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		t.Fatal(err)
	}
	if meta.CPUPercent != 30 || meta.MemPercent != 40 || meta.DiskPercent != 50 {
		t.Fatalf("pressure = %d/%d/%d", meta.CPUPercent, meta.MemPercent, meta.DiskPercent)
	}
	if len(metaBytes) > 512 {
		t.Fatalf("metadata %d bytes exceeds gossip cap", len(metaBytes))
	}

	// Test nil MemberService does not panic.
	var nilService *MemberService
	nilService.SetPressure(1, 2, 3)
}
