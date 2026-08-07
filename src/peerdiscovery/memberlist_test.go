// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
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

	// Test nil MemberService does not panic
	var nilService *MemberService
	nilService.SetWorkloadCount(10)
}
