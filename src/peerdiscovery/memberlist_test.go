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
