// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"fmt"

	"github.com/hashicorp/memberlist"
)

func NewMemberList() (*memberlist.Memberlist, error) {
	list, err := memberlist.Create(memberlist.DefaultLocalConfig())
	if err != nil {
		return nil, fmt.Errorf("memberlist create: %w", err)
	}

	return list, nil
}

func JoinMembers(list *memberlist.Memberlist, existing []string) (int, error) {
	n, err := list.Join(existing)
	if err != nil {
		return 0, fmt.Errorf("list join: %w", err)
	}

	return n, nil
}
