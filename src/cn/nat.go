// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/coreos/go-iptables/iptables"
)

// SetupMasquerade adds an iptables SNAT rule on the host so containers
// in the bridge subnet can reach the internet. Outbound packets from
// 10.0.0.0/16 get their source address rewritten to the host's external
// IP. Container-to-container traffic on cn0 is excluded. Idempotent,
// safe to call on every startup.
func SetupMasquerade(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	ipt, err := iptables.New()
	if err != nil {
		return fmt.Errorf("setup masquerade: %w", err)
	}

	// Append a masquerade rule to the host's NAT POSTROUTING chain.
	//
	//   -t nat            NAT table.
	//   POSTROUTING       After routing, before the packet leaves.
	//   -s 10.0.0.0/16    Only packets with a container subnet source.
	//   -i cn0            Only packets arriving from the bridge interface.
	//   ! -o cn0          NOT leaving through the bridge (skip container-to-container).
	//   -j MASQUERADE     Rewrite source address to the host's external IP.
	//
	// The rule is idempotent AppendUnique skips it if it already exists.
	err = ipt.AppendUnique("nat", "POSTROUTING",
		"-s", "10.0.0.0/16",
		"-i", "cn0",
		"!", "-o", "cn0",
		"-j", "MASQUERADE",
	)
	if err != nil {
		return fmt.Errorf("append to iptable: %w", err)
	}

	return nil
}

// TeardownMasquerade removes the masquerade SNAT rule added by
// SetupMasquerade. Safe to call even if the rule was already removed
// or never added. Call on shutdown.
func TeardownMasquerade(ctx context.Context) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	ipt, err := iptables.New()
	if err != nil {
		return fmt.Errorf("teardown masquerade: %w", err)
	}

	err = ipt.DeleteIfExists("nat", "POSTROUTING",
		"-s", "10.0.0.0/16",
		"-i", "cn0",
		"!", "-o", "cn0",
		"-j", "MASQUERADE",
	)
	if err != nil {
		return fmt.Errorf("delete from iptable: %w", err)
	}

	return nil
}

// AddPortMapping adds an iptables DNAT rule that forwards traffic
// arriving on the host's hostPort to the container at
// containerIP:containerPort. Idempotent, safe to call on every
// reconciliation tick. containerIPAndCIDR assumes the following format "10.0.0.0/16".
//
//nolint:dupl // already deduplicated via buildPortRulespec
func AddPortMapping(ctx context.Context, hostPort uint16, containerIPAndCIDR string, containerPort uint16) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	ipt, err := iptables.New()
	if err != nil {
		return fmt.Errorf("setup port mapping: %w", err)
	}

	ruleSpec, err := buildPortRulespec(containerIPAndCIDR, hostPort, containerPort)
	if err != nil {
		return fmt.Errorf("build port rulespec: %w", err)
	}

	err = ipt.AppendUnique("nat", "PREROUTING", ruleSpec...)
	if err != nil {
		return fmt.Errorf("append to iptable: %w", err)
	}

	return nil
}

// RemovePortMapping removes the DNAT rule for hostPort. Safe to call
// even if the rule does not exist. Call when the container stops.
// containerIPAndCIDR assumes the following format "10.0.0.0/16".
//
//nolint:dupl // already deduplicated via buildPortRulespec
func RemovePortMapping(ctx context.Context, hostPort uint16, containerIPAndCIDR string, containerPort uint16) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	ipt, err := iptables.New()
	if err != nil {
		return fmt.Errorf("remove port mapping: %w", err)
	}

	ruleSpec, err := buildPortRulespec(containerIPAndCIDR, hostPort, containerPort)
	if err != nil {
		return fmt.Errorf("build port rulespec: %w", err)
	}

	err = ipt.DeleteIfExists("nat", "PREROUTING", ruleSpec...)
	if err != nil {
		return fmt.Errorf("delete from iptable: %w", err)
	}

	return nil
}

func buildPortRulespec(containerIPAndCIDR string, hostPort, containerPort uint16) ([]string, error) {
	hostPortStr := strconv.FormatUint(uint64(hostPort), 10)
	containerPortStr := strconv.FormatUint(uint64(containerPort), 10)
	containerIP, _, found := strings.Cut(containerIPAndCIDR, "/")
	if !found {
		return nil, fmt.Errorf("invalid CIDR format: %s", containerIPAndCIDR)
	}
	return []string{
		"-p", "tcp",
		"--dport", hostPortStr,
		"-j", "DNAT",
		"--to-destination", containerIP + ":" + containerPortStr,
	}, nil
}
