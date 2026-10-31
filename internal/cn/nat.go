// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// iptablesBin is the iptables binary name resolved via PATH.
const iptablesBin = "iptables"

// iptablesWaitTimeout bounds TeardownMasquerade when no caller context
// exists. It is a var so tests can shrink it.
var iptablesWaitTimeout = 15 * time.Second

// execIptables runs iptables with args and returns trimmed stderr on
// failure. It prepends -w so concurrent callers wait on the xtables
// lock instead of failing, matching the default go-iptables behavior
// of passing --wait. Global -w parses on legacy getopt and nf_tables.
func execIptables(ctx context.Context, args []string) (string, error) {
	withWait := append([]string{"-w"}, args...)
	cmd := exec.CommandContext(ctx, iptablesBin, withWait...) //nolint:gosec // iptablesBin is a fixed binary name resolved via PATH
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err := cmd.Run()
	if err != nil {
		out := strings.TrimSpace(stderr.String())
		if out == "" {
			return "", fmt.Errorf("iptables %s: %w", strings.Join(withWait, " "), err)
		}
		return out, fmt.Errorf("iptables %s: %w: %s", strings.Join(withWait, " "), err, out)
	}
	return "", nil
}

// runIptables executes iptables with args. Stderr is included in
// failures to aid diagnosis. See execIptables for -w handling.
func runIptables(ctx context.Context, args []string) error {
	_, err := execIptables(ctx, args)
	if err != nil {
		return err
	}
	return nil
}

// isAbsentRuleStderr reports whether iptables stderr means the -C probe
// found no matching rule rather than a real failure.
func isAbsentRuleStderr(stderr string) bool {
	return strings.Contains(stderr, "Bad rule") || strings.Contains(stderr, "matching rule")
}

// ruleExists probes for rulespec with iptables -C. Exit status 1 means
// the rule is absent only when stderr reports a missing rule, such as
// Bad rule (does a matching rule exist in that chain?). Exit 1 with
// any other stderr, such as No chain or unknown option, is a real
// failure and is returned so callers fail closed.
func ruleExists(ctx context.Context, table, chain string, rulespec []string) (bool, error) {
	args := append([]string{"-t", table, "-C", chain}, rulespec...)
	stderr, err := execIptables(ctx, args)
	if err == nil {
		return true, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		if isAbsentRuleStderr(stderr) {
			return false, nil
		}
		return false, err
	}
	return false, err
}

// appendUnique appends rulespec to table/chain unless it already exists.
//
// This is check-then-act with plain -A. Ticks from the single-goroutine
// reconciler cannot interleave, so tick-vs-tick is safe. A separate
// process can still append a duplicate between the -C probe and -A.
//
//nolint:unparam // table is always nat to keep the iptables -t flag explicit.
func appendUnique(ctx context.Context, table, chain string, rulespec ...string) error {
	exists, err := ruleExists(ctx, table, chain, rulespec)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	err = runIptables(ctx, append([]string{"-t", table, "-A", chain}, rulespec...))
	if err != nil {
		return err
	}
	return nil
}

// deleteIfExists deletes rulespec from table/chain only if present.
//
//nolint:unparam // table is always nat to keep the iptables -t flag explicit.
func deleteIfExists(ctx context.Context, table, chain string, rulespec ...string) error {
	exists, err := ruleExists(ctx, table, chain, rulespec)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	err = runIptables(ctx, append([]string{"-t", table, "-D", chain}, rulespec...))
	if err != nil {
		return err
	}
	return nil
}

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

	// Append a masquerade rule to the host's NAT POSTROUTING chain.
	//
	//   -t nat            NAT table.
	//   POSTROUTING       After routing, before the packet leaves.
	//   -s 10.0.0.0/16    Only packets with a container subnet source.
	//   ! -o cn0          NOT leaving through the bridge (skip container-to-container).
	//   -j MASQUERADE     Rewrite source address to the host's external IP.
	//
	// The rule is idempotent appendUnique skips it if it already exists.
	err = appendUnique(ctx, "nat", "POSTROUTING",
		"-s", "10.0.0.0/16",
		"!", "-o", "cn0",
		"-j", "MASQUERADE",
	)
	if err != nil {
		return fmt.Errorf("append to iptable: %w", err)
	}

	return nil
}

// TeardownMasquerade removes the masquerade SNAT rule added by
// SetupMasquerade. A missing rule is a no-op, but a missing chain or
// other probe failure is returned. It uses an internal timeout so a
// stuck xtables lock cannot block shutdown forever. Call on shutdown.
func TeardownMasquerade() error {
	ctx, cancel := context.WithTimeout(context.Background(), iptablesWaitTimeout)
	defer cancel()
	err := deleteIfExists(ctx, "nat", "POSTROUTING",
		"-s", "10.0.0.0/16",
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
func AddPortMapping(ctx context.Context, hostPort uint16, containerIPAndCIDR string, containerPort uint16) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	ruleSpec, err := buildPortRulespec(containerIPAndCIDR, hostPort, containerPort)
	if err != nil {
		return fmt.Errorf("build port rulespec: %w", err)
	}

	err = appendUnique(ctx, "nat", "PREROUTING", ruleSpec...)
	if err != nil {
		return fmt.Errorf("append to iptable: %w", err)
	}

	return nil
}

// RemovePortMapping removes the DNAT rule for hostPort. Safe to call
// even if the rule does not exist. Call when the container stops.
// containerIPAndCIDR assumes the following format "10.0.0.0/16".
func RemovePortMapping(ctx context.Context, hostPort uint16, containerIPAndCIDR string, containerPort uint16) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	ruleSpec, err := buildPortRulespec(containerIPAndCIDR, hostPort, containerPort)
	if err != nil {
		return fmt.Errorf("build port rulespec: %w", err)
	}

	err = deleteIfExists(ctx, "nat", "PREROUTING", ruleSpec...)
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
