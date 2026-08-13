// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"

	"github.com/google/uuid"

	"github.com/podomy/concord/sdk"
)

var (
	// ErrAmbiguousPrefix indicates multiple workloads matched the given UUID prefix.
	ErrAmbiguousPrefix = errors.New("ambiguous workload prefix")

	// ErrWorkloadNotFound indicates no workload matched the given ID or prefix.
	ErrWorkloadNotFound = errors.New("workload not found")

	// ErrMissingWorkloadID indicates an ID or prefix argument was not provided.
	ErrMissingWorkloadID = errors.New("workload ID or prefix is required")
)

// dialIPCClient connects to the Concord daemon IPC socket via the SDK.
// It returns an initialized sdk.Client and a cleanup function that callers should defer.
func dialIPCClient() (sdk.Client, func(), error) {
	client, err := sdk.Dial()
	if err != nil {
		return nil, nil, fmt.Errorf("connect to concord daemon: %w", err)
	}

	return client, func() { _ = client.Close() }, nil //nolint:errcheck // best-effort cleanup on exit
}

// dashIfEmpty returns the input string if non-empty, or a fallback "-" for table displays.
func dashIfEmpty(s string) string {
	if s != "" {
		return s
	}

	return "-"
}

// formatPorts formats host and container port mapping into a display string (e.g. "8080:80").
func formatPorts(hostPort, containerPort uint16) string {
	if hostPort > 0 && containerPort > 0 {
		return fmt.Sprintf("%d:%d", hostPort, containerPort)
	}

	return "-"
}

// truncateID returns the first 8 characters of a UUID string for compact terminal tables.
func truncateID(id uuid.UUID) string {
	s := id.String()
	if len(s) > 8 {
		return s[:8]
	}

	return s
}

// parsePortMapping parses a "host:container" string into two validated port numbers (1-65535).
func parsePortMapping(mapping string) (uint16, uint16, error) {
	mapping = strings.TrimSpace(mapping)
	if mapping == "" {
		return 0, 0, nil
	}

	parts := strings.Split(mapping, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid port format %q; expected host:container (e.g. 8080:80)", mapping)
	}

	hostPort, err1 := strconv.ParseUint(parts[0], 10, 16)
	containerPort, err2 := strconv.ParseUint(parts[1], 10, 16)
	if err1 != nil || err2 != nil || hostPort == 0 || containerPort == 0 {
		return 0, 0, fmt.Errorf("invalid ports in %q; must be valid port numbers (1-65535)", mapping)
	}

	return uint16(hostPort), uint16(containerPort), nil
}

// parseEnvVars parses a slice of "KEY=VALUE" strings into a map.
func parseEnvVars(entries []string) (map[string]string, error) {
	res := make(map[string]string, len(entries))
	for _, entry := range entries {
		k, v, ok := strings.Cut(entry, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("invalid environment variable %q; expected KEY=VALUE", entry)
		}
		res[k] = v
	}

	return res, nil
}

// resolveWorkloadID parses a full UUID or searches active workloads for a matching short prefix.
// This allows users to pass "concord workload stop 4b8d" instead of typing the entire 36-char UUID.
func resolveWorkloadID(ctx context.Context, client sdk.Client, input string) (uuid.UUID, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return uuid.Nil, fmt.Errorf("%w", ErrMissingWorkloadID)
	}

	// 1. Direct full UUID parse if the user provided all 36 characters
	if parsed, err := uuid.Parse(input); err == nil {
		return parsed, nil
	}

	// 2. Short prefix matching against currently active workloads
	workloads, err := client.List(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("query workloads for prefix resolution: %w", err)
	}

	var matches []uuid.UUID
	for _, w := range workloads {
		if strings.HasPrefix(w.ID.String(), input) {
			matches = append(matches, w.ID)
		}
	}

	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return uuid.Nil, fmt.Errorf("%w: %q", ErrWorkloadNotFound, input)
	default:
		return uuid.Nil, fmt.Errorf("%w %q (%d matching workloads)", ErrAmbiguousPrefix, input, len(matches))
	}
}

// newTableWriter initializes a tabwriter that computes column widths and aligns columns with spaces.
func newTableWriter(w io.Writer) *tabwriter.Writer {
	return tabwriter.NewWriter(w, 0, 4, 3, ' ', 0)
}
