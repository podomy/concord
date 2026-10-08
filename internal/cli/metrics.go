// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// newMetricsCommand creates the 'concord metrics' command.
func newMetricsCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "metrics",
		Short: "Print sampler state in Prometheus exposition format",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return handleMetrics(cmd.Context(), cmd.OutOrStdout())
		},
	}
}

// handleMetrics fetches Prometheus exposition text from the daemon and
// prints it unchanged for collectors and humans alike.
func handleMetrics(ctx context.Context, stdout io.Writer) error {
	client, closeFn, err := dialIPCClient()
	if err != nil {
		return err
	}
	defer closeFn()

	body, err := client.Metrics(ctx)
	if err != nil {
		return fmt.Errorf("get metrics: %w", err)
	}

	_, _ = fmt.Fprint(stdout, body) //nolint:errcheck // CLI output
	return nil
}
