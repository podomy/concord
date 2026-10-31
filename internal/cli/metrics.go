// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"context"
	"fmt"
	"io"
)

// printMetricsHelp writes metrics usage to w.
func printMetricsHelp(w io.Writer) {
	_, _ = fmt.Fprintln(w, "Print sampler state in Prometheus exposition format") //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "")                                                    //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "Usage:")                                              //nolint:errcheck // CLI output
	_, _ = fmt.Fprintln(w, "  concord metrics")                                   //nolint:errcheck // CLI output
}

// runMetricsCommand prints exposition text after handling metrics flags.
func runMetricsCommand(ctx context.Context, stdout, _ io.Writer, args []string) error {
	for _, a := range args {
		if isHelpRequest(a) {
			printMetricsHelp(stdout)
			return nil
		}
		if isFlagToken(a) {
			return fmt.Errorf("unknown flag: %s", a)
		}
	}
	return handleMetrics(ctx, stdout)
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
