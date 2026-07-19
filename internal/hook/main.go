// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Command hook runs the pre-commit verification: tidy, format, lint, test, and
// checks that no files were modified by the formatters.
package main

import (
	"context"
	"log"
	"os"
	"os/exec"
)

func run(ctx context.Context, name string, arg ...string) {
	// #nosec G204 — hook runs fixed commands from the repo, not user input.
	cmd := exec.CommandContext(ctx, arg[0], arg[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		log.Fatalf("%s: %v", name, err)
	}
}

func main() {
	ctx := context.Background()

	run(ctx, "go mod tidy", "go", "mod", "tidy")
	run(ctx, "golangci-lint fmt", "golangci-lint", "fmt", "./...")
	run(ctx, "golangci-lint header fix", "golangci-lint", "run", "--fix", "--enable-only=goheader", "--issues-exit-code=0", "./...")
	run(ctx, "golangci-lint", "golangci-lint", "run", "./...")
	run(ctx, "go test", "go", "test", "./...")

	// Ensure no unstaged changes remain after formatters.
	run(ctx, "git diff --exit-code", "git", "diff", "--exit-code")
}
