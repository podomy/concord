// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package logs

import (
	"errors"
	"fmt"
	"syscall"

	"go.uber.org/zap"
)

func Init() (*zap.Logger, func() error, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, nil, fmt.Errorf("initialize logger: %w", err)
	}

	// a slightly modified function for emptying the log buffer before exiting
	syncLogs := func() error {
		err := logger.Sync()
		if err == nil {
			return nil
		}

		// we suppress an error of "invalid argument"
		// it just means the output cannot be fsynced
		// it is noise during normal terminal use
		if errors.Is(err, syscall.EINVAL) {
			return nil
		}

		return fmt.Errorf("sync logger: %w", err)
	}

	return logger, syncLogs, nil
}

func Node(logger *zap.Logger, nodeID string) *zap.Logger {
	return logger.With(zap.String("node_id", nodeID))
}
