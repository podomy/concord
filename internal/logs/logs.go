// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package logs

import (
	"errors"
	"fmt"
	"strings"
	"syscall"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ParseLevel maps a flag value to a zap level. Empty means info, so
// unset flags and old callers keep today's behavior. Anything else
// fails loudly instead of silently running at an unintended level.
func ParseLevel(s string) (zapcore.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "info":
		return zapcore.InfoLevel, nil
	case "debug":
		return zapcore.DebugLevel, nil
	case "warn":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return zapcore.InfoLevel, fmt.Errorf("unknown log level %q: want debug, info, warn, or error", s)
	}
}

// Init initialises a production-grade zap logger at the given level and
// returns a sync function that should be called before exit to flush
// buffered log entries.
func Init(level zapcore.Level) (*zap.Logger, func() error, error) {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(level)
	logger, err := cfg.Build()
	if err != nil {
		return nil, nil, fmt.Errorf("initialize logger: %w", err)
	}

	// a slightly modified function for emptying the log buffer before exiting.
	syncLogs := func() error {
		err := logger.Sync()
		if err == nil {
			return nil
		}

		// we suppress an error of "invalid argument"
		// it just means the output cannot be fsynced
		// it is noise during normal terminal use.
		if errors.Is(err, syscall.EINVAL) {
			return nil
		}

		return fmt.Errorf("sync logger: %w", err)
	}

	return logger, syncLogs, nil
}

// Node returns a logger enriched with the node_id field for correlated logging.
func Node(logger *zap.Logger, nodeID string) *zap.Logger {
	return logger.With(zap.String("node_id", nodeID))
}
