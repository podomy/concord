// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/podomy/concord/internal/workload"
)

// CheckHealth queries the health endpoint on a port specified in the spec
// (HostPort and ContainerPort) and returns true if the endpoint returns a 2xx status code.
func CheckHealth(ctx context.Context, logger *zap.Logger, spec workload.Spec) bool {
	err := ctx.Err()
	if err != nil {
		logger.Error("context cancellation", zap.Error(err))
		return false
	}

	if spec.Removed {
		return false
	}

	if spec.HostPort == 0 || spec.ContainerPort == 0 {
		return true
	}

	path := spec.HealthPath
	if path == "" {
		path = "/health"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	url := fmt.Sprintf("http://127.0.0.1:%d%s", spec.HostPort, path)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		logger.Error("failed to create health check request", zap.Uint16("port", spec.HostPort), zap.Error(err))
		return false
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Warn("health check request failed", zap.Uint16("port", spec.HostPort), zap.Error(err))
		return false
	}
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body) //nolint:errcheck // best-effort body drain
		_ = resp.Body.Close()                 //nolint:errcheck // best-effort body close
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Warn("health check returned non-2xx status code", zap.Uint16("port", spec.HostPort), zap.Int("status_code", resp.StatusCode))
		return false
	}

	return true
}
