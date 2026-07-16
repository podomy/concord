// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Port is the HTTPS node-to-node transport listen port.
const (
	Port = "8443"
)

func InitTransport(caFile, certFile, keyFile string) (*http.Server, error) {
	mux := http.NewServeMux()

	mux.HandleFunc("POST /v1/sync", postSync)

	tlsConfig, err := loadTLSConfig(caFile, certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load tls config: %w", err)
	}

	srv := &http.Server{
		Addr: ":" + Port,
		// middleware
		Handler:           chain(mux, requireHTTP2),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		TLSConfig:         tlsConfig,
	}

	return srv, nil
}

func Start(ctx context.Context, logger *zap.Logger, caFile, certFile, keyFile string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}

	httpServer, err := InitTransport(caFile, certFile, keyFile)
	if err != nil {
		return err
	}

	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", httpServer.Addr)
	if err != nil {
		return fmt.Errorf("net listen failed: %w", err)
	}

	// Stop when runtime shuts down. WithoutCancel keeps a live parent after ctx ends
	// so Shutdown can finish in-flight requests within the timeout.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Error("transport shutdown", zap.Error(err))
		}
	}()

	// Serve in background. ServeTLS blocks until shutdown.
	go func() {
		if err := httpServer.ServeTLS(listener, certFile, keyFile); err != nil && err != http.ErrServerClosed {
			logger.Error("serve tls", zap.Error(err))
		}
	}()

	return nil
}
