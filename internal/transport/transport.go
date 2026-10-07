// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"go.uber.org/zap"
)

// Port is the node-to-node transport listen port.
var Port = "8443"

// Start serves the sync API over Noise on Port. Every accepted TCP connection
// runs the IK responder handshake first; connections presenting an unsigned or
// mismatched key are closed before serving. verify admits members and enforces
// pinning; see KeyPinner in runtime. Sync logic itself lives in sync.go and is
// unchanged by the framing. A nil index serves from full scans; a live one
// seeks by cursor offset and builds in the background until caught up.
func Start(
	ctx context.Context,
	logger *zap.Logger,
	static StaticKey,
	parcel []byte,
	verify Verifier,
	index *offsetIndex,
) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}

	mux := http.NewServeMux()
	// Every request carries the index so pages seek by cursor offset.
	// A nil index serves from full scans, which keeps index-less
	// setups on today's behavior.
	mux.HandleFunc("POST "+SyncPath, func(w http.ResponseWriter, r *http.Request) {
		postSync(w, r, index)
	})

	srv := &http.Server{
		Addr:              ":" + Port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
	}

	var lc net.ListenConfig
	raw, err := lc.Listen(ctx, "tcp", srv.Addr)
	if err != nil {
		return fmt.Errorf("net listen failed: %w", err)
	}

	listener := &noiseListener{
		Listener: raw,
		static:   static,
		parcel:   parcel,
		verify:   verify,
		logger:   logger,
	}

	// Fill the index in the background: one full scan in small batches.
	// Requests serve through the full-scan fallback until it catches up,
	// so serving never waits for the build.
	if index != nil {
		go buildSyncIndex(ctx, logger, index)
	}

	// Stop when runtime shuts down. WithoutCancel keeps a live parent after ctx ends
	// so Shutdown can finish in-flight requests within the timeout.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("transport shutdown", zap.Error(err))
		}
	}()

	// Serve in background. Serve blocks until shutdown closes the listener.
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			logger.Error("serve noise", zap.Error(err))
		}
	}()

	return nil
}

// buildSyncIndex runs the one-time offset backfill and logs the outcome.
// A failed build only costs full scans; every request still serves.
func buildSyncIndex(ctx context.Context, logger *zap.Logger, index *offsetIndex) {
	err := index.build(ctx)
	if err != nil {
		logger.Warn("sync index build failed", zap.Error(err))
	}
}

// handshakeError is a temporary Accept error carrying a failed Noise
// handshake. Temporary tells http.Serve to back off and keep serving instead
// of exiting, which is what a handshake failure must do.
type handshakeError struct {
	err error
}

// Error reports the handshake failure.
func (e *handshakeError) Error() string {
	return e.err.Error()
}

// Timeout reports no timeout: the failure is authentication, not time.
func (e *handshakeError) Timeout() bool {
	return false
}

// Temporary keeps http.Serve alive across handshake failures.
func (e *handshakeError) Temporary() bool {
	return true
}

// noiseListener wraps accepted TCP connections in the Noise responder
// handshake. Handshake failures close the connection and surface as temporary
// Accept errors, which makes http.Serve back off instead of exiting.
type noiseListener struct {
	net.Listener
	static StaticKey
	parcel []byte
	verify Verifier
	logger *zap.Logger
}

// Accept accepts one TCP connection and runs the Noise responder handshake.
func (l *noiseListener) Accept() (net.Conn, error) {
	raw, err := l.Listener.Accept()
	if err != nil {
		// Unwrapped on purpose: http.Serve detects backoff behavior through
		// a direct net.Error assertion, which wrapping would break.
		return nil, err //nolint:wrapcheck // see above
	}

	err = raw.SetDeadline(time.Now().Add(handshakeTimeout))
	if err != nil {
		_ = raw.Close() //nolint:errcheck // best-effort close on failed accept
		return nil, fmt.Errorf("set handshake deadline: %w", err)
	}

	conn, err := serveHandshake(raw, l.static, l.parcel, l.verify)
	if err != nil {
		_ = raw.Close() //nolint:errcheck // best-effort close on failed handshake
		l.logger.Warn("noise handshake failed", zap.Error(err))
		return nil, &handshakeError{err: err}
	}

	err = raw.SetDeadline(time.Time{})
	if err != nil {
		_ = raw.Close() //nolint:errcheck // best-effort close on failed accept
		return nil, fmt.Errorf("clear handshake deadline: %w", err)
	}

	return conn, nil
}
