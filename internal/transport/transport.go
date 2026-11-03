// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/podomy/concord/internal/logs"
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
	logger *logs.Logger,
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
		serveSyncPage(w, r, index, logger)
	})

	srv := newTransportServer(mux)

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
	// so Shutdown can finish in-flight requests within the timeout. The
	// bound covers one full Noise handshake plus margin: shorter would
	// abandon stalled handshakes to linger past shutdown.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Error("transport shutdown", logs.Error(err))
		}
	}()

	// Serve in background. Serve blocks until shutdown closes the listener.
	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			logger.Error("serve noise", logs.Error(err))
		}
	}()

	return nil
}

// serverReadHeaderTimeout bounds first-request headers. Shared by the
// HTTP server config and the post-handshake re-arm so the current Read
// stays bounded after the handshake deadline clears.
const serverReadHeaderTimeout = 5 * time.Second

// newTransportServer builds the node-to-node HTTP server over Noise.
// IdleTimeout mirrors the registry at 120s: every transport connection
// is Noise-authenticated, so an idle keep-alive pins an authenticated
// session and must be reaped instead of held open indefinitely.
func newTransportServer(handler http.Handler) *http.Server {
	return &http.Server{
		Addr:    ":" + Port,
		Handler: handler,
		// Header bound stays short; body and response bounds below are
		// frozen at 10s. A large-journal fallback scan (full file walk
		// on index miss or unknown cursor) runs inside postSync under
		// these timeouts, so a huge journal without a warm index can
		// hit WriteTimeout and fail the page instead of stalling the
		// connection. The offset index exists to keep the common path
		// a seek plus a bounded page; timeout values stay frozen.
		ReadHeaderTimeout: serverReadHeaderTimeout,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// buildSyncIndex runs the one-time offset backfill and logs the outcome.
// A failed build only costs full scans; every request still serves.
func buildSyncIndex(ctx context.Context, logger *logs.Logger, index *offsetIndex) {
	err := index.build(ctx)
	if err != nil {
		logger.Warn("sync index build failed", logs.Error(err))
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

// Unwrap preserves the cause chain for errors.Is and errors.As. Error
// stays flattened for logs while matching still reaches the wrapped cause.
func (e *handshakeError) Unwrap() error {
	return e.err
}

// Timeout reports no timeout: the failure is authentication, not time.
func (e *handshakeError) Timeout() bool {
	return false
}

// Temporary keeps http.Serve alive across handshake failures.
func (e *handshakeError) Temporary() bool {
	return true
}

// noiseListener wraps accepted TCP connections so the Noise responder
// handshake runs in the per-connection serve path, not on the Accept path.
// Accept returns the raw conn fast with the handshake deadline preset; the
// first Read or Write runs the handshake before any HTTP bytes flow. A
// stalled peer therefore occupies one serve goroutine instead of the
// listener, so honest peers proceed while it waits out the deadline.
// Handshake failures close the conn and surface as read/write errors on
// that conn, which the server logs with the remote addr and a static
// failure class. The handshakeError type stays for failure classification:
// its Temporary/Unwrap contract is unchanged, but it no longer drives
// http.Serve backoff, since Accept itself never blocks on a handshake.
//
// TLA drift note (model internal/noise/tla/ik.tla, no model edits): the
// model runs the handshake inline; deferring it past Accept intentionally
// exceeds the model. Observable wire behavior is unchanged: no HTTP byte
// moves before the handshake completes, and the deadline still bounds it.
// One deliberate tradeoff: the handshake clears the preset deadline on
// success, which also clears any http.Server read deadline set before the
// first Read. Server timeouts re-apply to later I/O on that conn.
type noiseListener struct {
	net.Listener
	static StaticKey
	parcel []byte
	verify Verifier
	logger *logs.Logger
}

// Accept accepts one TCP connection and presets the handshake deadline.
// The handshake itself runs on first I/O in the serving goroutine.
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

	return &deferredNoiseConn{
		raw:    raw,
		static: l.static,
		parcel: l.parcel,
		verify: l.verify,
		logger: l.logger,
	}, nil
}

// deferredNoiseConn runs the Noise responder handshake on first I/O and
// then behaves as the established session. It exists so Accept stays
// fast: http.Server serves each accepted conn in its own goroutine, so the
// blocking handshake happens there instead of serializing the listener.
type deferredNoiseConn struct {
	raw    net.Conn
	static StaticKey
	parcel []byte
	verify Verifier
	logger *logs.Logger

	once         sync.Once
	conn         *noiseConn
	handshakeErr error
}

// ensure runs the responder handshake once and reports its outcome. The
// deadline was preset by Accept; success re-arms the header bound so the
// current Read stays bounded while http.Server manages the rest. Failure
// closes the raw conn and logs the remote addr with a static failure
// class, never peer bytes.
func (c *deferredNoiseConn) ensure() error {
	c.once.Do(func() {
		conn, err := serveHandshake(c.raw, c.static, c.parcel, c.verify)
		if err != nil {
			remote := c.raw.RemoteAddr()
			addr := ""
			if remote != nil {
				addr = remote.String()
			}
			c.logger.Warn("noise handshake failed",
				logs.String("remote_addr", addr),
				logs.String("failure_class", classifyHandshakeError(err)),
				logs.Error(err),
			)
			_ = c.raw.Close() //nolint:errcheck // failed handshake, nothing to report
			c.handshakeErr = &handshakeError{err: err}
			return
		}

		err = c.raw.SetDeadline(time.Now().Add(serverReadHeaderTimeout))
		if err != nil {
			_ = c.raw.Close() //nolint:errcheck // best-effort close on failed accept
			c.handshakeErr = fmt.Errorf("re-arm header deadline: %w", err)
			return
		}

		c.conn = conn
	})

	return c.handshakeErr
}

// Read handshakes on first use, then returns decrypted stream bytes.
func (c *deferredNoiseConn) Read(p []byte) (int, error) {
	if err := c.ensure(); err != nil {
		return 0, err
	}

	n, err := c.conn.Read(p)
	if err != nil {
		return n, err
	}

	return n, nil
}

// Write handshakes on first use, then sends encrypted stream bytes.
func (c *deferredNoiseConn) Write(p []byte) (int, error) {
	if err := c.ensure(); err != nil {
		return 0, err
	}

	n, err := c.conn.Write(p)
	if err != nil {
		return n, err
	}

	return n, nil
}

// Close closes the underlying connection.
func (c *deferredNoiseConn) Close() error {
	err := c.raw.Close()
	if err != nil {
		return fmt.Errorf("close deferred noise conn: %w", err)
	}

	return nil
}

// LocalAddr returns the underlying local address.
func (c *deferredNoiseConn) LocalAddr() net.Addr {
	return c.raw.LocalAddr()
}

// RemoteAddr returns the underlying remote address.
func (c *deferredNoiseConn) RemoteAddr() net.Addr {
	return c.raw.RemoteAddr()
}

// SetDeadline delegates to the underlying connection.
func (c *deferredNoiseConn) SetDeadline(t time.Time) error {
	err := c.raw.SetDeadline(t)
	if err != nil {
		return fmt.Errorf("set deadline: %w", err)
	}

	return nil
}

// SetReadDeadline delegates to the underlying connection.
func (c *deferredNoiseConn) SetReadDeadline(t time.Time) error {
	err := c.raw.SetReadDeadline(t)
	if err != nil {
		return fmt.Errorf("set read deadline: %w", err)
	}

	return nil
}

// SetWriteDeadline delegates to the underlying connection.
func (c *deferredNoiseConn) SetWriteDeadline(t time.Time) error {
	err := c.raw.SetWriteDeadline(t)
	if err != nil {
		return fmt.Errorf("set write deadline: %w", err)
	}

	return nil
}
