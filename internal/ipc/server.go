// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/sdk"
)

var (
	// ErrAlreadyRunning indicates the IPC server has already been started.
	ErrAlreadyRunning = errors.New("ipc server is already running")

	// ErrShutdown indicates the server failed to cleanly shut down all resources.
	ErrShutdown = errors.New("ipc server shutdown encountered errors")
)

// Server coordinates the local Unix domain socket IPC HTTP API for the Concord daemon.
// It bridges SDK client requests (such as submitting or stopping workloads) with
// Concord's persistent journal, materialized views, and cluster membership.
type Server struct {
	nodeID      uuid.UUID
	journal     journal.Journal
	views       []journalview.View
	workloads   *journalview.Workloads
	peerService *peerdiscovery.MemberService
	logger      *zap.Logger

	mu         sync.Mutex
	httpServer *http.Server
	listener   net.Listener
	socketPath string
	closed     bool
}

// NewServer constructs an unstarted IPC server instance.
func NewServer(
	nodeID uuid.UUID,
	j journal.Journal,
	views []journalview.View,
	workloads *journalview.Workloads,
	peerService *peerdiscovery.MemberService,
	logger *zap.Logger,
) *Server {
	if logger == nil {
		logger = zap.NewNop()
	}

	return &Server{
		nodeID:      nodeID,
		journal:     j,
		views:       views,
		workloads:   workloads,
		peerService: peerService,
		logger:      logger.Named("ipc"),
	}
}

// Start opens the Unix domain socket at socketPath and begins serving incoming IPC requests:
// 1. Resolves default socket path (~/.config/concord/concord.sock) if none provided.
// 2. Creates parent directories with 0700 permissions.
// 3. Cleans up any stale socket files from prior process runs.
// 4. Binds listener and restricts socket permissions to owner-only (0600).
// 5. Mounts REST handlers and starts serving HTTP in the background.
// 6. Listens on context cancellation for automatic graceful shutdown.
func (s *Server) Start(ctx context.Context, socketPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener != nil {
		return fmt.Errorf("%w", ErrAlreadyRunning)
	}

	if socketPath == "" {
		defaultPath, err := sdk.DefaultSocketPath()
		if err != nil {
			return fmt.Errorf("resolve default socket path: %w", err)
		}
		socketPath = defaultPath
	}

	socketDir := filepath.Dir(socketPath)
	if err := os.MkdirAll(socketDir, 0o700); err != nil {
		return fmt.Errorf("create socket directory %s: %w", socketDir, err)
	}

	// Clean up any stale socket file left from previous process termination.
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		s.logger.Debug("cleaning up old socket file", zap.Error(err))
	}

	var lc net.ListenConfig
	l, err := lc.Listen(ctx, "unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on unix socket %s: %w", socketPath, err)
	}

	// Restrict permissions to owner-only access.
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = l.Close() //nolint:errcheck // error during cleanup after failed chmod
		_ = os.Remove(socketPath)
		return fmt.Errorf("chmod socket file: %w", err)
	}

	mux := http.NewServeMux()
	s.registerRoutes(mux)

	s.httpServer = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	s.listener = l
	s.socketPath = socketPath
	s.closed = false

	s.logger.Info("local ipc server listening", zap.String("socket", socketPath))

	// Background serve loop.
	go func() {
		if err := s.httpServer.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			s.logger.Error("ipc http serve failed", zap.Error(err))
		}
	}()

	// Monitor context cancellation for graceful shutdown.
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer cancel()
		_ = s.Shutdown(shutdownCtx) //nolint:errcheck // best-effort shutdown on context cancellation
	}()

	return nil
}

// Shutdown gracefully stops the HTTP server, closes the listener, and unlinks the Unix socket file.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}
	s.closed = true

	var errs []error

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errs = append(errs, fmt.Errorf("http server shutdown: %w", err))
		}
	}

	if s.listener != nil {
		if err := s.listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			errs = append(errs, fmt.Errorf("close socket listener: %w", err))
		}
	}

	if s.socketPath != "" {
		if err := os.Remove(s.socketPath); err != nil && !os.IsNotExist(err) {
			errs = append(errs, fmt.Errorf("remove socket file %s: %w", s.socketPath, err))
		}
	}

	s.logger.Info("local ipc server stopped")

	if len(errs) > 0 {
		return fmt.Errorf("%w: %w", ErrShutdown, errors.Join(errs...))
	}

	return nil
}
