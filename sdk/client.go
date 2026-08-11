// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
)

const (
	// unixSocketHost is a dummy URL scheme and hostname required by net/http request constructors.
	// The custom DialContext transport intercepts this host and routes all traffic directly into the Unix socket.
	unixSocketHost = "http://unix"
)

var (
	// ErrNotFound is returned when a requested workload is not found in the cluster.
	ErrNotFound = errors.New("workload not found")

	// ErrDaemonUnreachable is returned when the Unix domain socket cannot be reached.
	ErrDaemonUnreachable = errors.New("concord daemon unreachable; ensure the daemon is running")
)

// DefaultSocketPath returns the standard Unix domain socket path for Concord.
func DefaultSocketPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config dir: %w", err)
	}

	return filepath.Join(dir, "concord", "concord.sock"), nil
}

// Client provides an interface for interacting with a Concord node.
type Client interface {
	// Submit submits a workload specification to be committed to the cluster journal.
	Submit(ctx context.Context, w Workload) (uuid.UUID, error)

	// Stop stops and removes a workload by committing a tombstone event.
	Stop(ctx context.Context, id uuid.UUID) error

	// Get retrieves a workload specification by its unique ID.
	Get(ctx context.Context, id uuid.UUID) (*Workload, error)

	// List returns all active workloads currently recorded in the cluster view.
	List(ctx context.Context) ([]Workload, error)

	// Nodes lists all known cluster nodes and their network status.
	Nodes(ctx context.Context) ([]Node, error)

	// Close closes the client transport.
	Close() error
}

type unixClient struct {
	httpClient *http.Client
	socketPath string
}

// Dial creates a new Concord SDK client connected to the local daemon's Unix socket.
// If socketPath is omitted, it defaults to ~/.config/concord/concord.sock.
func Dial(socketPath ...string) (Client, error) {
	var path string
	if len(socketPath) > 0 && socketPath[0] != "" {
		path = socketPath[0]
	} else {
		defaultPath, err := DefaultSocketPath()
		if err != nil {
			return nil, err
		}
		path = defaultPath
	}

	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			conn, err := d.DialContext(ctx, "unix", path)
			if err != nil {
				return nil, fmt.Errorf("%w: %w", ErrDaemonUnreachable, err)
			}

			return conn, nil
		},
		DisableKeepAlives: true,
	}

	return &unixClient{
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second,
		},
		socketPath: path,
	}, nil
}

type submitResponse struct {
	ID    uuid.UUID `json:"id"`
	Error string    `json:"error,omitempty"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// Submit sends a workload specification to the Concord daemon to commit to the distributed journal.
func (c *unixClient) Submit(ctx context.Context, w Workload) (uuid.UUID, error) {
	data, err := json.Marshal(w)
	if err != nil {
		return uuid.Nil, fmt.Errorf("marshal workload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, unixSocketHost+"/v1/workloads", bytes.NewReader(data))
	if err != nil {
		return uuid.Nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return uuid.Nil, fmt.Errorf("submit request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return uuid.Nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		var errResp errorResponse
		jsonErr := json.Unmarshal(body, &errResp)
		if jsonErr == nil && errResp.Error != "" {
			return uuid.Nil, fmt.Errorf("server rejected submission: %s", errResp.Error)
		}

		return uuid.Nil, fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
	}

	var submitResp submitResponse
	err = json.Unmarshal(body, &submitResp)
	if err != nil {
		return uuid.Nil, fmt.Errorf("decode response: %w", err)
	}

	return submitResp.ID, nil
}

// Stop sends a deletion tombstone for a workload to be broadcasted across the cluster mesh.
func (c *unixClient) Stop(ctx context.Context, id uuid.UUID) error {
	url := unixSocketHost + "/v1/workloads/" + id.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("stop request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound //nolint:wrapcheck // sentinel error
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("server error (%d)", resp.StatusCode)
		}

		return fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// Get queries the cluster state for a specific workload by its UUID.
func (c *unixClient) Get(ctx context.Context, id uuid.UUID) (*Workload, error) {
	url := unixSocketHost + "/v1/workloads/" + id.String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrNotFound //nolint:wrapcheck // sentinel error
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
	}

	var w Workload
	err = json.Unmarshal(body, &w)
	if err != nil {
		return nil, fmt.Errorf("decode workload: %w", err)
	}

	return &w, nil
}

type listResponse struct {
	Workloads []Workload `json:"workloads"`
	Error     string     `json:"error,omitempty"`
}

// List returns all active workloads currently recorded in the cluster view.
//
//nolint:dupl // List and Nodes perform distinct typed endpoint unmarshaling.
func (c *unixClient) List(ctx context.Context) ([]Workload, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, unixSocketHost+"/v1/workloads", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("list request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
	}

	var listResp listResponse
	err = json.Unmarshal(body, &listResp)
	if err != nil {
		return nil, fmt.Errorf("decode workload list: %w", err)
	}

	return listResp.Workloads, nil
}

type nodesResponse struct {
	Nodes []Node `json:"nodes"`
	Error string `json:"error,omitempty"`
}

// Nodes queries all known cluster peer nodes and their WireGuard mesh metadata.
//
//nolint:dupl // List and Nodes perform distinct typed endpoint unmarshaling.
func (c *unixClient) Nodes(ctx context.Context) ([]Node, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, unixSocketHost+"/v1/nodes", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("nodes request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server error (%d): %s", resp.StatusCode, string(body))
	}

	var nResp nodesResponse
	err = json.Unmarshal(body, &nResp)
	if err != nil {
		return nil, fmt.Errorf("decode nodes list: %w", err)
	}

	return nResp.Nodes, nil
}

// Close releases idle connections in the underlying HTTP client.
func (c *unixClient) Close() error {
	c.httpClient.CloseIdleConnections()

	return nil
}
