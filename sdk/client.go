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
	"strconv"
	"strings"
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

	// ErrValidation is returned when the daemon rejects a request as
	// invalid (HTTP 400). The wrapped message preserves the server text.
	ErrValidation = errors.New("workload validation failed")

	// ErrUnavailable is returned when the daemon reports 503: the sampler
	// backing Metrics or Trail is missing, so the reading cannot be
	// served. The wrapped message preserves the server text.
	ErrUnavailable = errors.New("service unavailable")

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

	// Stats returns live utilization for one running workload.
	Stats(ctx context.Context, id uuid.UUID) (*WorkloadStats, error)

	// Metrics returns sampler state in Prometheus text exposition format.
	Metrics(ctx context.Context) (string, error)

	// Trail returns our recorded positions since boot, oldest-first.
	// Bounded by the 720-point ring; full history lives in track.jsonl.
	Trail(ctx context.Context) ([]TrailPoint, error)

	// Logs returns one workload's captured output lines oldest-first.
	// Tail caps the result to the last N lines; zero or negative means all.
	Logs(ctx context.Context, id uuid.UUID, tail int) ([]LogLine, error)

	// UpdateImage bumps a live workload to a new image keeping its ID,
	// assignment, and history. The node restarts the container on the
	// new image; rollback is another bump.
	UpdateImage(ctx context.Context, id uuid.UUID, image string) error

	// SetPosition applies one live position fix: persisted for reboot,
	// gossiped to the fleet, and recorded to trail and track log.
	SetPosition(ctx context.Context, lat, lon float64) error

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

// validationError maps an HTTP 400 body to ErrValidation, preserving the
// server message. Non-400 errors stay unstructured: only validation has
// a typed sentinel, ErrNotFound keeps its own mapping where it exists.
func validationError(body []byte) error {
	var errResp errorResponse
	msg := strings.TrimSpace(string(body))
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
		msg = errResp.Error
	}
	return fmt.Errorf("%w: %s", ErrValidation, msg)
}

// notFoundError maps an HTTP 404 body to ErrNotFound, preserving the
// server message. Empty bodies keep the bare sentinel so errors.Is
// callers never depend on message text.
func notFoundError(body []byte) error {
	msg := bytes.TrimSpace(body)
	if len(msg) == 0 {
		return ErrNotFound //nolint:wrapcheck // sentinel error
	}
	return fmt.Errorf("%w: %s", ErrNotFound, string(msg))
}

// requestError maps a non-2xx status with its body to an error, routing
// 400 and 422 through ErrValidation, any 5xx through ErrUnavailable,
// and leaving every other status unstructured. All 5xx map to
// ErrUnavailable for consistency: every 5xx is a server-side failure
// the client cannot fix by resending the same bytes. 404 keeps its own
// ErrNotFound mapping at call sites that check it first.
func requestError(status int, body []byte) error {
	if status == http.StatusBadRequest || status == http.StatusUnprocessableEntity {
		return validationError(body)
	}
	if status >= 500 && status < 600 {
		return unavailableError(body)
	}
	return fmt.Errorf("server error (%d): %s", status, string(body))
}

// unavailableError maps an HTTP 503 body to ErrUnavailable, preserving
// the server message. Non-JSON bodies keep their trimmed text.
func unavailableError(body []byte) error {
	var errResp errorResponse
	msg := strings.TrimSpace(string(body))
	if json.Unmarshal(body, &errResp) == nil && errResp.Error != "" {
		msg = errResp.Error
	}
	return fmt.Errorf("%w: %s", ErrUnavailable, msg)
}

// listError maps a List or Nodes non-2xx status to a sentinel: 400 and
// 422 arrive as ErrValidation, 404 as ErrNotFound, any 5xx as
// ErrUnavailable, all preserving the server message. Other statuses
// stay unstructured so auth or rate limits never misreport.
func listError(status int, body []byte) error {
	if status == 400 || status == 422 {
		return validationError(body)
	}
	if status == http.StatusNotFound {
		return notFoundError(body)
	}
	if status >= 500 && status < 600 {
		return unavailableError(body)
	}
	return fmt.Errorf("server error (%d): %s", status, string(body))
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
		return uuid.Nil, requestError(resp.StatusCode, body)
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
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("%w: workload not found", ErrNotFound)
		}
		return notFoundError(body)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return fmt.Errorf("server error (%d)", resp.StatusCode)
		}

		return requestError(resp.StatusCode, body)
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
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("%w: workload not found", ErrNotFound)
		}
		return nil, notFoundError(body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, requestError(resp.StatusCode, body)
	}

	var w Workload
	err = json.Unmarshal(body, &w)
	if err != nil {
		return nil, fmt.Errorf("decode workload: %w", err)
	}

	return &w, nil
}

// Stats queries live utilization for one running workload by its UUID.
// Absent readings (never sampled, already stopped) report ErrNotFound.
func (c *unixClient) Stats(ctx context.Context, id uuid.UUID) (*WorkloadStats, error) {
	url := unixSocketHost + "/v1/workloads/" + id.String() + "/stats"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("stats request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	if resp.StatusCode == http.StatusNotFound {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("%w: workload not found", ErrNotFound)
		}
		return nil, notFoundError(body)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, requestError(resp.StatusCode, body)
	}

	var stats WorkloadStats
	err = json.Unmarshal(body, &stats)
	if err != nil {
		return nil, fmt.Errorf("decode stats: %w", err)
	}

	return &stats, nil
}

// Metrics returns sampler state in Prometheus text exposition format.
func (c *unixClient) Metrics(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, unixSocketHost+"/metrics", nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("metrics request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", requestError(resp.StatusCode, body)
	}

	return string(body), nil
}

// Trail returns our recorded positions oldest-first.
func (c *unixClient) Trail(ctx context.Context) ([]TrailPoint, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, unixSocketHost+"/v1/nodes/self/trail", nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("trail request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, requestError(resp.StatusCode, body)
	}

	var trailResp struct {
		Trail []TrailPoint `json:"trail"`
	}
	err = json.Unmarshal(body, &trailResp)
	if err != nil {
		return nil, fmt.Errorf("decode trail: %w", err)
	}

	return trailResp.Trail, nil
}

// Logs returns one workload's captured output lines oldest-first.
func (c *unixClient) Logs(ctx context.Context, id uuid.UUID, tail int) ([]LogLine, error) {
	url := unixSocketHost + "/v1/workloads/" + id.String() + "/logs"
	if tail > 0 {
		url += "?tail=" + strconv.Itoa(tail)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("logs request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return nil, notFoundError(body)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, requestError(resp.StatusCode, body)
	}

	var logsResp struct {
		Logs []LogLine `json:"logs"`
	}
	err = json.Unmarshal(body, &logsResp)
	if err != nil {
		return nil, fmt.Errorf("decode logs: %w", err)
	}

	return logsResp.Logs, nil
}

// UpdateImage bumps a live workload to a new image keeping its ID.
func (c *unixClient) UpdateImage(ctx context.Context, id uuid.UUID, image string) error {
	data, err := json.Marshal(ImageUpdate{Image: image}) //nolint:errchkjson // struct encoding never fails, checked for symmetry
	if err != nil {
		return fmt.Errorf("marshal image update: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, unixSocketHost+"/v1/workloads/"+id.String()+"/image", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("image update request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode == http.StatusNotFound {
		return notFoundError(body)
	}
	if resp.StatusCode != http.StatusOK {
		return requestError(resp.StatusCode, body)
	}

	return nil
}

// SetPosition applies one live position fix on the local daemon.
func (c *unixClient) SetPosition(ctx context.Context, lat, lon float64) error {
	data, err := json.Marshal(Position{Lat: lat, Lon: lon})
	if err != nil {
		return fmt.Errorf("marshal position: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, unixSocketHost+"/v1/nodes/self/position", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("position request failed: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort response close

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return requestError(resp.StatusCode, body)
	}

	return nil
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
		return nil, listError(resp.StatusCode, body)
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
		return nil, listError(resp.StatusCode, body)
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
