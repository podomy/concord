// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ipc

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/workload"
	"github.com/podomy/concord/sdk"
)

// registerRoutes mounts the REST API endpoints onto the HTTP server mux.
func (s *Server) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/workloads", s.handleSubmitWorkload)
	mux.HandleFunc("GET /v1/workloads", s.handleListWorkloads)
	mux.HandleFunc("GET /v1/workloads/{id}", s.handleGetWorkload)
	mux.HandleFunc("DELETE /v1/workloads/{id}", s.handleDeleteWorkload)
	mux.HandleFunc("GET /v1/nodes", s.handleListNodes)
}

// Response Types & JSON Helpers
type submitResponse struct {
	ID uuid.UUID `json:"id"`
}

type listResponse struct {
	Workloads []sdk.Workload `json:"workloads"`
}

type nodesResponse struct {
	Nodes []sdk.Node `json:"nodes"`
}

type errorResponse struct {
	Error string `json:"error"`
}

// writeJSON writes a JSON-encoded response body with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	data, err := json.Marshal(v)
	if err != nil {
		http.Error(w, `{"error":"internal json encoding error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data) //nolint:errcheck // response header already committed
}

// writeError writes a standardized JSON error response.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}

// HTTP Handlers
// handleSubmitWorkload processes incoming workload submissions:
// 1. Decodes and validates the public SDK workload specification.
// 2. Assigns a unique workload ID if one was not provided.
// 3. Translates the specification into Concord's internal workload.Spec.
// 4. Appends a "workload.spec" event to the distributed journal and applies it to views.
// 5. Returns 201 Created with the assigned workload ID.
func (s *Server) handleSubmitWorkload(w http.ResponseWriter, r *http.Request) {
	var input sdk.Workload
	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: "+err.Error())
		return
	}

	if strings.TrimSpace(input.Image) == "" {
		writeError(w, http.StatusBadRequest, "workload image reference is required")
		return
	}

	if input.ID == uuid.Nil {
		input.ID = uuid.New()
	}

	spec := sdkToInternalWorkload(input)
	payload, err := json.Marshal(spec)
	if err != nil {
		s.logger.Error("failed to marshal workload spec", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to marshal workload spec")
		return
	}

	event := journal.NewEvent(s.nodeID, "workload.spec", payload)
	err = journalview.RecordEvent(r.Context(), s.journal, s.views, event)
	if err != nil {
		s.logger.Error("failed to commit workload event to journal", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to commit workload: "+err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, submitResponse{ID: spec.ID})
}

// handleListWorkloads returns all currently active (non-tombstoned) workloads in the cluster.
func (s *Server) handleListWorkloads(w http.ResponseWriter, r *http.Request) {
	specs, err := s.workloads.List(r.Context())
	if err != nil {
		s.logger.Error("failed to list workloads from view", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to list workloads: "+err.Error())
		return
	}

	active := make([]sdk.Workload, 0, len(specs))
	for _, spec := range specs {
		if spec.Removed {
			continue
		}
		active = append(active, internalToSDKWorkload(spec))
	}

	writeJSON(w, http.StatusOK, listResponse{Workloads: active})
}

// handleGetWorkload retrieves the specification of a specific workload by its unique UUID.
// Returns 404 Not Found if the workload does not exist or has been removed.
func (s *Server) handleGetWorkload(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid workload id: "+idStr)
		return
	}

	spec, err := s.workloads.Get(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to get workload from view", zap.Error(err), zap.String("id", idStr))
		writeError(w, http.StatusInternalServerError, "failed to get workload: "+err.Error())
		return
	}

	if spec == nil || spec.Removed {
		writeError(w, http.StatusNotFound, "workload not found")
		return
	}

	writeJSON(w, http.StatusOK, internalToSDKWorkload(*spec))
}

// handleDeleteWorkload marks a workload as removed by appending a tombstone event to the journal:
// 1. Verifies the workload exists and is not already removed.
// 2. Creates a tombstone spec with Removed = true.
// 3. Appends the tombstone event to the journal and updates materialized views.
// 4. Returns 204 No Content upon success.
func (s *Server) handleDeleteWorkload(w http.ResponseWriter, r *http.Request) {
	idStr := r.PathValue("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid workload id: "+idStr)
		return
	}

	spec, err := s.workloads.Get(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to get workload for deletion", zap.Error(err), zap.String("id", idStr))
		writeError(w, http.StatusInternalServerError, "failed to get workload: "+err.Error())
		return
	}

	if spec == nil || spec.Removed {
		writeError(w, http.StatusNotFound, "workload not found")
		return
	}

	tombstone := *spec
	tombstone.Removed = true

	payload, err := json.Marshal(tombstone)
	if err != nil {
		s.logger.Error("failed to marshal tombstone spec", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to marshal tombstone")
		return
	}

	event := journal.NewEvent(s.nodeID, "workload.spec", payload)
	err = journalview.RecordEvent(r.Context(), s.journal, s.views, event)
	if err != nil {
		s.logger.Error("failed to commit tombstone event to journal", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to commit tombstone: "+err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// handleListNodes queries the peer discovery service for all known cluster nodes.
func (s *Server) handleListNodes(w http.ResponseWriter, _ *http.Request) {
	if s.peerService == nil {
		writeJSON(w, http.StatusOK, nodesResponse{Nodes: []sdk.Node{}})
		return
	}

	members, err := s.peerService.Members()
	if err != nil {
		s.logger.Error("failed to query peer members", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to list nodes: "+err.Error())
		return
	}

	nodes := make([]sdk.Node, 0, len(members))
	for _, m := range members {
		nodes = append(nodes, sdk.Node{
			ID:                 m.ID,
			Address:            m.Address.String(),
			State:              string(m.State),
			WireGuardPublicKey: m.Metadata.WireGuardPublicKey,
		})
	}

	writeJSON(w, http.StatusOK, nodesResponse{Nodes: nodes})
}

// Model Translation (SDK <-> Internal Runtime)
// sdkToInternalWorkload translates a public sdk.Workload into Concord's internal workload.Spec.
func sdkToInternalWorkload(w sdk.Workload) workload.Spec {
	return workload.Spec{
		ID:                 w.ID,
		Image:              w.Image,
		Command:            w.Command,
		Env:                w.Env,
		Resources:          workload.Resources{CPUShares: w.Resources.CPUShares, MemoryMB: w.Resources.MemoryMB},
		Restart:            workload.RestartPolicy(w.Restart),
		HostPort:           w.HostPort,
		ContainerPort:      w.ContainerPort,
		StopTimeoutSeconds: w.StopTimeoutSeconds,
		HealthAction:       workload.HealthAction(w.HealthAction),
		HealthPath:         w.HealthPath,
	}
}

// internalToSDKWorkload translates an internal workload.Spec into a public sdk.Workload.
func internalToSDKWorkload(s workload.Spec) sdk.Workload {
	return sdk.Workload{
		ID:                 s.ID,
		Image:              s.Image,
		Command:            s.Command,
		Env:                s.Env,
		Resources:          sdk.Resources{CPUShares: s.Resources.CPUShares, MemoryMB: s.Resources.MemoryMB},
		Restart:            sdk.RestartPolicy(s.Restart),
		HostPort:           s.HostPort,
		ContainerPort:      s.ContainerPort,
		StopTimeoutSeconds: s.StopTimeoutSeconds,
		HealthAction:       sdk.HealthAction(s.HealthAction),
		HealthPath:         s.HealthPath,
	}
}
