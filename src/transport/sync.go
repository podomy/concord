// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package transport

import (
	"encoding/json"
	"net/http"

	"github.com/podomy/concord/src/journal"
)

type SyncRequest struct {
	Watermark string `json:"watermark"`
	Limit     int    `json:"limit"`
}

type SyncResponse struct {
	NextWatermark string          `json:"next_watermark"`
	Events        []journal.Event `json:"events"`
}

func postSync(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MiB cap

	var req SyncRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "sync failed", http.StatusBadRequest)
		return
	}

	// Stub: real journal pull/merge comes later. Keep the wire contract stable.
	resp := SyncResponse{
		Events:        []journal.Event{},
		NextWatermark: req.Watermark,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, "sync failed", http.StatusInternalServerError)
		return
	}
}
