// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ipc

import (
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/node"
)

// handleMetrics serves sampler state in Prometheus text exposition format.
// Anything that scrapes can collect it; the format is the standard, no
// Concord-specific protocol is invented. Values are point readings from
// memory, never journal history.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	if s.sampler == nil {
		http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		return
	}
	lat, lon := s.selfPosition()
	body := renderMetrics(s.sampler.LastHost(), lat, lon, s.sampler.Workloads())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(body)) //nolint:errcheck // best-effort metrics write
}

// selfPosition returns our gossiped coordinates for trail gauges, or zeros
// when unknown. It reads our own memberlist entry: the same bytes every
// peer sees, so scraped trails match the fleet map exactly.
func (s *Server) selfPosition() (float64, float64) {
	if s.peerService == nil {
		return 0, 0
	}
	members, err := s.peerService.Members()
	if err != nil {
		s.logger.Error("self position members", zap.Error(err))
		return 0, 0
	}
	for _, m := range members {
		if m.ID == s.nodeID {
			return m.Metadata.Lat, m.Metadata.Lon
		}
	}
	return 0, 0
}

// renderMetrics formats host and per-workload readings as Prometheus
// exposition text. Workload ids sort ascending so output is stable across
// scrapes; ids are hex UUIDs, safe unquoted in labels. Position gauges let
// scrapers keep trails; zeros mean unknown, matching the gossip convention.
func renderMetrics(host node.HostPressure, lat, lon float64, workloads map[uuid.UUID]node.WorkloadSample) string {
	lines := []string{}
	lines = append(lines, gaugeLines("concord_node_cpu_percent", "Node CPU utilization percent.", percentString(host.CPU), "")...)
	lines = append(lines, gaugeLines("concord_node_mem_percent", "Node memory utilization percent.", percentString(host.Mem), "")...)
	lines = append(lines, gaugeLines("concord_node_disk_percent", "Node disk utilization percent.", percentString(host.Disk), "")...)
	lines = append(lines, gaugeLines("concord_node_latitude", "Node latitude in decimal degrees, 0 when unknown.", floatString(lat), "")...)
	lines = append(lines, gaugeLines("concord_node_longitude", "Node longitude in decimal degrees, 0 when unknown.", floatString(lon), "")...)
	ids := make([]uuid.UUID, 0, len(workloads))
	for id := range workloads {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		sample := workloads[id]
		label := `{id="` + id.String() + `"}`
		lines = append(lines, gaugeLines("concord_workload_cpu_percent", "Workload CPU utilization percent.", percentString(sample.CPUPercent), label)...)
		lines = append(lines, gaugeLines("concord_workload_mem_percent", "Workload memory utilization percent.", percentString(sample.MemPercent), label)...)
		lines = append(lines, gaugeLines("concord_workload_mem_usage_mb", "Workload memory usage in MB.", strconv.FormatUint(sample.MemUsageMB, 10), label)...)
	}
	return strings.Join(lines, "\n") + "\n"
}

// gaugeLines renders one HELP/TYPE/value group. An empty label writes a
// bare gauge, otherwise the label rides on the value line.
func gaugeLines(name, help, value, label string) []string {
	head := name + " " + value
	if label != "" {
		head = name + label + " " + value
	}
	return []string{
		"# HELP " + name + " " + help,
		"# TYPE " + name + " gauge",
		head,
	}
}

// percentString renders a 0-100 percent as decimal without fmt.
func percentString(v uint8) string {
	return strconv.FormatUint(uint64(v), 10)
}

// floatString renders a coordinate with microdegree precision.
func floatString(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}
