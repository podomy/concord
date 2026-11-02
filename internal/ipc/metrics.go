// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ipc

import (
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

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
		s.logger.Error("self position members", logs.Error(err))
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
// Each HELP/TYPE header appears once per scrape; per-workload series are
// value lines under the shared header.
func renderMetrics(host node.HostPressure, lat, lon float64, workloads map[uuid.UUID]node.WorkloadSample) string {
	lines := []string{}
	lines = append(lines, gaugeLines("concord_node_cpu_percent", "Node CPU utilization percent.", percentString(host.CPU))...)
	lines = append(lines, gaugeLines("concord_node_mem_percent", "Node memory utilization percent.", percentString(host.Mem))...)
	lines = append(lines, gaugeLines("concord_node_disk_percent", "Node disk utilization percent.", percentString(host.Disk))...)
	lines = append(lines, gaugeLines("concord_node_latitude", "Node latitude in decimal degrees, 0 when unknown.", floatString(lat))...)
	lines = append(lines, gaugeLines("concord_node_longitude", "Node longitude in decimal degrees, 0 when unknown.", floatString(lon))...)
	ids := make([]uuid.UUID, 0, len(workloads))
	for id := range workloads {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	lines = append(lines, gaugeHeader("concord_workload_cpu_percent", "Workload CPU utilization percent.")...)
	for _, id := range ids {
		lines = append(lines, gaugeValue("concord_workload_cpu_percent", id, percentString(workloads[id].CPUPercent)))
	}
	lines = append(lines, gaugeHeader("concord_workload_mem_percent", "Workload memory utilization percent.")...)
	for _, id := range ids {
		lines = append(lines, gaugeValue("concord_workload_mem_percent", id, percentString(workloads[id].MemPercent)))
	}
	lines = append(lines, gaugeHeader("concord_workload_mem_usage_mb", "Workload memory usage in MB.")...)
	for _, id := range ids {
		lines = append(lines, gaugeValue("concord_workload_mem_usage_mb", id, strconv.FormatUint(workloads[id].MemUsageMB, 10)))
	}
	return strings.Join(lines, "\n") + "\n"
}

// gaugeLines renders one HELP/TYPE/value group for a bare node gauge.
func gaugeLines(name, help, value string) []string {
	return []string{
		"# HELP " + name + " " + help,
		"# TYPE " + name + " gauge",
		name + " " + value,
	}
}

// gaugeHeader renders one HELP/TYPE header pair for a metric.
func gaugeHeader(name, help string) []string {
	return []string{
		"# HELP " + name + " " + help,
		"# TYPE " + name + " gauge",
	}
}

// gaugeValue renders one labeled sample line for a workload metric.
func gaugeValue(name string, id uuid.UUID, value string) string {
	return name + `{id="` + id.String() + `"} ` + value
}

// percentString renders a 0-100 percent as decimal without fmt.
func percentString(v uint8) string {
	if v > 100 {
		v = 100
	}
	return strconv.FormatUint(uint64(v), 10)
}

// floatString renders a coordinate with microdegree precision.
func floatString(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}
