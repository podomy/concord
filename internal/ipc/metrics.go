// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package ipc

import (
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"

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
	body := renderMetrics(s.sampler.LastHost(), s.sampler.Workloads())
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(body)) //nolint:errcheck // best-effort metrics write
}

// renderMetrics formats host and per-workload readings as Prometheus
// exposition text. Workload ids sort ascending so output is stable across
// scrapes; ids are hex UUIDs, safe unquoted in labels.
func renderMetrics(host node.HostPressure, workloads map[uuid.UUID]node.WorkloadSample) string {
	lines := []string{}
	lines = append(lines, gaugeLines("concord_node_cpu_percent", "Node CPU utilization percent.", percentString(host.CPU), "")...)
	lines = append(lines, gaugeLines("concord_node_mem_percent", "Node memory utilization percent.", percentString(host.Mem), "")...)
	lines = append(lines, gaugeLines("concord_node_disk_percent", "Node disk utilization percent.", percentString(host.Disk), "")...)
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
