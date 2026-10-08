// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import "time"

// trendWindow caps one series: 720 slots at the 5s capture beat is one
// hour. Oldest entries fall off; memory stays flat forever.
const trendWindow = 720

// trendPoint is one timestamped utilization reading. CPU and Mem are
// 0-100 percents; MemMB applies where the series tracks bytes, else zero.
type trendPoint struct {
	at    time.Time
	cpu   uint8
	mem   uint8
	memMB uint64
}

// trend is a fixed-size round-robin series: appends overwrite the oldest
// entry once full, so memory never grows. Not safe for concurrent use;
// the Sampler guards it.
type trend struct {
	pts  []trendPoint
	next int
	full bool
}

// newTrend creates an empty series holding up to trendWindow trendPoints.
func newTrend() *trend {
	return &trend{pts: make([]trendPoint, 0, trendWindow)}
}

// add appends a trendPoint, overwriting the oldest once full.
func (r *trend) add(p trendPoint) {
	if len(r.pts) < trendWindow {
		r.pts = append(r.pts, p)
		r.next = len(r.pts) % trendWindow
		return
	}
	r.pts[r.next] = p
	r.next = (r.next + 1) % trendWindow
	r.full = true
}

// len returns the trendPoints currently held.
func (r *trend) len() int {
	if r.full {
		return trendWindow
	}
	return len(r.pts)
}

// average folds the held trendPoints into mean CPU and memory percents plus
// the count folded. Empty trends report zeros.
func (r *trend) average() (cpu, mem uint8, n int) {
	n = r.len()
	if n <= 0 {
		return 0, 0, 0
	}
	var cpuSum, memSum uint64
	for _, p := range r.pts {
		cpuSum += uint64(p.cpu)
		memSum += uint64(p.mem)
	}
	cpuAvg := cpuSum / uint64(n)
	memAvg := memSum / uint64(n)
	if cpuAvg > 100 {
		cpuAvg = 100
	}
	if memAvg > 100 {
		memAvg = 100
	}
	return uint8(cpuAvg), uint8(memAvg), n
}
