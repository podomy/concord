// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"bufio"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/geo"
)

// ErrNoCPULine indicates that the aggregate cpu line could not be found in /proc/stat.
var ErrNoCPULine = errors.New("cpu line not found in /proc/stat")

// HostPressure holds node utilization as 0-100 percents.
type HostPressure struct {
	CPU  uint8
	Mem  uint8
	Disk uint8
}

// WorkloadSample holds per-workload utilization. MemPercent is 0 when the
// workload has no memory limit; MemUsageMB always reports.
type WorkloadSample struct {
	CPUPercent uint8
	MemPercent uint8
	MemUsageMB uint64
}

// cpuTimes holds one /proc/stat cpu line split into idle and total jiffies.
type cpuTimes struct {
	idle  uint64
	total uint64
}

// workloadPrev holds the previous cumulative reading for one workload.
type workloadPrev struct {
	cpuTotal uint64
	at       time.Time
}

// Sampler turns cumulative host and cgroup counters into percents. It keeps
// the previous readings, so the first sample of anything reports CPU 0 and
// only later samples carry deltas. It is safe for concurrent use: the fast
// beat writes while inspection endpoints read.
type Sampler struct {
	mu         sync.Mutex
	lastCPU    cpuTimes
	haveHost   bool
	last       HostPressure
	prev       map[uuid.UUID]workloadPrev
	samples    map[uuid.UUID]WorkloadSample
	workTrends map[uuid.UUID]*trend
	path       *trail
	// track is the durable trail sink. Nil in tests and when the log
	// cannot open; RecordPosition then keeps memory only.
	track *TrackLog
	// lastFix is the latest valid position handed to RecordPosition,
	// gated or not. Anchor ordering reads this, not the trail: the trail
	// skips sub-10m jitter while ordering wants the freshest fix.
	lastFix geo.Point
	haveFix bool
}

// NewSampler creates an empty pressure sampler.
func NewSampler() *Sampler {
	return &Sampler{
		prev:       make(map[uuid.UUID]workloadPrev),
		samples:    make(map[uuid.UUID]WorkloadSample),
		workTrends: make(map[uuid.UUID]*trend),
		path:       newTrail(),
	}
}

// SampleHost reads /proc/stat, /proc/meminfo, and disk usage, and returns
// the node trio. The first call establishes the CPU baseline and reports
// CPU 0; memory and disk report immediately.
func (s *Sampler) SampleHost() (HostPressure, error) {
	times, err := readCPUTimes()
	if err != nil {
		return HostPressure{}, err
	}
	mem, err := readMemPercent()
	if err != nil {
		return HostPressure{}, err
	}
	disk, err := readDiskPercent()
	if err != nil {
		return HostPressure{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	pressure := HostPressure{Mem: mem, Disk: disk}
	if s.haveHost {
		pressure.CPU = cpuPercent(s.lastCPU, times)
	}
	s.lastCPU = times
	s.haveHost = true
	s.last = pressure
	return pressure, nil
}

// LastHost returns the most recently sampled trio, or zeros before the
// first sample.
func (s *Sampler) LastHost() HostPressure {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

// SampleWorkload records cumulative cgroup counters for one workload and
// returns its utilization. cpuTotal is nanoseconds of CPU consumed,
// memUsageBytes and memLimitBytes come from cgroup memory stats and the
// spec. The first call for an id reports CPU 0. A counter going backwards
// (cgroup recreated) also reports 0 instead of wrapping.
func (s *Sampler) SampleWorkload(id uuid.UUID, cpuTotal, memUsageBytes, memLimitBytes uint64, now time.Time) WorkloadSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	sample := WorkloadSample{MemUsageMB: memUsageBytes / (1024 * 1024)}
	if memLimitBytes > 0 {
		sample.MemPercent = percentOf(memUsageBytes, memLimitBytes)
	}
	if prev, ok := s.prev[id]; ok && now.After(prev.at) && cpuTotal >= prev.cpuTotal {
		ns := now.Sub(prev.at).Nanoseconds()
		if ns > 0 {
			// Nanoseconds of CPU over nanoseconds of wall clock: over 100
			// on multi-core, so clamp to the percent range.
			sample.CPUPercent = percentOf(cpuTotal-prev.cpuTotal, uint64(ns))
		}
	}
	s.prev[id] = workloadPrev{cpuTotal: cpuTotal, at: now}
	s.samples[id] = sample
	return sample
}

// DropWorkload forgets a workload's counters, latest sample, and trend.
func (s *Sampler) DropWorkload(id uuid.UUID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.prev, id)
	delete(s.samples, id)
	delete(s.workTrends, id)
}

// RecordPosition appends to the position trail when the node moved at
// least trailMoveKM since the last recorded point. The first valid
// position always records. Invalid positions never record, and neither
// does the exact zero point: zeros mean unknown across gossip, gauges,
// and node list, so recording them would plant a Null Island waypoint
// no node ever visited. Gated points land in the ring and the track log
// together, so the file always holds everything the ring does and more.
func (s *Sampler) RecordPosition(pos geo.Point) {
	if !pos.Valid() || (pos.Lat == 0 && pos.Lon == 0) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFix = pos
	s.haveFix = true
	if last, ok := s.path.last(); ok && geo.DistanceKM(last.Pos, pos) < trailMoveKM {
		return
	}
	now := time.Now()
	s.path.add(trailPoint{At: now, Pos: pos})
	if s.track != nil {
		// Best-effort: the track log is an audit trail, not coordination.
		// A failed append (full disk) must not break gossip or the ring;
		// disk trouble already surfaces loudly through journal appends.
		_ = s.track.Append(now, pos.Lat, pos.Lon) //nolint:errcheck // best-effort audit write
	}
}

// LastPosition returns the latest valid position fix, or nil when none
// was ever recorded. Freshest fix, not trail-gated.
func (s *Sampler) LastPosition() *geo.Point {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.haveFix {
		return nil
	}
	pos := s.lastFix
	return &pos
}

// SetTrackLog attaches the durable trail sink. Nil detaches.
func (s *Sampler) SetTrackLog(t *TrackLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.track = t
}

// Trail returns the recorded positions oldest-first.
func (s *Sampler) Trail() []trailPoint {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.path.points()
}

// Workloads returns a copy of the latest per-workload samples.
func (s *Sampler) Workloads() map[uuid.UUID]WorkloadSample {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[uuid.UUID]WorkloadSample, len(s.samples))
	maps.Copy(out, s.samples)
	return out
}

// CaptureTrend appends the latest readings to their trend series. Call it
// on a slow beat, not per sample: history is trend context at 5s granularity
// while latest values stay on the fast beat.
func (s *Sampler) CaptureTrend() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for id, sample := range s.samples {
		hist, ok := s.workTrends[id]
		if !ok {
			hist = newTrend()
			s.workTrends[id] = hist
		}
		hist.add(trendPoint{at: now, cpu: sample.CPUPercent, mem: sample.MemPercent, memMB: sample.MemUsageMB})
	}
}

// Trend folds a workload's series into mean CPU and memory percents plus
// the samples folded. Unknown ids report zeros.
func (s *Sampler) Trend(id uuid.UUID) (cpu, mem uint8, n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hist, ok := s.workTrends[id]
	if !ok {
		return 0, 0, 0
	}
	return hist.average()
}

// percentOf returns 100*a/b clamped to 0-100, or 0 when b is 0.
func percentOf(a, b uint64) uint8 {
	if b == 0 || a == 0 {
		return 0
	}
	pct := 100 * a / b
	if pct > 100 {
		return 100
	}
	return uint8(pct)
}

// cpuPercent returns 100*(1-idleDelta/totalDelta) clamped to 0-100.
func cpuPercent(before, after cpuTimes) uint8 {
	if after.total <= before.total {
		return 0
	}
	busy := (after.total - before.total) - (after.idle - before.idle)
	return percentOf(busy, after.total-before.total)
}

// readCPUTimes parses the aggregate cpu line of /proc/stat.
func readCPUTimes() (cpuTimes, error) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return cpuTimes{}, fmt.Errorf("open stat: %w", err)
	}
	defer file.Close() //nolint:errcheck // best-effort file cleanup on read

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		// Aggregate line plus idle and iowait: anything shorter is not
		// a cpu line worth trusting.
		if len(fields) < 6 || fields[0] != "cpu" {
			continue
		}
		var total uint64
		var idle uint64
		for i, f := range fields[1:] {
			v, err := strconv.ParseUint(f, 10, 64)
			if err != nil {
				return cpuTimes{}, fmt.Errorf("parse cpu field %d: %w", i, err)
			}
			total += v
			// idle is the 4th field, iowait the 5th: both count as idle.
			if i == 3 || i == 4 {
				idle += v
			}
		}
		return cpuTimes{idle: idle, total: total}, nil
	}
	serr := scanner.Err()
	if serr != nil {
		return cpuTimes{}, fmt.Errorf("scanner error: %w", serr)
	}
	return cpuTimes{}, fmt.Errorf("no cpu line: %w", ErrNoCPULine)
}

// readMemPercent returns used over total system memory as 0-100.
func readMemPercent() (uint8, error) {
	mem, err := readMeminfo()
	if err != nil {
		return 0, err
	}
	if mem.total == 0 {
		return 0, fmt.Errorf("meminfo has no total: %w", ErrMemTotalNotFound)
	}
	avail := mem.avail
	if avail == 0 {
		// Kernels without MemAvailable: approximate from free+buffers+cached.
		avail = mem.free + mem.buffers + mem.cached
	}
	if avail > mem.total {
		avail = mem.total
	}
	return percentOf(mem.total-avail, mem.total), nil
}

// meminfoValues holds raw kB figures parsed from /proc/meminfo.
type meminfoValues struct {
	total   uint64
	avail   uint64
	free    uint64
	buffers uint64
	cached  uint64
}

// readMeminfo parses /proc/meminfo into raw kB figures.
func readMeminfo() (meminfoValues, error) {
	var mem meminfoValues
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return mem, fmt.Errorf("open meminfo: %w", err)
	}
	defer file.Close() //nolint:errcheck // best-effort file cleanup on read

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		v, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			continue
		}
		switch fields[0] {
		case "MemTotal:":
			mem.total = v
		case "MemAvailable:":
			mem.avail = v
		case "MemFree:":
			mem.free = v
		case "Buffers:":
			mem.buffers = v
		case "Cached:":
			mem.cached = v
		}
	}
	serr := scanner.Err()
	if serr != nil {
		return mem, fmt.Errorf("scanner error: %w", serr)
	}
	return mem, nil
}

// readDiskPercent returns used over total blocks of the filesystem holding
// the concord config directory as 0-100. That disk holds the journal,
// bbolt, registry, and images, so it is the one that must never fill.
func readDiskPercent() (uint8, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return 0, fmt.Errorf("get user config directory: %w", err)
	}
	var stat syscall.Statfs_t
	serr := syscall.Statfs(filepath.Join(dir, "concord"), &stat)
	if serr != nil {
		return 0, fmt.Errorf("statfs: %w", serr)
	}
	var used uint64
	if stat.Bavail <= stat.Blocks {
		used = stat.Blocks - stat.Bavail
	}
	return percentOf(used, stat.Blocks), nil
}
