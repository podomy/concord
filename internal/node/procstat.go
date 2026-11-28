// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package node

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// ReadProcStat returns one process's cumulative CPU time in nanoseconds
// and resident memory in bytes. CPU sums utime plus stime from
// /proc/<pid>/stat, converted with the kernel clock tick rate. Memory
// reads VmRSS from /proc/<pid>/status. A dead or unreadable pid is an
// error; callers keep the previous sample.
func ReadProcStat(pid int) (cpuTotalNS, memBytes uint64, err error) {
	stat, err := os.ReadFile(procStatPath(pid))
	if err != nil {
		return 0, 0, fmt.Errorf("read proc stat: %w", err)
	}
	ticks, err := parseProcStatTicks(stat)
	if err != nil {
		return 0, 0, err
	}
	status, err := os.ReadFile(procStatusPath(pid))
	if err != nil {
		return 0, 0, fmt.Errorf("read proc status: %w", err)
	}
	memKB, err := parseProcStatusVmRSS(status)
	if err != nil {
		return 0, 0, err
	}
	return ticks * uint64(1_000_000_000/clockTicksPerSec), memKB * 1024, nil
}

// procStatPath locates one process's stat file.
func procStatPath(pid int) string {
	return "/proc/" + strconv.Itoa(pid) + "/stat"
}

// procStatusPath locates one process's status file.
func procStatusPath(pid int) string {
	return "/proc/" + strconv.Itoa(pid) + "/status"
}

// parseProcStatTicks sums utime and stime, fields 14 and 15 after the
// comm field. The comm field may hold spaces and parens, so fields are
// counted from the final closing paren, not from whitespace splits.
func parseProcStatTicks(stat []byte) (uint64, error) {
	rest, err := afterComm(stat)
	if err != nil {
		return 0, err
	}
	fields := strings.Fields(rest)
	if len(fields) < 13 {
		return 0, fmt.Errorf("proc stat has %d post-comm fields, want at least 13", len(fields))
	}
	utime, err := strconv.ParseUint(fields[11], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse proc utime: %w", err)
	}
	stime, err := strconv.ParseUint(fields[12], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse proc stime: %w", err)
	}
	return utime + stime, nil
}

// afterComm returns the stat line past the comm field's closing paren.
func afterComm(stat []byte) (string, error) {
	line := strings.TrimSpace(string(stat))
	idx := strings.LastIndexByte(line, ')')
	if idx < 0 || idx+2 > len(line) {
		return "", errors.New("proc stat missing comm field") //nolint:wrapcheck // plain error, no wrap target
	}
	return line[idx+2:], nil
}

// parseProcStatusVmRSS reads the VmRSS kB figure from status text.
func parseProcStatusVmRSS(status []byte) (uint64, error) {
	for line := range strings.Lines(string(status)) {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "VmRSS:" {
			continue
		}
		memKB, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			return 0, fmt.Errorf("parse proc VmRSS: %w", err)
		}
		return memKB, nil
	}
	return 0, errors.New("proc status has no VmRSS line") //nolint:wrapcheck // plain error, no wrap target
}

// clockTicksPerSec is the kernel USER_HZ rate that scales /proc stat
// jiffies to seconds. Linux fixes it at 100 on every architecture Go
// supports, so no sysconf call is needed to resolve it.
const clockTicksPerSec = 100
