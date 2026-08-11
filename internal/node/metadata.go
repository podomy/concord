// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var (
	// ErrMemTotalNotFound indicates that the MemTotal line could not be found in /proc/meminfo.
	ErrMemTotalNotFound = errors.New("memTotal field not found in /proc/meminfo")
	// ErrNoCPUMHzFound indicates that no cpu MHz entries were found in /proc/cpuinfo.
	ErrNoCPUMHzFound = errors.New("no 'cpu MHz' entries found in /proc/cpuinfo")
)

// GetTotalMemoryMB reads /proc/meminfo from the host procfs and returns
// total physical system memory converted from KiB to MiB.
func GetTotalMemoryMB() (uint64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, fmt.Errorf("open meminfo: %w", err)
	}
	defer file.Close() //nolint:errcheck // best-effort file cleanup on read

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "MemTotal:") {
			fields := strings.Fields(line)
			if len(fields) < 2 {
				continue
			}

			valKiB, err := strconv.ParseUint(fields[1], 10, 64)
			if err != nil {
				return 0, fmt.Errorf("parse uint: %w", err)
			}

			// KiB -> MiB.
			memoryMB := valKiB / 1024
			return memoryMB, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scanner error: %w", err)
	}

	return 0, ErrMemTotalNotFound //nolint:wrapcheck // sentinel error
}

// GetCPUFreqMHz reads the aggregate CPU frequency in MHz across all active
// CPU cores listed in /proc/cpuinfo.
func GetCPUFreqMHz() (float64, error) {
	mhz, err := readProcCPU()
	if err != nil {
		return 0, fmt.Errorf("read sys cpu freq: %w", err)
	}

	return mhz, nil
}

// readProcCPU parses /proc/cpuinfo to compute the aggregate clock frequency in MHz
// across all CPU cores present on the system.
func readProcCPU() (float64, error) {
	file, err := os.Open("/proc/cpuinfo")
	if err != nil {
		return 0, fmt.Errorf("open file: %w", err)
	}
	defer file.Close() //nolint:errcheck // best-effort file cleanup on read

	var totalMHz float64
	var coreCount int

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		// Looking for lines that start with "cpu MHz".
		if strings.HasPrefix(line, "cpu MHz") {
			parts := strings.Split(line, ":")
			if len(parts) < 2 {
				continue
			}

			mhzVal := strings.TrimSpace(parts[1])
			mhz, err := strconv.ParseFloat(mhzVal, 64)
			if err != nil {
				// skip malformed lines.
				continue
			}

			totalMHz += mhz
			coreCount++
		}
	}

	err = scanner.Err()
	if err != nil {
		return 0, fmt.Errorf("scanner error: %w", err)
	}

	if coreCount == 0 {
		return 0, ErrNoCPUMHzFound //nolint:wrapcheck // sentinel error
	}

	return totalMHz, nil
}
