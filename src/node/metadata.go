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
	ErrMemTotalNotFound = errors.New("memTotal field not found in /proc/meminfo")
	ErrNoCPUMHzFound    = errors.New("no 'cpu MHz' entries found in /proc/cpuinfo")
)

func GetTotalMemoryMB() (uint64, error) {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, nil
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

			// KiB -> MiB
			memoryMB := valKiB / 1024
			return memoryMB, nil
		}
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scanner error: %w", err)
	}

	return 0, ErrMemTotalNotFound //nolint:wrapcheck // sentinel error
}

func GetCPUFreqMHz() (float64, error) {
	khz, err := readProcCPU()
	if err != nil {
		return 0, fmt.Errorf("read sys max freq: %w", err)
	}

	return khz, nil
}

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

		// Looking for lines that start with "cpu MHz"
		if strings.HasPrefix(line, "cpu MHz") {
			parts := strings.Split(line, ":")
			if len(parts) < 2 {
				continue
			}

			mhzVal := strings.TrimSpace(parts[1])
			mhz, err := strconv.ParseFloat(mhzVal, 64)
			if err != nil {
				// skip malformed lines
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
