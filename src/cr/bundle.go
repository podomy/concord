// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"context"
	"fmt"
	"syscall"

	"github.com/opencontainers/cgroups"
	"github.com/opencontainers/runc/libcontainer/configs"

	"github.com/podomy/concord/src/workload"
)

// bundleBuilder produces a libcontainer Config from a pulled image.
//
// Invariants:
//   - The caller is responsible for constructing the libcontainer Process
//     (Args, Env, Cwd, User) from the image config and workload Spec.
//   - All containers get standard isolation namespaces and pseudo-fs mounts.
//   - Resources (CPU, memory) from the spec are mapped to cgroup limits.
//     MemoryMB is converted from megabytes to bytes for the kernel.
//     CPUShares of 0 means no limit is set (kernel default applies).
func BundleBuilder(ctx context.Context, result PullResult, spec workload.Spec) (*configs.Config, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("context cancellation: %w", err)
	}

	var cg *cgroups.Cgroup
	if spec.Resources.CPUShares > 0 || spec.Resources.MemoryMB > 0 {
		res := &cgroups.Resources{}
		if spec.Resources.CPUShares > 0 {
			res.CpuShares = spec.Resources.CPUShares
		}
		if spec.Resources.MemoryMB > 0 {
			res.Memory = spec.Resources.MemoryMB * 1024 * 1024
		}
		cg = &cgroups.Cgroup{Resources: res}
	}

	namespaces := configs.Namespaces{}
	namespaces.Add(configs.NEWNS, "")
	namespaces.Add(configs.NEWPID, "")
	namespaces.Add(configs.NEWNET, "")
	namespaces.Add(configs.NEWUTS, "")
	namespaces.Add(configs.NEWIPC, "")

	// Proc for the processes
	mountProc := configs.Mount{
		Device:      "proc",
		Destination: "/proc",
		Source:      "proc",
		Flags:       syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_RDONLY,
	}

	// Shm for the ram access
	mountTmpfs := configs.Mount{
		Device:      "tmpfs",
		Destination: "/dev/shm",
		Source:      "tmpfs",
	}

	// Pseudo terminal access
	mountPts := configs.Mount{
		Device:      "devpts",
		Destination: "/dev/pts",
		Source:      "devpts",
		Data:        "newinstance,ptmxmode=0666",
	}

	// Proc for the device tree
	mountSys := configs.Mount{
		Device:      "sysfs",
		Destination: "/sys",
		Source:      "sysfs",
		Flags:       syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_RDONLY,
	}

	mounts := []*configs.Mount{}
	mounts = append(mounts, &mountProc, &mountTmpfs, &mountPts, &mountSys)

	config := configs.Config{
		Rootfs:     result.RootFS,
		Namespaces: namespaces,
		Mounts:     mounts,
		Cgroups:    cg,
	}

	return &config, nil
}
