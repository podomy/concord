// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/opencontainers/cgroups"
	cdevices "github.com/opencontainers/cgroups/devices/config"
	"github.com/opencontainers/runc/libcontainer/configs"
	runcdevices "github.com/opencontainers/runc/libcontainer/devices"

	"github.com/podomy/concord/internal/workload"
)

// BundleBuilder produces a libcontainer Config from a pulled image.
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

	// Proc for the processes.
	mountProc := configs.Mount{
		Device:      "proc",
		Destination: "/proc",
		Source:      "proc",
		Flags:       syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_RDONLY,
	}

	// Shm for the ram access.
	mountTmpfs := configs.Mount{
		Device:      "tmpfs",
		Destination: "/dev/shm",
		Source:      "tmpfs",
	}

	// Pseudo terminal access.
	mountPts := configs.Mount{
		Device:      "devpts",
		Destination: "/dev/pts",
		Source:      "devpts",
		Data:        "newinstance,ptmxmode=0666",
	}

	// Proc for the device tree.
	mountSys := configs.Mount{
		Device:      "sysfs",
		Destination: "/sys",
		Source:      "sysfs",
		Flags:       syscall.MS_NOEXEC | syscall.MS_NOSUID | syscall.MS_RDONLY,
	}

	mounts := []*configs.Mount{}
	mounts = append(mounts, &mountProc, &mountTmpfs, &mountPts, &mountSys)

	hostDevices := defaultHostDevices()
	deviceNodes, deviceRules := splitDevices(hostDevices)

	config := configs.Config{
		Rootfs:     result.RootFS,
		Namespaces: namespaces,
		Mounts:     mounts,
		Cgroups:    cg,
		Devices:    deviceNodes,
	}

	if len(deviceRules) > 0 {
		if config.Cgroups == nil {
			config.Cgroups = &cgroups.Cgroup{}
		}
		res := config.Cgroups.Resources
		if res == nil {
			res = &cgroups.Resources{}
			config.Cgroups.Resources = res
		}
		res.Devices = append(res.Devices, deviceRules...)
	}

	return &config, nil
}

// deniedDeviceNames are host nodes never exposed to any container:
// raw host memory, virtualization control, NVRAM, CPU registers.
var deniedDeviceNames = map[string]bool{
	"/dev/mem":      true,
	"/dev/kmem":     true,
	"/dev/port":     true,
	"/dev/kvm":      true,
	"/dev/nvram":    true,
	"/dev/msr":      true,
	"/dev/snapshot": true,
}

// deniedDevicePrefixes are host node names never exposed: disks and
// volumes in every spelling, plus symlink trees that only point back
// at them or at other nodes. Everything else in /dev is fair game:
// cameras, lidars, TPUs, sensors, GPUs nodes, serial ports.
var deniedDevicePrefixes = []string{
	"/dev/sd", "/dev/hd", "/dev/vd",
	"/dev/nvme", "/dev/mmcblk", "/dev/loop", "/dev/dm-",
	"/dev/md", "/dev/ram", "/dev/fd",
	"/dev/mapper/", "/dev/disk/", "/dev/block/", "/dev/char/",
	"/dev/pts/", "/dev/cpu/",
}

// deniedDevice reports whether a /dev path must stay off containers.
// Both the entry name and, for symlinks, the resolved target are
// checked, so /dev/disk/by-uuid/X is refused as the /dev/sda1 it
// points at.
func deniedDevice(path string) bool {
	if deniedDeviceNames[path] {
		return true
	}
	for _, prefix := range deniedDevicePrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}
	return false
}

// scanDepth caps the /dev walk: the tree itself plus bus addresses and
// driver subdirectories. Nothing sensor-shaped lives deeper.
const scanDepth = 3

// defaultHostDevices exposes host devices to every container. The scan
// walks /dev and lets everything through except the deny lists, so a
// sensor nobody has heard of yet works on first plug with no code
// change. Symlinks resolve first: a udev stable name like
// /dev/camera-front is exposed under its own name with the target's
// numbers, and a symlink into denied storage stays denied. Absent
// hardware contributes nothing; a node without sensors runs the same
// config shape with fewer devices.
func defaultHostDevices() []*cdevices.Device {
	return scanDevices("/dev", 0)
}

// scanDevices walks one directory level, collecting device nodes.
func scanDevices(dir string, depth int) []*cdevices.Device {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []*cdevices.Device
	for _, entry := range entries {
		if dev, ok := scanEntry(dir, entry, depth); ok {
			out = append(out, dev...)
		}
	}
	return out
}

// scanEntry resolves one /dev entry to devices: subdirectories recurse,
// symlinks resolve against the deny lists, plain nodes pass through.
func scanEntry(dir string, entry os.DirEntry, depth int) ([]*cdevices.Device, bool) {
	name := dir + "/" + entry.Name()
	if deniedDevice(name) {
		return nil, false
	}
	if entry.IsDir() {
		if depth >= scanDepth {
			return nil, false
		}
		return scanDevices(name, depth+1), true
	}
	target := name
	if entry.Type()&os.ModeSymlink != 0 {
		resolved, err := filepath.EvalSymlinks(name)
		if err != nil || deniedDevice(resolved) {
			return nil, false
		}
		target = resolved
	}
	dev, err := runcdevices.DeviceFromPath(target, "rwm")
	if err != nil {
		return nil, false
	}
	dev.Path = name
	return []*cdevices.Device{dev}, true
}

// splitDevices separates container nodes from cgroup rules. libcontainer
// creates each node in container /dev; each rule opens the same
// major:minor in the cgroup (devices default-deny, so presence alone
// grants nothing).
func splitDevices(devs []*cdevices.Device) ([]*cdevices.Device, []*cdevices.Rule) {
	nodes := make([]*cdevices.Device, 0, len(devs))
	rules := make([]*cdevices.Rule, 0, len(devs))
	for _, dev := range devs {
		nodes = append(nodes, dev)
		rule := dev.Rule
		rules = append(rules, &rule)
	}
	return nodes, rules
}
