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

	// Blank import for the eBPF backend registration: its init wires the
	// device-rule appliers both hierarchies call. Without it any
	// non-empty rule set fails hard, so every container with sensors
	// would fail at device setup while sensorless ones pass silently.
	_ "github.com/opencontainers/cgroups/devices"
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

	cg := &cgroups.Cgroup{
		// Every container gets its own cgroup scope named after the
		// workload. An empty path would resolve to the parent of the
		// daemon's own cgroup, which always contains the daemon itself:
		// factory.Create's emptiness check could then never pass, no
		// container could ever start, limits would apply to the
		// aggregate, and per-workload stats would read the aggregate
		// too. Explicit per-workload paths make the check meaningful
		// again.
		Path: "/concord/" + spec.ID.String(),
	}
	if spec.Resources.CPUShares > 0 || spec.Resources.MemoryMB > 0 {
		res := &cgroups.Resources{}
		if spec.Resources.CPUShares > 0 {
			res.CpuShares = spec.Resources.CPUShares
		}
		if spec.Resources.MemoryMB > 0 {
			res.Memory = spec.Resources.MemoryMB * 1024 * 1024
		}
		cg.Resources = res
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
	deviceNodes, deviceRules := splitDevices(mergeDevices(defaultAllowedDevices(), hostDevices))

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
// change. Only character and block devices pass: FIFOs and anything
// else unnumbered would fail program compilation for every container.
// Symlinks resolve first: a udev stable name like
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
	// The cgroup device controller only understands character, block,
	// and wildcard rules: a FIFO (or anything else unnumbered the scan
	// resolves) would fail program compilation for every container on
	// the host, so it never leaves the scan.
	if !dev.Type.CanCgroup() {
		return nil, false
	}
	// DeviceFromPath leaves Allow at its zero value, which means DENY:
	// every scanned rule would forbid its device instead of opening it,
	// and the first one trips the hole-punch guard against our own mknod
	// wildcards. Set explicitly; presence here always means allow.
	dev.Allow = true
	dev.Path = name
	return []*cdevices.Device{dev}, true
}

// defaultAllowedDevices mirrors runc's canonical allow-list: mknod
// wildcards plus null, random, full, tty, zero, urandom, pts, ptmx,
// and net/tun, all 0666. runc prepends these to every container; our
// scanned sensor rules ride after, with scanned entries winning path
// collisions. Without this list the eBPF program allows exactly the
// configured rules and nothing else, so even /dev/null (needed by
// container init itself) would be denied. The majors are kernel ABI
// and the list is frozen upstream by fear of breaking dependents, so
// copying it is stable, not drift-prone.
func defaultAllowedDevices() []*cdevices.Device {
	mknod := func(typ cdevices.Type) cdevices.Device {
		return cdevices.Device{
			Rule: cdevices.Rule{
				Type:        typ,
				Major:       cdevices.Wildcard,
				Minor:       cdevices.Wildcard,
				Permissions: "m",
				Allow:       true,
			},
		}
	}
	node := func(path string, major, minor int64) *cdevices.Device {
		return &cdevices.Device{
			Rule: cdevices.Rule{
				Type:        cdevices.CharDevice,
				Major:       major,
				Minor:       minor,
				Permissions: "rwm",
				Allow:       true,
			},
			Path:     path,
			FileMode: 0o666,
		}
	}
	rule := func(typ cdevices.Type, major, minor int64) *cdevices.Device {
		return &cdevices.Device{
			Rule: cdevices.Rule{
				Type:        typ,
				Major:       major,
				Minor:       minor,
				Permissions: "rwm",
				Allow:       true,
			},
		}
	}
	charMknod, blockMknod := mknod(cdevices.CharDevice), mknod(cdevices.BlockDevice)
	return []*cdevices.Device{
		&charMknod, &blockMknod,
		node("/dev/null", 1, 3),
		node("/dev/random", 1, 8),
		node("/dev/full", 1, 7),
		node("/dev/tty", 5, 0),
		node("/dev/zero", 1, 5),
		node("/dev/urandom", 1, 9),
		rule(cdevices.CharDevice, 136, cdevices.Wildcard),
		rule(cdevices.CharDevice, 5, 2),
		rule(cdevices.CharDevice, 10, 200),
	}
}

// mergeDevices prepends the canonical allow-list ahead of scanned
// devices, dropping scanned entries that collide by path (the scanned
// node wins, mirroring runc's dedup where explicit devices take
// priority). Defaults must always come first: without them the eBPF
// program allows exactly the configured rules and nothing else, which
// is how /dev/null ended up denied.
func mergeDevices(defaults, scanned []*cdevices.Device) []*cdevices.Device {
	seen := make(map[string]bool, len(scanned))
	for _, dev := range scanned {
		if dev.Path != "" {
			seen[dev.Path] = true
		}
	}
	out := make([]*cdevices.Device, 0, len(defaults)+len(scanned))
	for _, dev := range defaults {
		if dev.Path != "" && seen[dev.Path] {
			continue
		}
		out = append(out, dev)
	}
	return append(out, scanned...)
}

// splitDevices separates container nodes from cgroup rules. libcontainer
// creates each node in container /dev; each rule opens the same
// major:minor in the cgroup (devices default-deny, so presence alone
// grants nothing). Pathless entries (mknod wildcards, pts) emit rules
// only: there is no node to create for them.
func splitDevices(devs []*cdevices.Device) ([]*cdevices.Device, []*cdevices.Rule) {
	nodes := make([]*cdevices.Device, 0, len(devs))
	rules := make([]*cdevices.Rule, 0, len(devs))
	for _, dev := range devs {
		if dev.Path != "" {
			nodes = append(nodes, dev)
		}
		rule := dev.Rule
		rules = append(rules, &rule)
	}
	return nodes, rules
}
