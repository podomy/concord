// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"testing"

	cdevices "github.com/opencontainers/cgroups/devices/config"

	"github.com/podomy/concord/internal/workload"
)

// Every default device yields one container node plus one cgroup rule:
// presence without the rule grants nothing, the rule without presence
// reaches no path. The two lists stay parallel and same-length.
func TestSplitDevicesKeepsNodeAndRule(t *testing.T) {
	t.Parallel()

	dev := &cdevices.Device{
		Rule: cdevices.Rule{
			Type:        cdevices.CharDevice,
			Major:       81,
			Minor:       0,
			Permissions: "rwm",
			Allow:       true,
		},
		Path: "/dev/video0",
	}

	nodes, rules := splitDevices([]*cdevices.Device{dev})
	if len(nodes) != 1 || len(rules) != 1 {
		t.Fatalf("nodes = %d, rules = %d", len(nodes), len(rules))
	}
	if nodes[0].Major != 81 || nodes[0].Minor != 0 || nodes[0].Path != "/dev/video0" {
		t.Fatalf("node = %+v", nodes[0])
	}
	if rules[0].Major != 81 || rules[0].Minor != 0 || !rules[0].Allow {
		t.Fatalf("rule = %+v", rules[0])
	}
}

// The scan never fails hard: a host without sensors (like CI) yields an
// empty list, and BundleBuilder still produces a valid config.
func TestBundleBuildsWithoutDevices(t *testing.T) {
	t.Parallel()

	cfg, err := BundleBuilder(t.Context(), PullResult{RootFS: t.TempDir()}, workload.Spec{})
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	if cfg == nil || cfg.Rootfs == "" {
		t.Fatal("expected usable config")
	}
}

// The deny lists refuse storage in every spelling, raw host memory,
// virtualization control, and symlink trees pointing at either. Sensor
// names in every common spelling pass.
func TestDeniedDevice(t *testing.T) {
	t.Parallel()

	denied := []string{
		"/dev/sda", "/dev/sda1", "/dev/hdb", "/dev/vdc",
		"/dev/nvme0n1", "/dev/nvme0n1p1", "/dev/mmcblk0", "/dev/mmcblk0p1",
		"/dev/loop0", "/dev/dm-0", "/dev/md0", "/dev/ram0", "/dev/fd0",
		"/dev/mapper/vg-root", "/dev/disk/by-uuid/1234", "/dev/block/8:0",
		"/dev/char/81:0", "/dev/pts/0", "/dev/cpu/0/msr",
		"/dev/mem", "/dev/kmem", "/dev/port", "/dev/kvm",
		"/dev/nvram", "/dev/msr", "/dev/snapshot",
	}
	for _, path := range denied {
		if !deniedDevice(path) {
			t.Errorf("deniedDevice(%q) = false", path)
		}
	}

	allowed := []string{
		"/dev/video0", "/dev/ttyUSB0", "/dev/ttyACM0", "/dev/apex_0",
		"/dev/i2c-1", "/dev/spidev0.0", "/dev/gpiochip0",
		"/dev/dri/card0", "/dev/kfd", "/dev/nvidia0", "/dev/accel0",
		"/dev/bus/usb/001/002", "/dev/input/event0", "/dev/hidraw0",
		"/dev/camera-front", "/dev/lidar",
	}
	for _, path := range allowed {
		if deniedDevice(path) {
			t.Errorf("deniedDevice(%q) = true", path)
		}
	}
}

// The live scan of host /dev returns no denied node: whatever the
// machine holds, storage and raw memory never reach a container. It
// also resolves symlinks, so a stable name is exposed when its target
// is allowed.
func TestDefaultHostDevicesDenyNothingThrough(t *testing.T) {
	t.Parallel()

	for _, dev := range defaultHostDevices() {
		if deniedDevice(dev.Path) {
			t.Errorf("scan exposed denied node %q", dev.Path)
		}
	}
}

// A directory without devices scans empty instead of failing.
func TestScanDevicesEmptyDir(t *testing.T) {
	t.Parallel()

	if got := scanDevices(t.TempDir(), 0); len(got) != 0 {
		t.Fatalf("scan = %v, want empty", got)
	}
}
