// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cr

import (
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/opencontainers/cgroups"
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

// The merged bundle always carries a working null pair regardless of
// what the host enumerates: no host state may remove init's lifeline.
func TestBundleAlwaysCarriesWorkingNull(t *testing.T) {
	t.Parallel()

	cfg, err := BundleBuilder(t.Context(), PullResult{RootFS: t.TempDir()}, workload.Spec{ID: uuid.New()})
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}

	var node, rule bool
	for _, dev := range cfg.Devices {
		if dev.Path == "/dev/null" {
			node = true
		}
	}
	for _, r := range cfg.Cgroups.Devices {
		if r.Type == cdevices.CharDevice && r.Major == 1 && r.Minor == 3 && r.Allow && string(r.Permissions) == "rwm" {
			rule = true
		}
	}
	if !node || !rule {
		t.Fatalf("null node = %v, null rule = %v", node, rule)
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

// Every default entry allows: the bundle emits allows only, and a
// single deny would invert the eBPF program's meaning for that device.
func TestDefaultAllowedDevicesAllow(t *testing.T) {
	t.Parallel()

	for _, dev := range defaultAllowedDevices() {
		if !dev.Allow {
			t.Fatalf("default %q does not allow", dev.Path)
		}
	}
}

// Scanned entries allow too: DeviceFromPath leaves Allow at its zero
// value (deny), so the scan must set it explicitly. A temp symlink to
// /dev/null exercises the symlink path without root.
func TestScanEntrySetsAllow(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	link := dir + "/camera-front"
	if err := os.Symlink("/dev/null", link); err != nil {
		t.Fatal(err)
	}

	devs, ok := scanEntry(dir, mustDirEntry(t, dir, "camera-front"), 0)
	if !ok || len(devs) != 1 {
		t.Fatalf("scan = %v, %v", devs, ok)
	}
	if !devs[0].Allow {
		t.Fatal("scanned entry does not allow; it would deny its device")
	}
	if devs[0].Path != link {
		t.Fatalf("path = %q, want stable name %q", devs[0].Path, link)
	}
	if devs[0].Major != 1 || devs[0].Minor != 3 {
		t.Fatalf("numbers = %d:%d, want target 1:3", devs[0].Major, devs[0].Minor)
	}
}

// mustDirEntry reads one named entry for scanEntry tests.
func mustDirEntry(t *testing.T, dir, name string) os.DirEntry {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == name {
			return entry
		}
	}
	t.Fatalf("entry %q not found", name)
	return nil
}

func TestDefaultAllowedDevicesCoverNull(t *testing.T) {
	t.Parallel()

	var found *cdevices.Device
	for _, dev := range defaultAllowedDevices() {
		if dev.Path == "/dev/null" {
			found = dev
		}
	}
	if found == nil {
		t.Fatal("no /dev/null entry")
	}
	if found.Major != 1 || found.Minor != 3 || !found.Allow || string(found.Permissions) != "rwm" {
		t.Fatalf("null rule = %+v", found.Rule)
	}
}

// Merging drops default entries colliding by path and keeps scanned
// nodes; pathless rules always survive on both sides.
func TestMergeDevicesDedupsByPath(t *testing.T) {
	t.Parallel()

	scanned := &cdevices.Device{Path: "/dev/null"}
	scanned.Major, scanned.Minor, scanned.Allow = 1, 3, true
	merged := mergeDevices(defaultAllowedDevices(), []*cdevices.Device{scanned})

	nulls := 0
	for _, dev := range merged {
		if dev.Path == "/dev/null" {
			nulls++
			if dev != scanned {
				t.Fatal("default won over scanned node")
			}
		}
	}
	if nulls != 1 {
		t.Fatalf("null entries = %d, want 1", nulls)
	}
}

// Pathless entries emit rules but no nodes: there is nothing to mknod
// for mknod wildcards or pts.
func TestSplitDevicesSkipsPathlessNodes(t *testing.T) {
	t.Parallel()

	nodes, rules := splitDevices(defaultAllowedDevices())
	if len(rules) == 0 {
		t.Fatal("no rules emitted")
	}
	for _, node := range nodes {
		if node.Path == "" {
			t.Fatal("empty path node emitted")
		}
	}
	nulls := 0
	for _, rule := range rules {
		if rule.Major == 1 && rule.Minor == 3 && rule.Allow {
			nulls++
		}
	}
	if nulls != 1 {
		t.Fatalf("null rules = %d, want 1", nulls)
	}
}

// The bundle always carries an explicit per-workload cgroup scope: an
// empty path would resolve to the daemon's own parent cgroup, whose
// emptiness check can never pass, so no container could ever start.
// Limits ride on the same Cgroups, never instead of the scope.
func TestBundleCarriesExplicitCgroupScope(t *testing.T) {
	t.Parallel()

	id := uuid.New()
	cfg, err := BundleBuilder(t.Context(), PullResult{RootFS: t.TempDir()}, workload.Spec{ID: id})
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	if cfg.Cgroups == nil {
		t.Fatal("expected non-nil Cgroups")
	}
	want := "/concord/" + id.String()
	if cfg.Cgroups.Path != want {
		t.Fatalf("cgroup path = %q, want %q", cfg.Cgroups.Path, want)
	}

	limited, err := BundleBuilder(t.Context(), PullResult{RootFS: t.TempDir()}, workload.Spec{
		ID:        uuid.New(),
		Resources: workload.Resources{CPUShares: 512, MemoryMB: 256},
	})
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	if limited.Cgroups.Resources == nil {
		t.Fatal("expected resources preserved alongside the scope")
	}
	if got := limited.Cgroups.Memory; got != 256*1024*1024 {
		t.Fatalf("memory = %d", got)
	}
	if limited.Cgroups.Path == "" {
		t.Fatal("expected scope with limits too")
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

// The eBPF device backend must be registered: without its init, any
// non-empty device rule set fails hard at setup while empty sets pass
// silently, which is exactly the sensor-only failure this guards.
func TestDeviceBackendRegistered(t *testing.T) {
	t.Parallel()

	if cgroups.DevicesSetV1 == nil || cgroups.DevicesSetV2 == nil {
		t.Fatal("device backends unwired; import the devices package for its init")
	}
}

// The merged rule set must compile through the real v2 rule compiler:
// construction bugs (unknown types, unpunchable overlaps) surface here,
// before any cgroup exists. Compilation happens before any filesystem
// touch, so a fake dir proves the set clean with no privileges: any
// error short of the directory open means our rules, not the kernel.
func TestDeviceRulesCompile(t *testing.T) {
	t.Parallel()

	cfg, err := BundleBuilder(t.Context(), PullResult{RootFS: t.TempDir()}, workload.Spec{ID: uuid.New()})
	if err != nil {
		t.Fatalf("bundle: %v", err)
	}
	if cfg.Cgroups == nil || cfg.Cgroups.Resources == nil {
		t.Fatal("no resources to compile")
	}
	err = cgroups.DevicesSetV2("/nonexistent-concord-probe", cfg.Cgroups.Resources)
	if err == nil {
		t.Fatal("expected directory open to fail")
	}
	if strings.Contains(err.Error(), "punch hole") || strings.Contains(err.Error(), "not supported") {
		t.Fatalf("rule set rejected: %v", err)
	}
}
