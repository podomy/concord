# Devices: What Every Container Can Touch

Every Concord container sees the host's sensors, always, with no
opt-in and no spec field. A camera on the truck is visible to every
workload on the truck. This is deliberate and follows from the trust
model: one operator, one fleet, cooperating workloads. Kubernetes
hides devices by default because strangers share its nodes; Concord
nodes are never shared, so the default is open.

The rule is a deny list, not an allow list: everything in `/dev`
reaches containers except storage and raw host control. A sensor
nobody has heard of yet works on first plug with no code change. If a
device you own does not show up, that is a bug: report it.

## Two checks, both required

Think of a locked room. The cgroup rule is the guest list: it names
which devices a container may touch, by their numbers. The device
node is the door: the actual `/dev/video0` path inside the container.
You need both. A door with no guest-list entry opens onto nothing:
every read fails. A guest-list entry with no door reaches nowhere:
no path resolves to it. Concord writes both together for every
allowed device at every container start. Either one alone grants
nothing, so there is no half-applied state to reason about.

Concretely, for a camera that is major 81 minor 0, the builder emits
"character device 81:0 may be read, written, and created" into the
container's cgroup rules, plus a `/dev/video0` node with those same
numbers inside the container. Same pair for every device, every
start.

The scanned set rides on top of runc's canonical allow-list (null,
zero, full, random, urandom, tty, pts, ptmx, tun, plus mknod
wildcards), which every container needs regardless of sensors: init
itself reopens `/dev/null`, and the eBPF program allows exactly the
configured rules and nothing else. Scanned entries win path collisions
by the same dedup runc applies to explicit devices.

Everything allowed is allowed fully: read, write, and create. A lidar
that needs write commands must not discover the refusal at 3 AM.
Per-device trimming would rebuild the policy complexity this default
exists to avoid.

## The ground counts too: no nodev under bundles

A device open passes four gates and the rule set is only the first:
the cgroup program, the node's own bits, mandatory-access policy,
and the mount flags holding the node. A cgroup refusal reads
`operation not permitted`; anything else reads `permission denied`.
The filesystem gate is the trap: a `nodev` mount lets `mknod`
succeed and refuses every open, so the node exists at 0666 with an
allowing program and still fails. Bundles live under the config
directory, so aiming `XDG_CONFIG_HOME` at a `nodev` mount (the `/tmp`
tmpfs on most systems) kills every container on `/dev/null` with a
line identical to a rule bug. The full story, including the errno
discriminator and the elimination runbook, is `docs/nodev.md`.

Operational rule: keep the config directory on the system volume.
The `~/.config` default is fine; `/var/tmp` works for harnesses.
Never aim `XDG_CONFIG_HOME` at `/tmp`.

## What stays out, and why

```
never exposed
  disks and volumes (/dev/sd*, /dev/nvme*, /dev/mmcblk*, /dev/loop*,
  /dev/dm-*, /dev/md*, /dev/mapper/*, /dev/disk/*, ...): always-on
  access must not reach storage. One compromised workload must not
  reformat the host.
  raw host control (/dev/mem, /dev/kmem, /dev/port, /dev/kvm,
  /dev/nvram, ...): direct host memory and virtualization.
```

Symlinks are judged by their target: `/dev/disk/by-uuid/X` is refused
as the `/dev/sda1` it points at. Symlink trees that only alias other
nodes (`/dev/block/`, `/dev/char/`) are skipped for the same reason.
Subdirectories deeper than bus addresses and driver folders are not
walked; nothing sensor-shaped lives deeper.

GPUs need more than nodes: an NVIDIA card also needs its driver
libraries injected into the container, which is a toolkit integration
with its own version matrix. That support comes later. The device
nodes themselves already pass through like any other allowed node.

## The sensors this covers

Most robotics hardware is a plain device file, which is why the deny
list works:

- USB cameras (`/dev/video*`) open through standard video calls.
- Serial lidars like RPLidar, serial GPS, and radios
  (`/dev/ttyUSB*`, `/dev/ttyACM*`) open as serial ports.
- Coral sticks (`/dev/apex_*`) are Google's small USB or M.2 AI
  accelerator boards: they run neural-network math beside the CPU so
  the main processor stays free. They look like one device file per
  stick.
- I2C and SPI sensors (`/dev/i2c-*`, `/dev/spidev*`): IMUs,
  temperature, pressure, small displays.
- GPIO chips (`/dev/gpiochip*`): individual on/off lines.
- USB devices reached through software libraries instead of serial
  ports live under `/dev/bus/usb/`, which the scan walks into.
- GPU render nodes (`/dev/dri/*`) pass through as nodes; only the
  NVIDIA library injection is missing (above).

## Stable names with udev

The problem: the kernel numbers devices in plug order. Two identical
cameras enumerate as `video0` and `video1` today; unplug one, reboot,
and the survivor may come back as the other number. A workload
opening `video0` then gets the wrong camera with no error.

The fix: one rule file per vehicle that pins a name to a physical
stick by its factory serial number. After that, `/dev/camera-front`
always means the same camera on every boot, whatever number the
kernel assigned.

```
# 1. Find each camera's serial:
udevadm info -a /dev/video0 | grep serial

# 2. Write /etc/udev/rules.d/99-concord-cameras.rules:
SUBSYSTEM=="video4linux", ATTRS{serial}=="FRONT-CAM-01", SYMLINK+="camera-front"
SUBSYSTEM=="video4linux", ATTRS{serial}=="REAR-CAM-02", SYMLINK+="camera-rear"

# 3. Activate without rebooting:
udevadm control --reload-rules && udevadm trigger
```

Reading the rule line by line: when a video device appears whose
serial matches, create an extra name for it. The workload opens
`/dev/camera-front`, which the scan picks up as a symlink and
exposes under that same name with the real camera's numbers. Do this
once per vehicle at provisioning, next to the CA and gossip key. Same
pattern for serial lidars (`/dev/lidar`, match by USB serial).

If two identical sensors have no rules, they may swap names across
reboots and nothing can tell. That is a provisioning gap, not a
protocol problem.

## Not devices: CAN and ethernet sensors

Two sensor families look like hardware but arrive through other
interfaces, and no device rule reaches them:

- **CAN bus** (motor controllers, mobile bases, battery systems) is a
  network interface (`can0`), not a device file. It lives in the
  container's network namespace by different machinery: virtual CAN
  plumbing, or moving the physical interface into the namespace.
  Separate feature, separate doc when it lands.
- **Ethernet lidars** (Velodyne, Ouster, Hesai) are UDP packet
  streams. The sensor is a peer on the wire; the workload needs
  network reachability (host networking or bridged ports), never a
  node. No `/dev` entry exists or ever will.

If a sensor has no file under `/dev` and no address on the network,
it is one of these two.

## Consequences, stated plainly

- A compromised workload reads every sensor on its node. Accepted:
  the fleet is one trust domain, and a workload that may run here may
  sense here. The damage ends at the node boundary, which the network
  namespaces still enforce.
- Unplug a sensor mid-run and reads fail until the container
  restarts. Nothing re-scans a running container.
- New hardware appears in containers started after the plug,
  including hardware nobody taught Concord about. That is the point
  of the deny list.

## Where it lives

Deny lists and scan (`deniedDeviceNames`, `deniedDevicePrefixes`,
`scanDevices` in `internal/cr/bundle.go`), node/rule split
(`splitDevices`, same file), libcontainer application
(`configs.Config.Devices` plus `Resources.Devices`, applied by runc's
device cgroup controller), bundle placement (`bundleDirPath` in
`internal/reconciler/reconciler.go`: bundles must sit on a
dev-capable filesystem, see above).
