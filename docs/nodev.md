# The ground under the bundles: filesystems that forbid devices

This page explains one failure mode in full: every container dies in
init with `open /dev/null: permission denied`, while every device
rule is correct. The cause is not a rule. It is the ground the rules
stand on: the bundle directory sits on a filesystem mounted with
`nodev`, and that flag alone refuses every device open. Read this
page if a container will not start and the logs blame `/dev/null`.

## The failure, exactly

The daemon log shows this line and the container never runs:

```
error reopening /dev/null inside container:
open /dev/null: permission denied
```

The device rules allow null. The node exists with mode 0666. Nothing
about the rule set is wrong, and changing rules cannot fix it. The
rest of this page is why.

## Part 1: the pieces, from zero

A device file is a name that stands for hardware. Most files hold
bytes on a disk; opening `/home/user/notes.txt` reads bytes. Opening
`/dev/null` reads nothing and accepts everything written to it,
because the kernel sees the file's numbers, not its contents, and
hands the request to a driver. The file is a doorbell, not a box.

The numbers are the address. Every device file carries two: the major
number names the driver, the minor number names the unit. `/dev/null`
is character device 1:3: driver 1, unit 3. When documentation says
"character device 1:3 may be read, written, and created", it means
exactly this address. Two nodes with the same numbers are the same
device, wherever they are created.

`/dev/null` is special because init needs it. A fresh process has
three standard streams (input, output, error). Container init rebuilds
them inside the container, and any stream still pointing at the old
`/dev/null` must be reopened against the new one. This happens
before the workload's own binary ever runs, which is why a broken
`/dev/null` kills every container regardless of image: the failure
is in the hallway, not in any room.

Creating a device file and opening one are different acts. `mknod`
writes a name with numbers attached: cheap bookkeeping, allowed
almost anywhere. `open` asks the driver to start talking, and that
request passes every guard on the path. Remember this split; the
whole mystery turns on it.

A mount is a filesystem attached at a directory, and mounts carry
flags: rules posted at the gate. `nodev` means "names for hardware
are decoration here; no request reaches any driver". On a `nodev`
mount, `mknod` still succeeds (bookkeeping is fine) and `open` of the
result always fails with `permission denied`. The node is real, its
permissions are generous, and the door is bricked from the outside.

`tmpfs` is a filesystem that lives in RAM instead of on a disk, and
`/tmp` on most distributions is one. Distributions mount it with
`nodev` (plus `nosuid`, which likewise neuters setuid binaries)
because `/tmp` is shared scratch space: throwaway files there must
never become device access or privilege escalation. Sensible for
scratch. Fatal for bundles, as Part 3 shows. Confirm any path with:

```
findmnt -o OPTIONS --target /tmp
```

If the output lists `nodev`, device opens under that tree fail, full
stop.

## Part 2: the four gates on every device open

Opening a device inside a container passes four gates in order.
Concord's rule set is only the first.

1. The cgroup program. A small kernel program attached to the
   container's cgroup checks the request's numbers against the rule
   set. A refusal here reads `operation not permitted`. The kernel
   error is `EPERM`.
2. The node's own permission bits. Mode 0666 lets everyone read and
   write; anything narrower checks owner and group. A refusal here
   reads `permission denied`. The error is `EACCES`.
3. Mandatory-access policy. SELinux or AppArmor in enforcing mode can
   refuse even a 0666 node to an unlabeled process. Also `permission
   denied`, also `EACCES`. Nothing in any Concord log distinguishes
   it; only the audit log does.
4. The mount flags. `nodev` on the filesystem holding the node
   refuses with `permission denied`, `EACCES`, before any driver is
   consulted.

The error text is the whole diagnosis. `operation not permitted`
means gate 1: read the rules. `permission denied` means gates 2, 3,
or 4: read the node, the policy, and the mount table, in that order.
Conflating the two texts is what turns a one-line mount problem into
a week of rule archaeology.

## Part 3: why Concord stands on this ground

Follow one container from submit to start and watch where its files
live. The daemon keeps per-node state under the config directory,
which defaults to `~/.config` and moves wherever `XDG_CONFIG_HOME`
points. Image layers unpack into
`$XDG_CONFIG_HOME/concord/bundles/<workload>/rootfs` at pull time.
At start time the device nodes, `/dev/null` included, are created
inside that same tree. After the container pivots into its root, its
`/dev/null` is that file on that filesystem.

So the mount flags of one directory decide every container on the
node. Aim `XDG_CONFIG_HOME` at a `nodev` mount and the sequence runs
like this: pull succeeds (layers are plain files; `nodev` ignores
them), node creation succeeds (`mknod` is bookkeeping), and the first
`open` of `/dev/null` fails with `permission denied`. The log line is
byte-identical to a rule bug, and no rule change can move it. This is
not a misconfiguration of devices. It is a misplacement of state.

## Part 4: how the innocent were eliminated

This actually happened, and the order is worth keeping, because it is
the runbook for the next `permission denied`. Each suspect below was
closed with one observation, cheapest first.

- Stale binary. The running daemon's hash was compared against a
  fresh build from the same commit. Identical, so the code under test
  was the code under suspicion. Minutes, not hours.
- Construction. The daemon logs its exact rule input at bundle time:
  rule count plus whether a working null rule is present (see the
  debug line in the reconciler). The log showed the null rule
  present, so the builder was exonerated in the log itself.
- The compiler. The rule set was driven through the real compiler
  entry point in a test, proving the input compiles to an allowing
  program. Deterministic input means the same program every run.
- Ancestor programs. Device programs apply down the cgroup hierarchy,
  so a program on a parent scope constrains everything below it. The
  container's ancestry was swept (`/`, then `/concord`, then the
  workload scope): zero programs above, and the workload scope never
  persists between attempts. Nothing above could be voting.
- Destroy paths. Every teardown in the tree was instrumented to log,
  and the failing runs show zero destroy lines. The disappearing
  cgroup turned out to be libcontainer's own error-path cleanup
  inside the same start call, not a second bug.
- Host permissions. The host `/dev/null` is character device 1:3,
  mode 0666. Nothing exotic.
- SELinux. The distribution enforces it, and it was the favorite
  suspect, until one run with enforcement off (`setenforce 0`) failed
  byte-identically. Exonerated by experiment, not by argument.

What remained after all of that was gates 2 and 4 with gate 1
exonerated: a 0666 node, an allowing program, `EACCES`. One `findmnt`
showed the bundle tree on a `nodev` tmpfs, and the case closed. The
discriminator that would have ended it on day one was the errno in
the log line: `EACCES` on an allowed 0666 node cannot be the device
controller, because the controller answers `EPERM`.

## Part 5: runbook for the next permission denied

1. Read the error text. `operation not permitted` goes to
   `docs/devices.md`. `permission denied` continues here.
2. Find the bundle: `$XDG_CONFIG_HOME/concord/bundles/<workload>/`
   on the assigned node (the default root needs `sudo` to inspect).
3. `stat` the node: `stat -c '%a %u %g %t:%T'
   .../rootfs/dev/null`. Expect `666 0 0 1:3`. Anything else is a
   creation bug, not a rule bug.
4. `findmnt -o OPTIONS --target .../rootfs`. If `nodev` appears,
   stop: that is the denial. Move the config directory and rerun.
5. If the mount is clean, check the audit log for a same-timestamp
   denial (`ausearch -m avc -ts recent`). A hit names the policy.
6. If the audit log is empty, one permissive run (`setenforce 0`,
   reproduce, `setenforce 1`) settles SELinux definitively.
7. Only then look at rules and programs: the bundle-time rule line,
   then a sweep of ancestor cgroups for attached programs.

## Part 6: rules for placing state

- The config directory (and therefore bundles) stays on the system
  volume. The `~/.config` default is fine.
- Throwaway harnesses and test rigs use `/var/tmp`, not `/tmp`.
- Never aim `XDG_CONFIG_HOME` at `/tmp`, however convenient the
  cleanup looks. The cleanup is convenient because the mount forbids
  everything interesting.
- Rigs that must accept an arbitrary state directory check it first:
  `findmnt -no OPTIONS --target "$dir"` must not list `nodev`.
  Refuse early with that message instead of failing every container
  with a line that blames the rules.

## Part 7: what the fix promoted

Moving state off `nodev` did not produce a working container. It
produced the next failure, and that is the pattern to expect: init
runs stages in order, so a fixed blocker promotes the failure waiting
one stage downstream. Each new log line names its stage; read it
literally instead of reopening the closed case.

**Empty argv.** The new line was `panic from initialization:
... index out of range ... exec.LookPath`: the container process had
an empty argv. The submit ran `workload run <image>` with no command,
and the reconciler passed the empty spec command straight through
with no fallback to the image default. It never appeared before
because reopening `/dev/null` runs before the argv lookup: the nodev
denial killed every run at the earlier stage and masked this one. The
fix defaults a commandless submit to the image entrypoint plus cmd
(the contract in `docs/cli.md`); both empty is a logged build error,
never an empty argv. See `buildProcess` in
`internal/reconciler/reconciler.go`.

**Stale bundle.** After the first start failure, every later tick
failed at `pull image: rootfs already exists`. The bundle directory
was never reused or cleaned, so a workload whose start failed once
could never retry: every transient start failure became permanent.
Pull now removes the stale rootfs after resolving the image (and the
partial tree when extraction fails), and a failed start destroys its
created container so the next tick recreates instead of wedging. See
`Pull` in `internal/cr/puller.go` and `startContainer` in
`internal/reconciler/reconciler.go`.

**Overlong veth name.** The failure moved to `create veth pair: ...
link add: numerical result out of range`. The host name wrapped the
full 36-char workload ID in brackets: 44 chars against the kernel's
15-char interface limit (`IFNAMSIZ`), so link creation failed with
`ERANGE` on any machine before the netns or bridge was consulted.
Names are now `veth-` plus the first 8 ID chars plus the end letter,
14 chars, mirroring the WireGuard naming in the same package. See
`VethHostName` in `internal/cn/veth.go`.

Together the session carried one container from pull through init to
networking. The next failure, when it comes, will sit past link
creation (bridge attach, address, netns move): read the stage from
the new line, fix that stage, and expect the one after.

## Where it lives

Bundle placement (`bundleDirPath` in
`internal/reconciler/reconciler.go`), rule construction and the
node/rule split (`internal/cr/bundle.go`), the bundle-time rule
summary in the reconciler, program attachment by runc's device
cgroup controller. The mount table belongs to the machine, not to
Concord, which is exactly why this page exists.
