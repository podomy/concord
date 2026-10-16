# Changelog

## Unreleased

- Device scan passes only character and block devices: a FIFO under
  `/dev` used to reach the cgroup program and fail compilation for
  every container on that host. Pinned by test.

## v1.1.1 (2026-10-16)

Containers now start end to end. A nodev bundle directory used to
kill every container in init, and fixing that promoted three latent
start bugs in init order; all four are fixed and recorded. The full
story lives in `docs/nodev.md`.

**Container start**

- Diagnose the `/dev/null` denial: bundles on a `nodev` mount fail
  every device open with a line identical to a rule bug. New
  `docs/nodev.md` with the four-gate model, the errno discriminator,
  the elimination runbook, and placement rules.
- Commandless submits run the image default: empty spec command falls
  back to image entrypoint plus cmd instead of panicking init on an
  empty argv. Both empty is a logged build error. Contract in
  `docs/cli.md`.
- Failed starts retry: pull removes the stale rootfs instead of
  wedging on `already exists`, and a failed start destroys its created
  container so the next tick recreates.
- Veth names fit the kernel: `veth-` plus the first 8 ID chars plus
  the end letter (14 chars), inside the 15-char `IFNAMSIZ` limit.
  Full UUIDs failed `LinkAdd` with `ERANGE` on any machine.

**Devices and runtime**

- Register the eBPF device backend, so non-empty rule sets apply on
  both hierarchies.
- Scanned devices allow explicitly instead of defaulting to deny;
  runc default devices ride ahead of scanned sensors; every bundle
  carries a working null pair (covered by test).
- Explicit per-workload cgroup scope in bundles.
- Initial starts take the init path, with the libcontainer re-exec
  init dispatched in main.
- Debug logging for device rule summaries and every destroy
  initiation, behind a validated `--log-level` flag (debug, info,
  warn, error).

**Trail and docs**

- `RecordPosition` releases the sampler lock before the track fsync,
  so one slow disk never stalls pressure sampling or reads.
- Trail page corrected: real lock scope, seed-then-publish boot
  ordering.
- Nodev page framed as a recorded incident, including the three
  failures the fix promoted.
