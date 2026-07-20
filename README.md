<p align="center">
  <img src="assets/concord-robot-transparent.png" alt="Concord" width="15%">
</p>

<h1 align="center">Concord</h1>

<p align="center">
  <a href="https://github.com/podomy/concord/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/podomy/concord/ci.yml?label=linux" alt="Linux"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/podomy/concord" alt="License"></a>
</p>

Concord is a runtime and control layer for running software across machine
fleets in environments with poor connectivity and conditions that degrade
electronics and communication, such as space, remote terrain,
underground sites, or the sea. Concord is built for clusters that segment,
keep operating locally, and meet again later.

Concord is designed for mathematical consistency. Each segment can keep
operating from local knowledge, and when segments meet again their state
is reconciled by explicit rules instead of a hidden central truth.

## What Concord Is For

- Machines that must keep working when the network goes away
- Fleets split across partitions that reunite later
- Environments with no reliable connection: space, sea, remote terrain, underground

## Kubernetes Compatibility

**Concord is not standard Kubernetes.** It may support Kubernetes-like
deployment workflows, but it does not promise a coherent cluster
network, a central control plane, or one always-current source of truth.
**If your software needs one live global truth, Concord is the wrong place to
run it.** Cluster segmentation is expected. Local operation and eventual consistency are part of the model.

## TODO

### Mesh foundation (in progress)

- [ ] Wire libcontainer for workload isolation
- [ ] Workload reconciliation from journal events (replay for state rebuild)

### Future segments

- [ ] **Strong consistency within a segment** — Raft group (3-5 nodes) for linearizable operations: workload assignment, distributed locks. Cross-segment stays eventually consistent.
- [ ] **Workload assignment to segment** — `Spec.SegmentID` tracks segment, not pinned node. Segment reassigns on node loss.
- [ ] **Distributed transactions within a segment** — Atomic multi-node operations through the segment Raft group.
- [ ] **Multi-segment hierarchy** — Segment representatives form a higher-level mesh. Scales beyond single memberlist limits (~thousands of nodes).
- [ ] **Global ordering / causality** — Vector clocks per event for causal ordering across segments.
- [ ] **Real-time / sub-second paths** — Local processing only for latency-sensitive workloads; cross-node sync stays at 5s interval.
- [ ] **Conflict-free mutable resources** — CRDT strategies for specific state types that two segments can both mutate.
- [ ] DNS state reconciliation between nodes + what happens when a bubble/segment reconnects. But the problem is connected, if we implement that, we have to implement reconcilliation for EVERYTHING at once. That is why this comes last.
- [ ] Peer sync catch-up for long journals (not needed for current stub/mesh path; do after real journal pull/apply works):

  | Piece               | Role                                             |
  | ------------------- | ------------------------------------------------ |
  | Persist watermarks  | Restart resumes mid-history, not from year 0     |
  | Paging (`Limit`)    | Already in pull loop — never one unbounded dump  |
  | Optional: snapshots | State as of T + events after T (later)           |
  | Idempotent apply    | Replay after restart does not corrupt (event id) |

- [ ] Extension friendliness (after real journal sync; core is solid but not plugin-first yet):

  | Gap                                  | Effect                                          |
  | ------------------------------------ | ----------------------------------------------- |
  | Runtime is one hard-wired `Run()`    | No registry of "start these services"           |
  | Sync handler stub inside `transport` | No pluggable sync backend / apply pipeline      |
  | Pull loop watermarks only in RAM     | No store interface for durable cursors          |
  | Event types are free strings         | No typed extension catalog                      |
  | No SDK / extension API surface       | External code cannot hang off lifecycle cleanly |
  | Tight package coupling via runtime   | Hard to ship optional modules                   |

## Documentation

- [Design](./DESIGN.md)
- [Commit message format](./COMMITS)
- [Contributor license agreement](./CLA)

## License

Concord is distributed under the GNU Affero General Public License v3.0 or
later. See [LICENSE](./LICENSE).
