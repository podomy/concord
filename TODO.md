# TODO

## Mesh foundation (in progress)

- [ ] Wire libcontainer for workload isolation
- [ ] Workload reconciliation from journal events (replay for state rebuild)

## Future segments

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
