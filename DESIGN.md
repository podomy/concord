# Design: Capabilities and Boundaries

## What Concord can do

- **Operate in fully disconnected environments** — Each node is fully
  autonomous. No central coordination needed. Works in submarines, space,
  disaster zones, and air-gapped networks.

- **Survive network partitions** — No split-brain problem because there is no
  leader and no quorum. Every partition makes progress independently.

- **Graceful catch-up** — When connectivity restores, reconciliation catches up
  naturally. Watermarks ensure efficient incremental sync.

- **Deterministic state reconstruction** — The journal is the source of truth.
  Any node can reconstruct state by replaying events. Audit trail is built in.

- **Eventual convergence** — Given enough connectivity, all nodes converge to the
  same set of events. Vector clocks can ensure causal ordering.

- **No SPOF** — No coordinator, no leader, no single registry. mDNS, gossip, and
  pull-only reconciliation.

- **Simple operational model** — Add a node. It finds peers, syncs, and
  converges.

## What Concord cannot do (fundamental boundaries)

- **Strong consistency across segments** — You cannot get read-your-writes,
  linearizability, or any form of cross-segment consensus. Events are eventually
  consistent by design. Segments that need linearizable operations must use a
  local Raft group for those specific operations.

- **Global ordering** — Events have causal order (via vector clocks) but no
  total order across segments. Events on different segments are independent
  unless causally linked.

- **Real-time guarantees** — Pull-based with a 5 s interval means second-scale
  latency at best. For sub-second reactions, only local processing works.

- **Conflict-free mutable resources** — Containers are mutable state. Two nodes
  cannot run the same container on the same port. CRDTs do not apply to physical
  resources like PIDs or ports.

- **Distributed transactions** — No distributed transactions and no atomic
  commits across nodes. Each node commits to its own journal independently. In
  the future, a segment-local Raft group can provide atomic multi-node
  operations within that segment.

- **Total workload migration** — Workloads are assigned to a segment, not pinned
  to a node. The segment reassigns on node loss. If a whole segment dies, the
  workload is lost.

- **Large-scale consensus-free meshes** — A single memberlist mesh is bounded to
  hundreds of nodes. Scaling to thousands requires a multi-segment hierarchy.

## What future layers can improve (but not eliminate)

- Causal ordering across events (determine which event happened before which)
- Detect concurrent updates (for example, two nodes both assigned the same
  workload)
- Provide deterministic tiebreakers (node ID comparison for conflicts)
- Richer conflict resolution strategies per workload type

None of these turn an eventually consistent system into a strongly consistent
one.
