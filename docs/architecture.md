# Architecture & Data Flow

Concord is a decentralized coordination engine. State is driven by an append-only event log, projected into local views, reconciled against container runtimes, and synchronized across nodes over an encrypted mesh.

---

## Data Flow Diagrams

### 1. Workload Submission Flow

```
CLI / Go SDK
     │
     │ (1) POST /workload/submit
     │     (JSON Workload Spec)
     ▼
Unix IPC Server
(~/.config/concord/concord.sock)
     │
     │ (2) Record "workload.spec"
     │     Event
     ▼
Append-Only Journal
(journal.jsonl)
     │
     │ (3) Deterministic
     │     Projection
     ▼
bbolt KV Views
(Workloads, EventsByID, ByNode)
```

### 2. Local Workload Reconciliation & Execution Flow

```
bbolt KV Views (Desired State)
     │
     │ (4) Active Workload Specs
     ▼
Reconciler Loop
     │
     ├──► (5) Fetch Image
     │         │
     │         ▼
     │    Embedded OCI Registry
     │    (Zot localhost:8444)
     │
     └──► (6) Lifecycle Control
               │
               ▼
          Container Runtime
          (internal/cr)
               │
               ├──► cgroups (CPU/Mem)
               │
               ├──► runc (Namespaces)
               │
               ├──► Bridge & veth (concord0)
               │
               └──► Health Check (/health)
```

### 3. Peer Discovery & WireGuard Mesh Flow

```
Node Discovery
     │
     ├──► mDNS (LAN Multicast)
     │         │
     ├──► SWIM Gossip (UDP :17946)
     │         │
     └──► DNS Server (SRV/A :15353)
               │
               ▼
     Peer Memberlist
               │
               │ (7) Exchange WG Keys & IPs
               ▼
     WireGuard Mesh (internal/cn)
     (Flat Encrypted P2P Overlay)
```

### 4. Cross-Node State & Image Replication Flow

```
[ Remote Node B ]
Transport Server & Registry
       │
       │ (8) Noise Pull Events
       │ (10) P2P Image/Blob Sync
       │ (Over WireGuard Mesh)
       ▼
[ Local Node A ]
Peer Sync Loop (internal/peersync)
       │
       ├──► (9) Missing Events
       │         │
       │         ▼
       │    Local Journal (journal.jsonl)
       │         │
       │         ▼
       │    Local bbolt Views
       │         │
       │         ▼
       │    Reconciler ──► runc
       │
       └──► (10) Missing Blobs
                 │
                 ▼
            Embedded OCI Registry
            (localhost:8444)
```

---

## Underlay vs overlay

Concord uses two disjoint IPv4 spaces.

**Underlay** is the host NIC. Memberlist, mDNS, and the Noise
transport use it. Join targets are underlay addresses. Peers
dial whatever `ResolveAdvertise` published, never `cn0`.

**Overlay** is `10.0.0.0/16`. Each node has `cn0` at
`10.0.{index}.1/24` and allocates containers in that `/24`.
WireGuard carries peer `/24`s between nodes. Memberlist
must not advertise `cn0`, `wg-*`, or any `10.0.0.0/16`
address. Joining an overlay IP on a node that also has
`cn0` is a local TCP connect, not a peer.

Resonance simulator attaches a tun as the
underlay NIC. That tun must not use `10.0.0.0/16`. Use a
disjoint prefix such as `192.168.100.0/24`, one address per
node. Overlay stays `10.0.0.0/16` inside each netns. If the
tun is `10.0.0.1` / `10.0.0.2`, Concord cannot tell underlay
from `cn0`, Join hits the local bridge, and membership
stays at one node.

---

## Scheduling

In each connected segment, the node with the lowest UUID string is the leader. It assigns unassigned workloads to the alive peer with the lowest pressure, breaking ties by fewest active workloads. When segments reunite, journals sync and state converges. Orphaned workloads are reassigned as described below. Pressure sampling and placement order are detailed in `docs/metrics.md`.

---

## Orphan reassignment

An orphan is a live spec (`Removed=false`) with `AssignedNodeID != Nil` whose owner is absent from the alive membership set. Only `NodeStateAlive` counts as alive; suspect, dead, left, and unknown all mark the owner gone. This favors fast failover: a flapping node can cause temporary duplicate execution across partitions, which the merge rules converge.

Only the segment leader acts, via `internal/reconciler/scheduler.go:scheduleWorkloads`, invoked leader-only from `RunLoop` on each tick. Each tick does exactly this:

1. Read members and build the alive set. Zero alive members is a no-op; nothing is assigned or reassigned.
2. Skip tombstones. A spec with `Removed=true` is never reassigned; a stop wins over any spec copy at any epoch.
3. Assign unassigned. A spec with `AssignedNodeID == Nil` is assigned to the `pickNode` choice (alive preferred, then lowest pressure, then fewest active workloads) and recorded as one new `workload.spec` event. Initial assignments carry epoch zero.
4. Skip healthy owners. A spec whose `AssignedNodeID` is in the alive set is left alone.
5. Reassign orphans. The owner is not alive, so the spec is pointed at the alive `pickNode` choice, `AssignmentEpoch` is incremented by one, and one new `workload.spec` event is recorded through `RecordEventAndLog`. The next tick reads the new copy from the view, so each orphan is rewritten once per epoch.
6. Guard the target. If the `pickNode` choice is not alive, the spec is skipped this tick.

Convergence follows the view rules in `internal/journalview/workloads.go` (`putEvent`, `keepStored`): the higher epoch always supersedes the stored copy, equal epochs fall back to byte-larger serialization, and tombstones dominate both. Concurrent reassignments from opposite partitions therefore converge on the highest epoch, and stale copies never resurrect a stopped workload. Execution stays at-most-once per partition: the reconciler runs a spec only on its assigned node.

An epoch is a reassignment count. Every time the scheduler moves a workload to a new node it goes up by one, and the stored copy with the largest count wins. Without it there is no way to tell which copy is newer: a reassignment differs from the stored copy only by node ID, so byte comparison would pick the larger UUID instead of the newer assignment, and the orphan would stall whenever the dead owner's ID happened to be larger.

---

## Reconciliation rhythm

Every loop in concord is a tick, not a subscription. Peer sync pulls
every 5 seconds and the reconciler compares desired against running
every 5 seconds, while health checks and pressure sampling run a local
500ms beat beside them (`internal/reconciler/fastpath.go`). Each pass
looks at the actual state, looks at the desired state, and closes the
gap. Nothing waits to be notified.

That is level-driven reconciliation, and it matches the failure model
on purpose. A missed notification, a crashed handler, a partition
healing mid-pass: the next tick picks all of it up without anyone
having to notice. Event-driven designs react faster but drift silently
when a delivery drops, which is why even Kubernetes pairs watches with
resync periods as a backstop. In a system built for partitions, event
delivery is exactly what cannot be relied on, so the tick is the
mechanism and five seconds is only its current tuning.

A faster loop does not change this, it nests inside it. Health checks
and watchdogs that need millisecond reactions run on a short local
beat over the same running state, and they stay silent: the fast loop
reads the journal never and writes it only on transitions, a container
crossing from healthy to sick, a restart decided. Raw observations stay
in memory; only their edges converge. The slow tick keeps converging
the world, the fast loop keeps the node alive between ticks.

---

## Conflict model

Concord sidesteps most conflicts by construction: every `workload run` mints a fresh unique ID, so concurrent submissions never disagree about the same key. Merge of distinct IDs is a union.

That union is a CvRDT (convergent replicated data type): the journal is a grow-only set of immutable events, merge is set union, and union is associative, commutative, and idempotent. Sync applies unknown IDs and skips known ones, so every segment converges to the same event set regardless of partition history or arrival order. Views are deterministic projections of that set: tombstone dominance, higher assignment epoch then byte-larger live specs, and highest-generation pins all resolve without wall-clocks.

The sidestep leaks in one place. Nodes re-record the spec (initial assignment writes its own `workload.spec` copy per ID, orphan reassignment writes a copy with the epoch incremented by one), so one workload ID can end up with several distinct spec events from different authors. These are conflicting same-ID writes, and the view must resolve them deterministically.

Landed rules in `internal/journalview/workloads.go` (`putEvent`):

* Tombstone dominance. A stored tombstone (`Removed=true`) is never replaced by a live spec copy, regardless of arrival order or epoch. A stop wins over any spec copy.
* Live-live tiebreak. Two live specs for one ID resolve by deterministic comparison: the higher `AssignmentEpoch` wins. Byte comparison runs only on an epoch draw, where the byte-larger serialization wins. A reassignment therefore always supersedes the stored copy it was derived from, and concurrent reassignments from opposite partitions converge on the highest epoch, regardless of arrival order.

All rules are order-independent: every node converges to the same stored copy no matter the sync arrival sequence. No wall-clock participates in these paths. The epoch is a per-workload generation counter, same family as the key-pin generation, incremented only by the scheduler on reassignment; initial assignments carry epoch zero.

Assignment rides in `workload.spec` itself rather than a separate event
type, deliberately. A separate assignment stream would not remove the
conflict, it would move it: opposite partitions still write concurrent
claims for one spec id, and those still need highest-epoch-wins to
converge. What it would add is a second stream to merge, a join across
streams in the scheduler and reconciler, and a stop-wins question
spanning two orderings instead of one three-line check. Duplicate spec
copies are the cheaper wart: storage is cheap, merge stays
deterministic, and old journals need no migration.

---

## Object model

Fleets are single tenant: one operator, one trust domain, every node provisioned with the same CA and gossip key. Nodes cooperate rather than distrust each other. Isolation between customers happens at fleet level, separate meshes, not inside one mesh: tenancy within a mesh would demand per-object auth, quotas, and tenant-aware scheduling, which is the CP machinery Concord rejects. Mutually distrustful workloads sharing one mesh would be a different system, not a new object kind.

The workload is the only object. There is one spec (`internal/workload/workload.go:Spec`), one assignment flow, and one convergence story.

### One spec

New needs arrive as workload fields. Image, command, env, resources, ports, health, restart policy, and timeouts already live there. Volume mounts, network policy, and secret references belong there too.

### Why not one kind per concern

In comparison, Kubernetes follows the CP model: a central API server backed by an etcd quorum arbitrates a registry of specialized kinds, each with its own controller and lifecycle.

* Deployments describe desired pods.
* PVs and PVCs separate storage provisioning from consumption, so volumes outlive their claimants.
* Services and Ingress carve networking and edge routing into independently owned objects.
* ConfigMaps and Secrets split configuration out of the pod spec.

That granularity serves multi-tenant clusters where different teams own different concerns and a quorum is always reachable to serialize them. Concord has neither pressure: single tenant, whole workloads pinned to nodes, local-first execution with no quorum to appeal to.

Each additional kind would multiply the expensive surfaces: its own view with merge rules, scheduler handling, reconciler branch, IPC and SDK endpoints, and partition coverage in Resonance. Convergence machinery is owed per kind, and the assignment epoch is what one installment of that debt looks like.

### What dies with the workload

A stop reaps everything the workload owns, storage included. The tombstone (`Removed=true`) dominates every live copy at any epoch, and nothing attached to the workload survives it. No storage survives tombstoning the workload, and this follows from failover rather than simplicity: a workload can be reassigned to another node at any time, so node-local data that outlives it strands itself where nothing runs. Making such data useful would require replicated volumes, a consistency subsystem of its own that contradicts local-first. Durable needs already have homes: shared mission state belongs in the journal where it converges, images and models in the registry, logs streamed out. What remains is scratch space, workload-scoped by nature, provisioned with the workload and deleted with it.

### When a kind splits out

A new kind splits out only when it has an independent lifecycle and different merge semantics, meaning state that must survive its workload's tombstone under rules stop-wins cannot express. No such case exists today. Until one arrives with a concrete use, bake it into the workload.

---

## The journal

The journal is one file, `journal.jsonl` under `XDG_CONFIG_HOME/concord`,
created with owner-only permissions. Every event is one JSON line:
marshal, append the line plus its newline in a single write, fsync before
return. Appends serialize on a mutex and open with `O_APPEND`, so
concurrent writers never interleave mid-line and every recorded byte
offset stays a line boundary for the life of the file. Readers open
separate read-only handles; the serving index seeks them by offset.

That encoding is more than enough and stays that way on purpose.
Coordination events are small and infrequent, so parse cost and verbosity
never matter, while grep-able text keeps failure dumps and Resonance
replays readable.

One real limit: lines cap at 10MB (`maxJournalLineBytes` in
`internal/journalreader/jsonl_reader.go`), far above the 64KB scanner
default, so a single oversized payload fails loudly instead of
mysteriously. High-rate telemetry never belongs here anyway: that
traffic wants last-value semantics on a data plane, meaning each topic
keeps only its latest reading and a newer arrival simply overwrites it.
No history, no replay, late duplicates harmless by construction. The
journal is the opposite, total history with deterministic replay, which
is exactly what coordination needs and exactly what telemetry would
drown in.
