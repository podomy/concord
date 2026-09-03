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
               └──► Health Checker (/health)
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
       │ (8) mTLS Pull Events
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

**Underlay** is the host NIC. Memberlist, mDNS, and the HTTPS
transport use it. Join targets are underlay addresses. Peers
dial whatever `ResolveAdvertise` published, never `cn0`.

**Overlay** is `10.0.0.0/16`. Each node has `cn0` at
`10.0.{index}.1/24` and allocates containers in that `/24`.
WireGuard carries peer `/24`s between nodes. Memberlist
must not advertise `cn0`, `wg-*`, or any `10.0.0.0/16`
address. Joining an overlay IP on a node that also has
`cn0` is a local TCP connect, not a peer.

Simulators (including Resonance) attach a tun as the
underlay NIC. That tun must not use `10.0.0.0/16`. Use a
disjoint prefix such as `192.168.100.0/24`, one address per
node. Overlay stays `10.0.0.0/16` inside each netns. If the
tun is `10.0.0.1` / `10.0.0.2`, Concord cannot tell underlay
from `cn0`, Join hits the local bridge, and membership
stays at one node.

---

## Scheduling

In each connected segment, the node with the lowest UUID string is the leader. It assigns unassigned workloads to the peer with the fewest active workloads. When segments reunite, journals sync and state converges.
