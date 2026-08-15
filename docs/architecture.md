# Architecture & Data Flow

Concord is a decentralized coordination engine. State is driven by an append-only event log, projected into local views, reconciled against container runtimes, and synchronized across nodes over an encrypted mesh.

---

## Data Flow Diagram

```
                                CONCORD DATA FLOW

[ 1. Workload Submission Flow ]

    CLI / Go SDK
         │
         │ (1) POST /workload/submit (JSON Workload Spec)
         ▼
    Unix IPC Server  (~/.config/concord/concord.sock)
         │
         │ (2) Record "workload.spec" Event
         ▼
    Append-Only Journal (journal.jsonl)
         │
         │ (3) Deterministic Projection
         ▼
    bbolt KV Views  (Workloads, EventsByID, EventsByNode)


[ 2. Local Workload Reconciliation & Execution Flow ]

    bbolt KV Views (Desired State)
         │
         │ (4) Active Workload Specs
         ▼
    Reconciler Loop
         │
         ├──► (5) Fetch Image ────────► Embedded OCI Registry (Zot localhost:8444)
         │
         └──► (6) Lifecycle Control ──► Container Runtime (internal/cr)
                                             │
                                             ├──► cgroups (CPU / Memory limits)
                                             ├──► runc (Linux namespaces)
                                             ├──► Bridge & veth (concord0 NAT)
                                             └──► HTTP Health Checker (/health)


[ 3. Peer Discovery & WireGuard Mesh Flow ]

    Node Discovery
         │
         ├──► mDNS (Local LAN Multicast) ────────┐
         │                                       ▼
         ├──► SWIM Gossip (UDP port 17946) ─────► Peer Memberlist
         │                                       ▲        │
         └──► Embedded DNS Server (SRV/A :15353) ┘        │ (7) Exchange WG Keys & IPs
                                                          ▼
                                                  WireGuard Mesh (internal/cn)
                                                  (Flat Encrypted P2P Overlay)


[ 4. Cross-Node State & Image Replication Flow (Over WireGuard Mesh) ]

         Node A (Local)                                            Node B (Remote)
    ┌──────────────────────┐                                 ┌──────────────────────┐
    │  Peer Sync Pull Loop │                                 │   Transport Server   │
    │  (internal/peersync) │◄──── (8) mTLS Pull Events ──────┤ (internal/transport) │
    └──────────┬───────────┘                                 └──────────┬───────────┘
               │                                                        │
               │ (9) Write Missing Events                               │ Reads From
               ▼                                                        ▼
         Local Journal                                            Remote Journal
               │                                                        │
               ▼                                                        ▼
         Local Views ──► Reconciler ──► runc                      Remote Views
               ▲                                                        ▲
               │                                                        │
    ┌──────────┴───────────┐                                 ┌──────────┴───────────┐
    │ Embedded OCI Registry│◄──── (10) P2P Image / Blob Sync ┤ Embedded OCI Registry│
    │ (internal/or :8444)  │                                 │ (internal/or :8444)  │
    └──────────────────────┘                                 └──────────────────────┘
```

---

## The Four Data Pipelines

### 1. Workload Submission Pipeline

1. The developer CLI or Go SDK connects to `~/.config/concord/concord.sock` and issues an HTTP POST request with the workload specification.
2. The IPC handler wraps the specification in an immutable `workload.spec` journal event.
3. The event is appended to `journal.jsonl` and immediately projected into local `bbolt` key-value view tables (`Workloads`, `EventsByID`).

### 2. Reconciliation & Container Execution Pipeline

1. **Segment Leader Scheduling**: In each reachable network segment, the node with the lowest lexicographical UUID acts as the segment leader. The leader assigns unassigned workloads to the peer with the fewest active workloads.
2. The reconciler continuously reads the desired active workload specs assigned to this node from the `Workloads` view.
3. It queries the local container runtime (`internal/cr`) for currently running containers.
4. For missing workloads, it pulls the container image layers from the local embedded **Zot OCI registry** (`localhost:8444`).
5. It configures Linux cgroups (CPU shares, memory limits), creates network namespaces with `veth` pairs attached to `concord0`, and spawns the container via `runc`.
6. It runs periodic HTTP health checks against the workload.

### 3. Peer Discovery & Mesh Pipeline

1. Nodes find each other via **mDNS** (local subnet broadcast) and **SWIM gossip** (UDP port `17946`).
2. The **Embedded DNS Server** (`internal/dnsserver`) answers queries on port `15353`, returning SRV and A records (`<UUID>.concord.local.`) for cross-subnet discovery.
3. Nodes exchange WireGuard public keys and endpoint addresses during gossip.
4. The `TunnelManager` automatically provisions peer tunnels, establishing a secure, flat network overlay.

### 4. Distributed Synchronization Pipeline

1. Every node runs a background peer sync loop (`internal/peersync`).
2. It connects to discovered peers over mutual TLS (mTLS) authenticated by the shared cluster CA.
3. It asks peers for journal events it does not yet have in its local log.
4. Missing events are appended to `journal.jsonl` and projected into local views.
5. If an event contains a new workload or a stop tombstone, the local reconciler automatically converges local containers to match.
6. The embedded Zot registry monitors active peers and pulls missing container image layers peer-to-peer across the WireGuard network.
