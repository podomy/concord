# Overview

Concord is a coordination layer for distributed systems with unreliable, intermittent, or partitioned connections.

Traditional distributed systems freeze or fail when machines lose network access because they depend on a central truth or synchronous quorums (like etcd/Raft). Concord is built from the ground up for environments where networks are degraded, intermittent, or absent. Every node operates independently from its local journal and synchronizes state deterministically whenever connections become available.

---

## Core Philosophy

1. **Local-first execution**: A node always trusts its local journal to execute workloads, even with zero peers reachable.
2. **Deterministic reconciliation**: State is an append-only event log. There is no central master or hidden database.
3. **Zero daemon bloat**: Direct OCI/runc container execution with cgroups isolation.
4. **Autonomous mesh**: Encrypted peer-to-peer WireGuard mesh with SWIM gossip discovery.

---

## Three Primitives

* **Journal**: An append-only log of immutable events (`workload.spec`, `workload.tombstone`, `node.started`).
* **Workloads**: Declarative specifications defining execution parameters, environment, ports, and health checks.
* **Mesh**: Automatic WireGuard tunnels connecting nodes with mutual TLS authentication.

---

## Next Steps

* [Architecture](architecture.md) -> How Concord works under the hood.
* [CLI Reference](cli.md) -> Command line commands and usage.
* [Go SDK](sdk.md) -> Programmatic workload management.
* [Deployment](deployment.md) -> Running nodes and clustering.
