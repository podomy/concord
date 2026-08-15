# Deployment Guide

Concord runs on Linux edge nodes, servers, and embedded controllers.

---

## Prerequisites

- **Operating System**: Linux kernel 5.10+ (cgroups v2, network namespaces).
- **Architecture**: `amd64`, `arm64`, `armv7`, or `riscv64`.
- **Runtime**: Root or `CAP_SYS_ADMIN` privileges (required by `runc` for container namespacing).

---

## Installation

### 1. Download Pre-compiled Binary

```bash
# Example for Linux amd64:
curl -LO https://github.com/podomy/concord/releases/download/v1.0/concord-linux-amd64.zip
unzip concord-linux-amd64.zip
chmod +x concord-linux-amd64
sudo mv concord-linux-amd64 /usr/local/bin/concord
rm concord-linux-amd64.zip
```

### 2. Build from Source

```bash
go install github.com/podomy/concord@latest
```

---

## Running Concord as a Systemd Service

Create `/etc/systemd/system/concord.service`:

```ini
[Unit]
Description=Concord Fleet Node
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/concord daemon
Restart=always
RestartSec=3
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now concord
```

Check status:

```bash
systemctl status concord
```

## Cluster Trust & Certificate Authority (CA) Provisioning

All nodes in a Concord cluster authenticate each other via mutual TLS (mTLS). **Every single node in the cluster must be provisioned with the exact same Root Certificate Authority (`ca.crt` and `ca.key`).**

Before starting any Concord node for the first time, upload your cluster's shared CA files to its config directory (defaults to `~/.config/concord/certs`):

```bash
# Must be executed on EVERY node in the cluster:
mkdir -p ~/.config/concord/certs
cp /path/to/shared/ca.crt ~/.config/concord/certs/ca.crt
cp /path/to/shared/ca.key ~/.config/concord/certs/ca.key
chmod 600 ~/.config/concord/certs/ca.key
```

> **Custom Config Directory**: Concord adheres to the XDG Base Directory specification. You can override the base configuration directory by setting the `XDG_CONFIG_HOME` environment variable (e.g. `export XDG_CONFIG_HOME=/etc` will store certificates in `/etc/concord/certs`).

When Concord starts:
1. It verifies that the shared `ca.crt` and `ca.key` exist.
2. It automatically generates a unique node identity (`UUID`) and mints a local `node.crt` and `node.key` signed by the shared CA.
3. If pre-minted `node.crt` and `node.key` already exist alongside `ca.crt`, it reuses them directly.

Because every node is signed by the same Root CA, all nodes can mutually verify each other's identity across the mesh.

---

## Multi-Node Cluster Discovery

Concord nodes automatically discover each other over the local subnet using SWIM gossip (UDP port `17946`).

When a node starts:
1. It initializes its mutual TLS identity from `~/.config/concord/certs/`.
2. It listens for gossip announcements from peer nodes on the local network.
3. Once discovered, nodes establish an encrypted WireGuard mesh and sync journal events over mTLS.

No central master server, control plane, or external database is required.

---

## Fleet Trust & Update Propagation Model

* **Symmetric Trust**: Every node in a Concord cluster is equally trusted. There is no master, primary, or central coordinator. Every machine runs the exact same daemon binary and executes the exact same deterministic state rules.
* **Substation / Relay Updates**: You do not need network access to every node in the field to deploy an update. You simply submit the workload or configuration change to **any single node** (for example, a base station, depot gateway, or mobile relay).
* **Opportunistic Physical Sync**: When other nodes (e.g. rovers, drones, edge units) move into physical radio range or connect to the local network of that substation, they automatically discover each other. Journal events and container images propagate immediately across the peer mesh, and each node reconciles its containers to the new desired state without manual intervention.

---

## Segment Leader Election & Scheduling

Concord does not use complex Raft quorums or voting rounds that freeze during network partitions. Instead:

1. **Per-Segment Deterministic Leader**: In any connected segment or partition of nodes, the leader is elected deterministically as the reachable node with the lowest lexicographical UUID string.
2. **Autonomous Scheduling**: The elected leader of that segment inspects unassigned workloads and schedules them onto the node in that segment with the fewest active containers.
3. **Partition Resilience**: If a cluster segments into multiple isolated groups (e.g. a swarm splits into two valleys), each segment automatically elects its own local leader and continues scheduling and running workloads independently. When segments reunite, their journals sync and state converges.
