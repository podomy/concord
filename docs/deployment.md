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
Description=Concord Node
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/concord
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

All nodes in a Concord cluster authenticate each other over Noise on `:8443`. **Every single node in the cluster must be provisioned with the exact same Root Certificate Authority (`ca.crt` and `ca.key`).** There is exactly one CA, self-signed, with no intermediate or secondary CAs. The CA keypair serves exactly one purpose: the private key in `ca.key` signs node parcels, and the public key in `ca.crt` checks those signatures. Nothing else in the system uses either half. The certificate format is storage only: Concord never performs X.509 verification and never checks dates; `ca.crt` is just the carrier for the CA public key. Each signature covers one node's ID, key generation, and static public key, and that signature is the node's membership credential.

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
2. It generates a long-term X25519 static keypair (`~/.config/concord/noise/secret.key`, created once and reused) and signs the binding of node ID, key generation, and public key with the shared CA.
3. It gossips the public key and generation; the CA signature travels inside each Noise session.

Because every binding is signed by the same Root CA, all nodes can mutually verify each other's identity across the mesh. Verification is by CA signature only; there are no certificates and no validity windows, so nodes need no wall-clock agreement.

Key rotation means deleting `secret.key`, bumping `noise/generation`, and restarting so boot generates a fresh key and the new binding is signed and gossiped. `concord node rotate-key` performs the delete and the bump; restart the daemon to apply. The bump is the load-bearing step, a fresh key at the same generation trips the pin alarm instead of splitting the fleet. The highest generation seen for a node wins.

Peers pin the first valid key they see for each node ID. A different key at the same generation is rejected with a warning and the pin sticks, which is how partial state loss (new key, old counter) surfaces instead of silently splitting the fleet. Recover by bumping the generation, an intentional act.

## Gossip Encryption Key Provisioning

Memberlist gossip is AES-GCM encrypted with a cluster-wide pre-shared key. **Every node must hold the exact same key bytes.** Generate once per cluster and distribute alongside the CA files:

```bash
# Generate ONCE per cluster, then copy the same file to EVERY node:
openssl rand 32 > gossip.key
mkdir -p ~/.config/concord/memberservice
cp /path/to/shared/gossip.key ~/.config/concord/memberservice/secret.key
chmod 600 ~/.config/concord/memberservice/secret.key
```

The file must hold 16, 24, or 32 raw bytes (AES-128/192/256); Concord refuses to start otherwise. A mismatched key is indistinguishable from a network partition at the gossip layer, so on split-brain symptoms compare the `sha256` fingerprint each node logs at startup (`gossip key loaded`).

---

## Multi-Node Cluster Discovery

Concord nodes automatically discover each other over the local subnet using SWIM gossip (UDP port `17946`).

When a node starts:
1. It initializes its Noise identity from `~/.config/concord/noise/` and `~/.config/concord/certs/`.
2. It listens for gossip announcements from peer nodes on the local network.
3. Once discovered, nodes establish an encrypted WireGuard mesh and sync journal events over Noise.

No central master server, control plane, or external database is required.
