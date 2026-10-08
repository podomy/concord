# CLI Reference

Concord uses a noun-first command structure: `concord <noun> <action> [flags]`.

---

## Starting the Daemon

```bash
# Start the node daemon in the foreground
concord
```

---

## Workload Commands

### Run a Workload
```bash
concord workload run [flags] <image> [command...]
```

**Flags:**
```
--port, -p (default ""): port mapping host:container (e.g. 8080:80)
--env, -e (default []): environment variables in KEY=VAL format (repeatable)
--restart (default always): policy always, never, on_failure
--cpu (default 1024): CFS CPU shares (1024 = 1 core)
--memory (default 0): memory limit in MB (0 = unlimited)
--health-path (default /health): HTTP endpoint for health checks
--health-action (default restart): action on failure restart or signal
```

**Examples:**
```bash
# Run nginx with port mapping
concord workload run -p 8080:80 nginx:alpine

# Run with environment variables and memory limit
concord workload run -e ENV=prod -e DB_HOST=10.0.0.5 --memory 512 redis:alpine

# Run with custom entrypoint command
concord workload run alpine:latest /bin/sh -c "while true; do echo hello; sleep 5; done"
```

---

### List Workloads
```bash
concord workload list
```

**Output:**
```
ID          IMAGE              PORTS       RESTART   HEALTH
4b8d7a12    nginx:alpine       8080:80     always    /health
9c1e3f80    redis:alpine       -           always    -
```

---

### Inspect a Workload
```bash
# Supports full UUIDs or 8-character prefixes
concord workload inspect <id>
```

Returns the full JSON specification for the workload.

---

### Workload Utilization
```bash
# Live CPU and memory readings sampled on the fast beat.
# Absent readings (never sampled, already stopped) report not found.
concord workload stats <id>
```

**Output:**
```
Workload 4b8d7a12
  CPU     25% (avg 22%)
  Memory  50% (512 of 1024 MB, avg 48%)
Node 9f2c1a44
  CPU     10%
  Memory  20%
  Disk    30%
```

Labeled lines with units everywhere; the memory line names the limit so
the percent means something (`512 MB` alone when unlimited). The node
block is the local node's trio: stats exist only where the workload
runs, so the local node is always the relevant context.

Percents are 0-100 utilization: CPU from `/proc/stat` deltas between
beats (first sample reports 0), memory as used over total, disk as used
blocks over total on the concord data disk. A percent says how full the
resource is, never how much room it has; the scheduler reads fullness
from the trio and headroom from the node's totals together. Full
definitions live in `docs/metrics.md`.

---

### Stop a Workload
```bash
# Stops container and writes a tombstone event to the journal
concord workload stop <id>
```

---

## Node Commands

### List Cluster Nodes
```bash
concord node list
```

**Output:**
```
NODE ID                                ADDRESS             STATE    WIREGUARD PUBLIC KEY    PRESSURE
a1b2c3d4-e5f6-7890-abcd-ef1234567890   192.168.1.10:17946  alive    +abc123xyz...           23%
```

PRESSURE is the highest of the node's gossiped CPU, memory, and disk
utilization: the number the scheduler places by. Each percent is 0-100
utilization of that resource, how full it is rather than how much room
remains. Full definitions live in `docs/metrics.md`.

---

### Rotate the Noise Key
```bash
concord node rotate-key
```

Deletes the static key and bumps the generation counter. Local file ops
only, no daemon involved: restart the daemon to apply. The full
procedure and the pin rules live in `docs/noise.md`.

---

## Metrics

### Scrape Node Metrics
```bash
concord metrics
```

Prints sampler state in Prometheus text exposition format: per-workload
CPU, memory, and usage plus the node trio. Point readings from memory,
scraped by anything that speaks the format.

**Output:**
```
# HELP concord_node_cpu_percent Node CPU utilization percent.
# TYPE concord_node_cpu_percent gauge
concord_node_cpu_percent 10
# HELP concord_workload_cpu_percent Workload CPU utilization percent.
# TYPE concord_workload_cpu_percent gauge
concord_workload_cpu_percent{id="4b8d7a12-..."} 25
```

---

## Autocompletion

```bash
# Bash
source <(concord completion bash)

# Zsh
source <(concord completion zsh)

# Fish
concord completion fish | source
```