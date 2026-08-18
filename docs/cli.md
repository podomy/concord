# CLI Reference

Concord uses a noun-first command structure: `concord <noun> <action> [flags]`.

---

## Starting the Daemon

```bash
# Start the node daemon in the foreground
concord
# or explicitly:
concord daemon
```

---

## Workload Commands

### Run a Workload
```bash
concord workload run [flags] <image> [command...]
```

**Flags:**
| Flag | Short | Default | Description |
| :--- | :--- | :--- | :--- |
| `--port` | `-p` | `""` | Port mapping: `host:container` (e.g. `8080:80`) |
| `--env` | `-e` | `[]` | Environment variables in `KEY=VAL` format (repeatable) |
| `--restart` | | `always` | Policy: `always`, `never`, `on_failure` |
| `--cpu` | | `1024` | CFS CPU shares (`1024` = 1 core) |
| `--memory` | | `0` | Memory limit in MB (`0` = unlimited) |
| `--health-path` | | `/health` | HTTP endpoint for health checks |
| `--health-action` | | `restart` | Action on failure: `restart` or `signal` |

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
concord workload inspect 4b8d7a12
```

Returns the full JSON specification for the workload.

---

### Stop a Workload
```bash
# Stops container and writes a tombstone event to the journal
concord workload stop 4b8d7a12
```

---

## Node Commands

### List Cluster Nodes
```bash
concord node list
```

**Output:**
```
NODE ID                                ADDRESS             STATE    WIREGUARD PUBLIC KEY
a1b2c3d4-e5f6-7890-abcd-ef1234567890   192.168.1.10:17946  alive    +abc123xyz...
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