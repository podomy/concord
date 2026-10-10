# IPC Reference

The daemon serves plain HTTP over a Unix domain socket. Anything that
speaks HTTP can drive Concord directly: `curl --unix-socket`, Python,
Rust, the Go SDK (which is exactly such a client, see `sdk/client.go`).
The CLI is another such client. There is no separate protocol to learn
and no IDL: endpoints, JSON shapes, and status codes below are the
contract.

## Connecting

Socket: `~/.config/concord/concord.sock` (`$XDG_CONFIG_HOME` overrides
the base dir), owner-only (`0600`), local machine only. No auth header,
no TLS: filesystem permissions are the auth. If you can read the
socket, you can submit workloads; keep that in mind for multi-user
hosts.

```bash
SOCK="$HOME/.config/concord/concord.sock"

# List workloads
curl -s --unix-socket "$SOCK" http://localhost/v1/workloads | jq .

# Submit one
curl -s --unix-socket "$SOCK" http://localhost/v1/workloads \
  -H 'Content-Type: application/json' \
  -d '{"image":"nginx:alpine","restart":"always"}' | jq .
```

Python (standard library only):

```python
import json, socket

SOCK = f"{os.environ['HOME']}/.config/concord/concord.sock"

def ipc(method, path, body=None):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.connect(SOCK)
    raw = json.dumps(body).encode() if body is not None else b""
    req = (
        f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n"
        f"Content-Type: application/json\r\nContent-Length: {len(raw)}\r\n"
        "Connection: close\r\n\r\n"
    ).encode() + raw
    s.sendall(req)
    resp = b""
    while chunk := s.recv(65536):
        resp += chunk
    s.close()
    _, _, rest = resp.partition(b"\r\n\r\n")
    return json.loads(rest)

print(ipc("GET", "/v1/nodes"))
```

Host name in the URL is irrelevant (`http://localhost` is convention;
the SDK uses `http://unix`). Only the path matters.

## Conventions

* JSON both ways; `Content-Type: application/json` on bodies.
* Errors are `{"error":"message"}` with a non-2xx status. Machine-readable
  only by status code; messages are human text and may change.
* UUIDs are strings in URLs and bodies. `POST /v1/workloads` mints one
  when `id` is absent.
* `GET /metrics` is Prometheus text exposition, not JSON.
* `DELETE` returns `204 No Content` with an empty body.
* Everything is local-node scope: submissions converge fleet-wide
  through the journal, readings (stats, trail, metrics) answer for this
  node only.

## Endpoints

### `POST /v1/workloads`: submit a workload

Body is a workload spec; only `image` is required. Returns `201` with
the assigned id:

```json
// request
{"image":"nginx:alpine","restart":"always","host_port":8080,"container_port":80}
// response 201
{"id":"4b8d7a12-..."}
```

Full spec fields: `id`, `image`, `command[]`, `env{}`, `resources`
(`cpu_shares`, `memory_mb`), `restart` (`always`, `never`,
`on_failure`), `host_port`, `container_port`, `stop_timeout_seconds`,
`health_action` (0 restart, 1 signal), `health_path`. `400` on bad JSON
or missing image. The spec commits to the journal and converges; placement
follows the scheduler, not this call.

### `GET /v1/workloads`: list active workloads

`200 {"workloads":[...]}` with full specs. Tombstoned (stopped) ones are
excluded, not marked.

### `GET /v1/workloads/{id}`: inspect one workload

`200` with the full spec. `400` on malformed UUID, `404` when unknown
or stopped.

### `GET /v1/workloads/{id}/stats`: live utilization

In-memory sampler readings, never the journal. `404` when never sampled
(still starting), already stopped, or the sampler is missing:

```json
{
  "cpu_percent": 25, "mem_percent": 50, "mem_usage_mb": 512,
  "mem_limit_mb": 1024, "avg_cpu_percent": 22, "avg_mem_percent": 48,
  "node": {"id": "9f2c1a44-...", "cpu_percent": 10, "mem_percent": 20, "disk_percent": 30}
}
```

`avg_*` fold the 5s trend window; the rest is the latest beat. `node`
is always the local node: stats exist only where the workload runs.

### `DELETE /v1/workloads/{id}`: stop a workload

Commits a tombstone; the reconciler reaps the container. `204` empty on
success, `404` when unknown or already stopped.

### `GET /v1/nodes`: list cluster nodes

Memberlist view with gossiped metadata. Empty list (not 503) when the
peer service is missing:

```json
{"nodes": [{
  "id": "a1b2c3d4-...", "address": "192.168.1.10:17946", "state": "alive",
  "wireguard_public_key": "+abc123xyz...",
  "cpu_percent": 10, "mem_percent": 20, "disk_percent": 30,
  "lat": 47.6, "lon": 8.9, "anchor": true
}]}
```

`state` is `alive`, `suspect`, `dead`, `left`, or `unknown`. Pressure
fields are 0-100 utilization; `lat`/`lon` are 0 when unknown.

### `GET /v1/nodes/self/trail`: this node's trail

Ring contents since boot, oldest-first. `503` when the sampler is
missing:

```json
{"trail": [{"at": "2026-10-09T10:00:00Z", "lat": 47.6, "lon": 8.9}]}
```

Empty trail is `{"trail": []}`, not an error. Full mission history is
the local `track.jsonl`, not this endpoint; see `docs/trail.md`.

### `PUT /v1/nodes/self/position`: apply a position fix

The only position writer. Persisted for reboot, gossiped, recorded to
trail and track log in one operation:

```json
// request
{"lat": 47.6, "lon": 8.9}
```

`200` echoes the fix. `400` on bad JSON or out-of-planet coordinates
(previous fix kept, nothing applied); `503` without sampler or peer
service. Exact `0,0` is allowed and means unknown. See `docs/trail.md`.

### `GET /metrics`: Prometheus exposition

Point readings from memory for scrapers: node trio, position gauges,
per-workload series. `Content-Type: text/plain; version=0.0.4`.
`503` when the sampler is missing. Details in `docs/metrics.md`.
