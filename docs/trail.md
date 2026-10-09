# Position Trail

The trail answers one question: where did this node go. It is local
memory, not converged state. No coordinate ever enters the journal, and
the full trail never appears in logs. What converges fleet-wide is only
the latest position via gossip; history stays on the node that lived it,
plus whatever external scrapers recorded.

## What the trail is not

* Not in the journal. `workload.spec`, `workload.instance.*`,
  `workload.unhealthy`, `node.started`, peer events converge. Positions
  do not. Telemetry wants last-value semantics, the journal wants total
  history; mixing them drowns replay in readings that never converge.
  The same rule already keeps pressure samples out of the log.
* Not in the logs. Transitions log (`workload.unhealthy` logs its edge,
  assignments log theirs). Points do not log per move; a 500ms or 5s
  beat that logged every 10m step would spam the log it is supposed to
  explain.
* Not replicated. Peers see your latest gossip position (`node list`
  LAT/LON). They never see your trail. A partition heals by gossiping
  latest positions, never by merging histories.

## Write path

```
config.json position  ──5s poll──▶ SetPosition (gossip atomics)
                     │
                     └──10m gate──▶ RecordPosition (sampler ring)
```

1. Source is `~/.config/concord/config.json` `position`. Whoever moves
   the node rewrites the file: operator, provisioning, autonomy stack.
   There is no GPS daemon, no IPC setter today. The file is the API.
2. Every discovery round (5s) `refreshDiscoveryState` reloads the file,
   gossips a valid position via `SetPosition`, and hands the point to
   `sampler.RecordPosition`. Invalid (out-of-planet) positions are
   ignored on both paths; zeros gossip as unknown and never record.
3. `RecordPosition` appends only when the node moved at least 10m
   (`trailMoveKM`) from the last recorded point. The first valid point
   always records. Sub-threshold jitter (depot idle, GPS noise) records
   nothing.
4. Anchors reload on the same read. Position and anchor list always move
   together; a boot snapshot is never used after start.

Latency is therefore up to 5s from file rewrite to gossip plus the 10m
move gate to trail. Rewriting the file every GPS tick works but churns
disk and races `UpdateNodeConfig` writers; a dedicated position feed
(IPC setter, GPS socket) is the intended replacement. Until then the
file keeps one property a socket would not: the position survives
restarts by construction.

## Storage

* In-memory ring, `internal/node/trail.go`, owned by the `Sampler`
  (`internal/node/pressure.go`), guarded by the sampler mutex.
* Capacity 720 points. Full overwrites oldest; `Trail()` returns
  oldest-first. At the 10m minimum step 720 points cover 7.2km of roam;
  at real roam steps (100m+) they cover far more. Time span is
  unbounded; memory is flat forever.
* Ephemeral. The sampler is built fresh in `Run` on every boot. A
  daemon restart empties the trail. A partition does not: the process
  keeps running, keeps recording, keeps serving.

The recovery story follows from that split. A node that partitions and
keeps running replays its dark-period roam from memory with no journal
involved. A node that restarts has no roam to replay; its trail starts
empty and its scrapers hold the only history.

## Read paths

Three doors, same memory:

```
sampler ring (memory only)
  ├── Trail() ──▶ GET /v1/nodes/self/trail ──▶ concord node trail (oldest-first, timestamped)
  ├── gossip latest ──▶ node list LAT/LON (fleet map, one point per node)
  └── gossip latest ──▶ concord_node_latitude/longitude gauges ──▶ scrapers keep history
```

* `concord node trail` prints `RFC3339 lat lon` oldest-first, one line
  per point. Empty trail prints nothing and succeeds; a stationary node
  has nowhere to go.
* `GET /v1/nodes/self/trail` serves the same points as JSON
  (`sdk.TrailPoint`). Local node only; there is no fleet-trail endpoint
  because there is no fleet trail.
* `/metrics` exposes only the latest gossiped position as
  `concord_node_latitude` / `concord_node_longitude` (0 when unknown,
  matching gossip). Scrapers reconstruct trails by polling; gaps in
  scraping are gaps in history. The gauges read the memberlist self
  entry, so scraped points match `node list` exactly, at the cost of one
  gossip round of staleness behind `SetPosition`.

## Gossip vs trail

|                   | Gossip position              | Trail                        |
|-------------------|------------------------------|------------------------------|
| Scope             | fleet-wide, converges        | local only                   |
| Content           | latest point                 | up to 720 timestamped points |
| Transport         | memberlist meta (microdegrees)| IPC / metrics                |
| Loss on restart   | no, reloads from config      | yes, memory only             |
| Unknown           | 0, 0                         | empty                        |
| Consumer          | scheduler anchor ordering, fleet map | operator replay, scraper history |

Anchor ordering reads the gossip position (`AnchorResolver{Self: pos}`),
never the trail. The trail never influences placement.

## Edge cases

* No position configured: gossip zeros, trail empty, anchors fall back
  to list order. LAN-only nodes live here; mDNS still discovers.
* Invalid fix: ignored, previous position kept, no trail append. Callers
  retry next round.
* Sub-10m moves: gossip still updates (peers see the crawl), trail does
  not append (history skips jitter). `node list` and `node trail` can
  therefore disagree on the freshest point by design.
* Clock: trail timestamps are local `time.Now()`. No wall-clock
  agreement is needed because trails never merge; cross-node ordering
  of trails is meaningless.
* Concurrency: fast beat (pressure `Publish`), discovery beat
  (position `Publish`), and IPC reads share the sampler and the
  delegate. Sets are lock-free atomics; `Publish` serializes the
  read-compare-broadcast; the sampler mutex guards the ring.

## Where it lives

Write: `refreshDiscoveryState` + `discoverAndJoin`
(`internal/runtime/runtime.go`), `SetPosition` + `Publish` +
`equalVolatile` (`internal/peerdiscovery/`), `RecordPosition`
(`internal/node/pressure.go`), ring (`internal/node/trail.go`),
coordinates (`internal/geo/`). Read: IPC trail handler
(`internal/ipc/handlers.go`), metrics gauges
(`internal/ipc/metrics.go`), CLI (`internal/cli/node.go`), SDK
(`sdk/client.go:Trail`, `sdk/types.go:TrailPoint`).
