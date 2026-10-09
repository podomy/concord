# Position Trail

The trail answers one question: where did this node go. Recent history
is local memory, full history is a local file, neither is converged
state. No coordinate ever enters the journal, and the full trail never
appears in logs. What converges fleet-wide is only the latest position
via gossip; history stays on the node that lived it, in its ring and
its track file, plus whatever external scrapers recorded.

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
autonomy / operator ──IPC──▶ persist config ──▶ gossip atomics
                                           │
                                           └──▶ RecordPosition ──10m gate──▶ ring (720, memory)
                                                                            └──▶ track.jsonl (all, disk)
```

1. The only writer is `PUT /v1/nodes/self/position` (`concord node
   position set --lat --lon`, SDK `SetPosition`). The daemon persists
   the fix to `config.json` first so the next boot republishes it, then
   applies gossip, trail, and track log together. Persist failure
   applies nothing. The config file is storage, never polled: no loop
   reads position back.
2. `RecordPosition` appends only when the node moved at least 10m
   (`trailMoveKM`) from the last recorded point. The first valid point
   always records. Sub-threshold jitter (depot idle, GPS noise) records
   nothing. Invalid (out-of-planet) fixes are rejected at the endpoint
   with 400; the exact zero point is allowed through as unknown and
   skips the trail by the zeros convention.
3. Anchor ordering reads the freshest fix (`LastPosition`), not the
   trail: the trail skips jitter while ordering wants every fix. Anchor
   list edits still reload from config each discovery round; position
   never does.

## Storage

Two depths, one choke point. `RecordPosition` is the only writer: gated
points land in the ring and the track file together, in the same order,
so the file always holds everything the ring does and more.

* Ring (`internal/node/trail.go`, owned by the `Sampler`): 720 points,
  overwrite-oldest when full, `Trail()` returns oldest-first. At the 10m
  minimum step 720 points cover 7.2km of roam; at real roam steps (100m+)
  they cover far more. Time span is unbounded; memory is flat forever.
  Ephemeral: rebuilt fresh on every boot.
* File (`~/.config/concord/track.jsonl`, `internal/node/track.go`): one
  JSON line per gated point, fsynced per append, never truncated or
  rotated by the daemon. Rotation and archival are operator-owned, same
  as `journal.jsonl`. This is the full mission history: ring overwrite
  and restarts lose nothing here.

The recovery story follows from that split. A node that partitions and
keeps running replays its dark-period roam from memory with no journal
involved. A node that restarts replays it from the track file instead;
only a lost disk loses the roam.

## Read paths

Three doors, same memory:

```
sampler ring (memory only)
  ├── Trail() ──▶ GET /v1/nodes/self/trail ──▶ concord node trail (oldest-first, timestamped)
  ├── gossip latest ──▶ node list LAT/LON (fleet map, one point per node)
  └── gossip latest ──▶ concord_node_latitude/longitude gauges ──▶ scrapers keep history
```

* `concord node trail` prints `RFC3339 lat lon` oldest-first, one line
  per point, from the ring: the roam since boot, up to 720 points. Empty
  trail prints nothing and succeeds; a stationary node has nowhere to
  go. Full mission history lives in `track.jsonl`, grepped, never
  paged through the CLI.
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

|                   | Gossip position              | Trail (ring)                 | Track file                   |
|-------------------|------------------------------|------------------------------|------------------------------|
| Scope             | fleet-wide, converges        | local only                   | local only                   |
| Content           | latest point                 | up to 720 timestamped points | every gated point ever       |
| Transport         | memberlist meta (microdegrees)| IPC / metrics               | file grep                    |
| Loss on restart   | no, reloads from config      | yes, memory only             | no                           |
| Unknown           | 0, 0                         | empty                        | absent                       |
| Consumer          | scheduler anchor ordering, fleet map | operator replay, scraper history | audit, crash forensics |

Anchor ordering reads the gossip position (`AnchorResolver{Self: pos}`),
never the trail. The trail never influences placement.

## Edge cases

* No position ever set: gossip zeros, trail empty, anchors fall back
  to list order. LAN-only nodes live here; mDNS still discovers. The
  boot fix comes from the persisted config position when one exists.
* Invalid fix: rejected at the endpoint with 400, previous fix kept, no
  trail append. Callers retry with a real fix.
* Sub-10m moves: gossip still updates (peers see the crawl), trail does
  not append (history skips jitter). `node list` and `node trail` can
  therefore disagree on the freshest point by design.
* Clock: trail timestamps are local `time.Now()`. No wall-clock
  agreement is needed because trails never merge; cross-node ordering
  of trails is meaningless.
* Concurrency: fast beat (pressure `Publish`), IPC setter, and IPC reads
  share the sampler and the delegate. Sets are lock-free atomics;
  `Publish` serializes the read-compare-broadcast; the sampler mutex
  guards ring, last fix, and sink.

## Where it lives

Write: IPC setter (`internal/ipc/handlers.go:handleSetPosition`),
persistence (`PersistPosition` in `internal/node/identity.go`),
`SetPosition` + `Publish` + `equalVolatile`
(`internal/peerdiscovery/`), `RecordPosition` + `LastPosition`
(`internal/node/pressure.go`), ring (`internal/node/trail.go`),
track file (`internal/node/track.go`), coordinates (`internal/geo/`).
Read: IPC trail handler (`internal/ipc/handlers.go`), metrics gauges
(`internal/ipc/metrics.go`), CLI (`internal/cli/node.go`), SDK
(`sdk/client.go:SetPosition`, `sdk/types.go:TrailPoint`). Boot seeds
gossip, ordering, and trail from config (`internal/runtime/runtime.go`).
Discovery refreshes anchors only, never position.
