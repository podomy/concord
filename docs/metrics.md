# Metrics and Pressure

Counts lie about load: two heavy workloads look lighter than five idle
ones. So the scheduler places by pressure, sampled on the fast beat,
with counts breaking ties.

## The rule

Sample fast locally, keep raw numbers in memory, let only summaries and
state transitions cross a boundary. Nothing sampled enters the journal
per-sample, and no per-workload detail enters gossip. What crosses is a
summary or a transition edge; what converges is the edge.

```
healthy ━━━━━━━━━━━━━━━┓
                       ┗━━━━━━━━━━━━━━━ sick
                       ▲
                       falling edge: the one fact worth writing down.
                       One journal event; the flat lines write nothing.
```

Nine hundred ninety-nine healthy checks write nothing anywhere. The
thousandth finds sickness, and that falling edge appends exactly one
event, which every node merges and agrees on. Rising edges work the
same way. Edges are events with history; samples are readings without
any.

## Beats and routing

Three beats share one goroutine, so nothing races and nothing locks:

```
time ───────────────────────────────────────────────────▶
fast  ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ ▲ sample + health (500ms)
slow  ▲                       ▲ reconcile + place (5s)
trend ▲                       ▲ capture trends (5s)
```

The beats divide the work: fast samples into memory and gossips the
trio, slow reads gossip and places, trend folds latest readings into
rings. All three share one goroutine, so no maps need locking; the
price is that a slow reconcile delays fast beats, accepted until health
latency needs the guarantee a mutex would buy.

Everything the sampler holds leaves through exactly one door:

```
sampler (memory only)
  ├── trio ────────▶ gossip ──▶ scheduler (slow tick, leader)
  ├── latest ──────▶ stats endpoint ──▶ CLI, SDK
  ├── trends ──────▶ stats averages
  ├── points ──────▶ /metrics ──▶ scrapers
  └── edges ───────▶ journal (transitions only)
```

No door leads to another: gossip never carries samples, the journal
never carries readings, exposition never carries history.

## The trio

Each node samples three utilizations as 0-100 percents:

* CPU from `/proc/stat` deltas between beats (first sample reports 0).
  Utilization is inherently a ratio over time: two cumulative counters
  differenced, not a value read off disk.

  ```
  CPU% = 100 × busy_delta / total_delta
       = 100 × (1 − idle_delta / total_delta)
  ```

  Both deltas span the same two beats; idle counts idle plus iowait.
* Memory as used over total from `/proc/meminfo`.
* Disk as used blocks over total on the concord data disk.

A percent says how full a node is, never how much room it has. Percents
normalize across heterogeneous hardware and fit the 512-byte gossip cap;
the capacity they normalize against (`CPUMHz`, `MemoryMB`) travels
beside them, so absolutes are always derivable and never transmitted.
Full formulas live in `internal/node/pressure.go`.

## Placement, worked

Pressure is the max of the trio: a node is as loaded as its most
constrained resource. Lowest pressure wins, fewest workloads on draws,
alive nodes preferred. Three candidates:

```
A: cpu 70, mem 20, disk 10, workloads 1  → pressure 70
B: cpu 15, mem 15, disk 80, workloads 0  → pressure 80
C: cpu 15, mem 15, disk 10, workloads 4  → pressure 15, wins
```

C wins despite four workloads: counts only break ties. B loses despite
zero workloads: its disk is the bottleneck. One writer makes this safe,
only the segment leader assigns, sequentially, so pressure's flappiness
converges instead of herding.

Disk doubles as the survival signal. A full disk fails journal appends
and bbolt writes together; a node reporting near-full disk sinks in
placement, and the number warns long before the failure.

## Per-workload stats

`Container.Stats()` differenced across beats gives per-workload CPU
percent and memory against its limit. Served through
`GET /v1/workloads/{id}/stats` and `workload stats`, in memory only.
Operators see what each workload costs; the mesh never hears about it.

## History and exposition

Latest values answer "how loaded now"; trends answer "where is it
going". Every 5 seconds the reconciler captures current readings into
round-robin trend series, 720 slots each, one hour of context per series
with memory flat forever. `workload stats` shows the window average beside
the live reading, and `GET /metrics` (plus `concord metrics`) serves
everything in Prometheus text exposition format for external
collectors. Trends hold the window, exposition serves points, the journal
carries neither: history that never converges stays out of the log by
the same rule as samples.

## What stays out

Per-sample journal writes, per-workload gossip, cross-node rollups, and
new dependencies. `/proc`, `Statfs`, and libcontainer cover it all.

## Where it lives

Sampler (`internal/node/pressure.go`, trends in `internal/node/trend.go`),
gossip fields and `SetPressure` (`internal/peerdiscovery/`), placement
(`pickNode` in `internal/reconciler/scheduler.go`), inspection (IPC stats
and metrics endpoints, SDK `Stats` and `Metrics`, CLI).
