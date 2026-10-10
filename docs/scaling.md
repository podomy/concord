# Scaling: How Far One Segment Goes

The question this answers: at what fleet size does a single memberlist
segment break, and do we need multi-segment hierarchy? 

Short version:
not at any fleet size we will run. The gossip layer stays bored into
the thousands; the practical ceiling is a few hundred nodes and it
comes from journal sync, not membership. Hierarchy is not needed.

Running example through this document: a 30-truck open-pit mine, one
segment, default 5s sync tick, 500ms pressure beat.

## The three membership mechanisms

Membership runs on three loops. Only one of them grows with fleet size
in a way that matters:

```
every 1s:      probe ONE random peer ──► ack? ──► suspect? ──► dead?
               (O(1) per node, always: one ping out, one ack back)

every 100ms:   gossip to 3 random peers ──► piggyback queued updates
               (metas, pressure, joins, suspects; each update sent
               ~2 x log10(N) times per node, then dropped)

every 15s:     push/pull ONE random peer ──► full state both ways
               (the backstop: heals anything gossip lost)
```

Picture 30 trucks. Each truck pings one random truck per second,
gossips to three trucks five times a second, and swaps full notebooks
with one truck every 15 seconds. Nobody ever broadcasts to everybody:
that is the whole trick, and it is why this scales.

## Measured on this tree (loopback)

| What | Result |
|---|---|
| Gossip metadata per node | 292 bytes of the 512 cap (43% headroom left) |
| Pressure update visible on a peer | ~200 ms |
| 8-node join storm | full mesh in 1.6 s |
| 10-node join storm, no rejoin | one viewer blind to one joiner for ~25 s, then healed, zero suspects |
| Same storm with production 5s rejoin | full mesh in 7.2 s |

The 25s stall deserves one paragraph because it looks like a scaling
wall and is not. A lost alive-message heals only at push/pull cadence,
and the first push/pull fires after a random 0-15s stagger, with a 15s
retry on failure, so a single lost message can hide one node from one
viewer for 15-30s. The scratch test never rejoined, so it sat out the
full cycle. Production re-joins candidates every discovery round (5s),
bounding the same event to ~7s as measured. No correctness impact
either way: the scheduler places from a slightly stale set for a few
seconds, the journal loses nothing.

## The math (memberlist v0.5.4, our `DefaultLocalConfig`)

Per update, each node transmits it about `2 x log10(N+1)` times:

| N | sends per node per update | gossip send BW at 1 update/s/node |
|---|---|---|
| 30 | ~4 | ~1.6 KB/s |
| 100 | ~6 | ~2.4 KB/s |
| 500 | ~6 | ~2.4 KB/s |
| 1000 | ~8 | ~3.2 KB/s |

Assumes ~400B compound packets (292B meta plus envelope, compression
and AES-GCM on). Note what is missing from the table: N. Per-node
gossip bandwidth is nearly flat with fleet size; only the log factor
moves. Dissemination takes gossip rounds of 100ms times ~log3(N):
under a second at any realistic size. Failure detection runs
`3 x log10(N) x 1s` plus probe timeouts, roughly 4s at N=10 stretching
to 9s at N=1000, so orphan failover degrades gracefully, not
suddenly. Push/pull full-state at 15s costs each node one transfer of
~150B x N per round: ~10 KB/s at N=1000. Trivial.

## Where the real wall is

Not gossip. Journal sync: every node pulls every alive peer every 5s
tick, one Noise handshake per peer per tick, O(N^2) cluster-wide:

```
tick (5s) on ONE node, N=100:          tick on ONE node, N=500:
  dial peer 1 .... handshake, pull        dial peer 1 .... handshake, pull
  dial peer 2 .... handshake, pull        ...
  ...                                     dial peer 499 .. handshake, pull
  dial peer 99 ... handshake, pull      x N nodes = 250,000 handshakes
x N nodes = 10,000 handshakes              per 5 seconds, cluster-wide
  per 5 seconds, cluster-wide
```

Handshake budget per node per tick, at ~3ms loopback / ~15ms LAN:

| N | loopback | LAN |
|---|---|---|
| 50 | 0.15 s | 0.7 s |
| 100 | 0.3 s | 1.5 s |
| 200 | 0.6 s | 3 s, saturating |
| 500 | over tick | over tick |

Practical single-segment ceiling: **low hundreds of nodes**. Behind it,
secondary O(N) costs: one WireGuard tunnel per peer per node, and
`Members()` unmarshalling the full metadata set several times per tick
in every subsystem that reads membership.

## Decision log

* No multi-segment hierarchy. Fleets of tens (or ambitious low
  hundreds) sit 10-50x below the practical ceiling.
* If pressure ever builds, levers in order: debounce pressure
  publishing on flapping values; stagger pulls across the tick instead
  of all-peers-every-tick; scale the pull interval with N the way
  push/pull already does.
* Revisit trigger: sustained fleets past ~200 nodes, or pull-tick
  saturation in metrics. The TODO's "representatives only" sketch
  (segments sync internally, reps exchange digests across segments)
  stays parked until then.
* Metadata headroom: ~220 bytes left under the 512 cap. Budget new
  gossip fields against it.

## Density, not just headcount

Every ceiling above assumes N nodes in one broadcast domain. Real
sites rarely do that: machines spread past radio range, links break,
and the fleet becomes natural partitions, which Concord treats as
first-class (per-partition leaders, reunion convergence). Geography
segments for free what hierarchy would segment in protocol. Plan
density like radio capacity: keep mutually reachable clusters in the
tens, let distance do the rest. The full procedure, antenna catalog,
link budgets, and worked cell sizings live in `docs/radio.md`.

If density still grows inside one connected segment, adapt cadence to
observed N (the pull tick can scale the way push/pull already does),
not behavior modes by anchor proximity. Proximity modes add boundary
flapping and partition fallbacks for a site shape (dense depot plus
sparse field, all mutually reachable) that operations should not
build. Parked unless a real deployment forces it.

The far end of this axis is the drone show: a thousand aircraft in one
sky, choreographed to the second. That system cannot be AP. Every
trajectory is computed centrally, time-synced, and broadcast
one-to-many from ground control with powerful antennas; a partition
stops the show by design (hover, then land). Total centralization for
total coordination. Concord sits at the opposite end: no center to
lose, coordination that survives the split, priced in headcount. Pick
by which failure you need to survive.

## Where it lives

Membership (`internal/peerdiscovery/memberlist.go`, `Start`), gossip
fields (`NodeMetadata` in `internal/peerdiscovery/peerdiscovery.go`),
publish-on-change (`Publish`), the pull loop (`internal/peersync/loop.go`:
`pullPeriodic` every 5s), tunnel manager (`internal/cn`), placement
(`internal/reconciler/scheduler.go:pickNode`). Library parameters from
`memberlist@v0.5.4` (`config.go`, `util.go`, `state.go`).
