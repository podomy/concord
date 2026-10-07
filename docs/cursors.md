# Journal Sync, Cursors, and the Serving Index

Every node holds the full journal, but no node is told when a peer appends.
So each node polls every alive peer every 5 seconds and pulls whatever
journal events it has not seen yet, up to 100 per request. The thing that
remembers "what I have seen from whom" is the cursor. The thing that lets
the serving side find that position without re-reading the whole file is
the offset index.

(The protocol calls it `cursor` too: `transport.SyncRequest.Cursor`
carries the position, `SyncResponse.NextCursor` carries the next one.
Mixed-version fleets degrade gracefully, an old node simply sees an
empty cursor and serves from the start, and idempotent apply absorbs
the overlap.)

## Pieces and where they live

* Pull side, `internal/peersync/`: `loop.go` runs the tick loop
  (`RunPullLoop`, `syncOne`) and `cursors.go` holds the per-peer
  positions (`cursorSet`) plus their bbolt persistence (`cursorStore`).
* Serve side, `internal/transport/`: `sync.go` answers pages
  (`postSync`, `loadSyncPage`, `loadIndexedPage`, `verifyCursor`,
  `readPageForward`, `readJournalPage`, `skipThroughCursor`,
  `recordObserved`) and `syncindex.go` keeps event id to byte offset
  (`offsetIndex`, `lookup`, `record`, `build`). `transport.go`
  wires the handler and starts the background build (`buildSyncIndex`).
* Byte tracking, `internal/journalreader/jsonl_reader.go`:
  `ReadWithOffset` reports each line's start byte and `SeekTo` jumps
  back to one.
* Wiring, `internal/runtime/runtime.go`: the pull loop gets a
  `cursorStore` on the shared kv store and the transport gets an
  `offsetIndex` on the same store.

## What a cursor is

A cursor is one string per peer: the id of the last journal event
already pulled from that peer. Nothing more. It is a bookmark, not data.

* Node A holds cursor `evt-120` for peer B: A has everything of B's up
  to and including `evt-120`.
* No entry for a peer means first contact: pull from B's very first event.

Cursors persist in bbolt (`synccursors` bucket, `internal/peersync/cursors.go`),
so a restart resumes mid-history instead of re-pulling from event zero.
A store load failure falls back to empty cursors; idempotent apply
discards the overlap. That is the whole of it: a plain bucket of
peer id to cursor string. No view, no journal events. Views project
journal events and cursors are not one, so there is nothing to project;
and journaling them would flood the log with an event per peer per tick
for state no other node can use.

## One sync round, concretely

Say B's journal holds 250 events and A's cursor for B is `evt-120`:

1. A asks B: "everything after `evt-120`, at most 100 events".
2. B looks up `evt-120` in its offset index, seeks straight to that
   byte in its journal, confirms the event there really is `evt-120`,
   and returns events 121 through 220, plus the next cursor `evt-220`.
   Offsets B walks while reading are recorded, so the index fills
   itself as it serves. (Full detail in "How a page is served" below.)
3. A appends the 220s it does not already have into its own journal and
   views, skipping known ids, then moves its cursor to `evt-220`, in
   memory and in the store together.
4. Next round A asks after `evt-220` and gets 221 through 250.
5. The round after that B has nothing new: empty page, and A's cursor
   stays at `evt-250` instead of being cleared.

One page per peer per request, but a full page means more may wait, so
the pull loop keeps going while pages come back full, up to 10 pages per
peer per tick. Steady state stays one page; a node 10,000 events behind
catches up in about 100 pages instead of 100 ticks.

## How a page is served

One request walks this path through `internal/transport/sync.go`:

```
POST /v1/sync {cursor: evt-120, limit: 100}
│
▼
loadSyncPage
│
├─ index usable and cursor non-empty?
│    │
│    ▼
│    loadIndexedPage ── lookup(evt-120)
│                            │
│                      miss ─┴─▶ full scan (below)
│                            │
│                           hit: offset 627
│                            │
│                            ▼
│                      seek(627) + verify ── id matches?
│                            │                     │
│                      ┌─────┘                     └──────┐
│                      ▼                                  ▼
│                read 121..220,                  heal entry with
│                record walked,                 true offset, then
│                return page                    full scan (below)
│     
└─▶ full scan: skipThroughCursor from byte 0,
    readPageForward up to limit,
    recordObserved everything walked
```

Every exit that read file bytes records the offsets it walked, in one
write transaction per request. That is the lazy half of index fill:
serving teaches the index.

## The journal as bytes

The journal file is plain JSON lines, one event per line, append-only.
The reader counts bytes as it goes, so every line's start offset is
known. Three lines look like this on disk:

```
byte:  0                 312               627
       ├──────────────────┼──────────────────┼───...
       │ {"id":"evt-118"  │ {"id":"evt-119"  │ {"id":"evt-120"
       │  ...}            │  ...}            │  ...}
       │ 312 bytes + \n   │ 315 bytes + \n   │ 314 bytes + \n
       └──────────────────┴──────────────────┴───...
```

`ReadWithOffset` returns each event together with its line's start
byte (`evt-119` starts at 312), and `SeekTo` jumps the file handle back
to any reported offset. Offsets stay valid because lines are never
rewritten or removed, only appended.

## The serving index

The `syncindex` bbolt bucket maps those two columns against each other:

```
syncindex bucket (bbolt):
  evt-118  →  0
  evt-119  →  312
  evt-120  →  627
```

One lookup plus one seek replaces a full file scan, so a page costs
100 lines no matter how long the journal grows. Like the cursors, it is
a plain bucket: no view, no journal events.

The index is advisory, never trusted blindly. The event at the stored
offset must carry the cursor id:

```
cursor evt-120 ──▶ lookup ──▶ 627 ──▶ seek(627) ──▶ read line
                                                 │
                                          id == evt-120?
                                           │        │
                                          yes  no: stale entry
                                           │        │
                                           ▼        ▼
                                 read 121..220   overwrite evt-120 → true
                                 return page     offset, full scan instead
```

Stale entries heal by overwrite on first use. The concrete case is a
journal wipe and reinstall: new file, old offsets. Say A still holds
`evt-250` at stored offset 81200, but B's new journal is only 9000
bytes long. The seek lands past EOF (read hits end, entry misses) or on
a line holding some other id (mismatch, entry heals). Either way the
request falls back to a full scan, A re-learns from event zero, and the
next request hits. No stuck cursor, no operator step.

The index fills two ways. A background build at startup
(`buildSyncIndex` in `transport.go`) scans the whole journal once in
500-entry batches, so no single transaction grows with the journal.
Requests fill the rest lazily as described above: every offset walked
during any scan, indexed or fallback, is recorded. While the build is
still running, requests simply serve through the fallback.

## The cursor moves only forward on success

The cursor advances only after the received events are safely stored
locally. Nothing about the events themselves can "fail" or get
"rejected". What fails is the local write, for ordinary mechanical
reasons:

* the disk errors while appending to the local journal,
* a view cannot parse a malformed payload,
* the node is shutting down mid-apply and the context is cancelled.

In all three cases the cursor stays where it was, in memory and in the
store together, so the next tick retries the same page. A crash between
a successful apply and its store write replays from the older cursor
and idempotent apply absorbs the overlap: the persisted cursor never
runs ahead of applied state, only behind it, and behind is the safe
direction. Skipped work is never lost work: the cursor only ever moves
past events that are already stored.

## Unknown cursors fall back to the beginning

If A's cursor names an event B does not have, B's scan reaches the end
of the journal while still skipping and would return empty pages
forever. A is then stuck: it keeps asking after an event that will
never appear.

Concrete case: B's disk dies and B is reinstalled with a fresh journal
starting over at new ids. A still holds `evt-250` from the old journal.
Every request after `evt-250` finds nothing.

So instead of sticking, B answers from the beginning of its journal.
A applies what it does not know and skips the rest by id. Progress is
guaranteed and the overlap is harmless, because apply skips known ids
anyway. Neither side ever fully trusts the other's cursor, and neither
side needs to.
