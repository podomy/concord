# Journal Sync and Cursors

Every node holds the full journal, but no node is told when a peer appends.
So each node polls every alive peer every 5 seconds and pulls whatever
journal events it has not seen yet, up to 100 per request. The thing that
remembers "what I have seen from whom" is the cursor.

(On the wire the field is still called `watermark`:
`transport.SyncRequest.Watermark` carries the cursor value. Renaming the
protocol is out of scope; the docs and the pull loop say cursor.)

## What a cursor is

A cursor is one string per peer: the id of the last journal event
already pulled from that peer. Nothing more. It is a bookmark, not data.

* Node A holds cursor `evt-120` for peer B: A has everything of B's up
  to and including `evt-120`.
* No entry for a peer means first contact: pull from B's very first event.

Cursors live in process memory only (`internal/peersync/cursors.go`,
type `cursorSet`). A restart wipes them, which is why every restart
re-pulls every peer journal from event zero. That is correct but
wasteful, and it is the reason persistence is the next job.

## One sync round, concretely

Say B's journal holds 250 events and A's cursor for B is `evt-120`:

1. A asks B: "everything after `evt-120`, at most 100 events".
2. B scans its journal from the start, skips through `evt-120`, and
   returns events 121 through 220, plus the next cursor `evt-220`.
3. A appends the 220s it does not already have into its own journal and
   views, skipping known ids, then moves its cursor to `evt-220`.
4. Next round A asks after `evt-220` and gets 221 through 250.
5. The round after that B has nothing new: empty page, and A's cursor
   stays at `evt-250` instead of being cleared.

One page per peer per tick, so a node 10,000 events behind takes
100 ticks to catch up.

## The cursor moves only forward on success

The cursor advances only after the received events are safely stored
locally. Nothing about the events themselves can "fail" or get
"rejected". What fails is the local write, for ordinary mechanical
reasons:

* the disk errors while appending to the local journal,
* a view cannot parse a malformed payload,
* the node is shutting down mid-apply and the context is cancelled.

In all three cases the cursor stays where it was, so the next tick
retries the same page. Skipped work is never lost work: the cursor only
ever moves past events that are already stored.

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

## The two known costs

* Every restart re-pulls every peer journal from event zero, with
  dedup absorbing the duplicates. Fix: persist the cursors.
* To serve one page, the server reads its whole journal file from the
  start to find the skip point. Long journals make every page
  expensive. Fix (separate job): an index from event id to file
  offset.
