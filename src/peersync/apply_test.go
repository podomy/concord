// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peersync

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/src/journal"
	"github.com/podomy/concord/src/journalreader"
	"github.com/podomy/concord/src/journalview"
)

// ApplyEvents skips ids already in the index and records new ones once.
func TestApplyEventsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	id := uuid.New()
	ev := journal.NewEvent(uuid.New(), "test.event", json.RawMessage(`{}`))
	ev.ID = id

	j := &memJournal{}
	idx := &journalIndex{j: j}
	views := []journalview.View{&countingView{}}

	n, err := ApplyEvents(ctx, j, views, idx, []journal.Event{ev})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(j.events) != 1 {
		t.Fatalf("first apply: n=%d len=%d", n, len(j.events))
	}

	n, err = ApplyEvents(ctx, j, views, idx, []journal.Event{ev})
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 || len(j.events) != 1 {
		t.Fatalf("second apply should no-op: n=%d len=%d", n, len(j.events))
	}
}

// Mix of known and new events in one page.
func TestApplyEventsSkipsKnownKeepsNew(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	knownID := uuid.New()
	newID := uuid.New()
	known := journal.NewEvent(uuid.New(), "known", json.RawMessage(`{}`))
	known.ID = knownID
	fresh := journal.NewEvent(uuid.New(), "fresh", json.RawMessage(`{}`))
	fresh.ID = newID

	j := &memJournal{events: []journal.Event{known}}
	idx := &journalIndex{j: j}

	n, err := ApplyEvents(ctx, j, nil, idx, []journal.Event{known, fresh})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 || len(j.events) != 2 || j.events[1].ID != newID {
		t.Fatalf("n=%d events=%v", n, j.events)
	}
}

// journalIndex treats memJournal contents as the local event index.
type journalIndex struct {
	j *memJournal
}

func (m *journalIndex) Get(_ context.Context, id uuid.UUID) (*journal.Event, error) {
	for i := range m.j.events {
		if m.j.events[i].ID == id {
			return &m.j.events[i], nil
		}
	}
	return nil, nil
}

type memJournal struct {
	events []journal.Event
}

func (m *memJournal) Append(_ context.Context, event journal.Event) error {
	m.events = append(m.events, event)
	return nil
}

type countingView struct {
	n int
}

func (c *countingView) Apply(context.Context, journal.Event) error {
	c.n++
	return nil
}

func (c *countingView) Rebuild(context.Context, journalreader.Reader) error {
	return nil
}
