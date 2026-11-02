// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/sdk"
)

// stubLogsClient replays canned log snapshots per Logs call.
type stubLogsClient struct {
	sdk.Client
	snapshots [][]sdk.LogLine
	calls     int
}

func (s *stubLogsClient) Logs(_ context.Context, _ uuid.UUID, _ int) ([]sdk.LogLine, error) {
	if s.calls >= len(s.snapshots) {
		last := s.snapshots[len(s.snapshots)-1]
		return last, nil
	}
	out := s.snapshots[s.calls]
	s.calls++
	return out, nil
}

// Lines sharing the last printed timestamp print once each across
// polls: the second poll skips the already-printed prefix at the same
// timestamp and prints only the newcomer.
func TestPrintLogLinesSameTimestamp(t *testing.T) {
	t.Parallel()

	at := time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)
	id := uuid.New()
	client := &stubLogsClient{snapshots: [][]sdk.LogLine{
		{
			{At: at, Stream: "stdout", Line: "one"},
			{At: at, Stream: "stdout", Line: "two"},
		},
		{
			{At: at, Stream: "stdout", Line: "one"},
			{At: at, Stream: "stdout", Line: "two"},
			{At: at, Stream: "stdout", Line: "three"},
		},
	}}

	var buf bytes.Buffer
	since, seen, err := printLogLines(context.Background(), client, &buf, id, 0, time.Time{}, 0)
	if err != nil {
		t.Fatalf("first poll: %v", err)
	}
	if buf.String() != "one\ntwo\n" {
		t.Fatalf("first poll = %q", buf.String())
	}
	if !since.Equal(at) || seen != 2 {
		t.Fatalf("first poll cursor = (%v, %d), want (%v, 2)", since, seen, at)
	}

	buf.Reset()
	since, seen, err = printLogLines(context.Background(), client, &buf, id, 0, since, seen)
	if err != nil {
		t.Fatalf("second poll: %v", err)
	}
	if buf.String() != "three\n" {
		t.Fatalf("second poll = %q, want only newcomer", buf.String())
	}
	if !since.Equal(at) || seen != 3 {
		t.Fatalf("second poll cursor = (%v, %d), want (%v, 3)", since, seen, at)
	}

	// A third poll with no new lines prints nothing.
	buf.Reset()
	_, _, err = printLogLines(context.Background(), client, &buf, id, 0, since, seen)
	if err != nil {
		t.Fatalf("third poll: %v", err)
	}
	if buf.String() != "" {
		t.Fatalf("third poll = %q, want empty", buf.String())
	}
}
