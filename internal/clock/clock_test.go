// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package clock

import (
	"testing"
	"time"
)

// nowTolerance bounds scheduling jitter between Now and the test clock.
const nowTolerance = 10 * time.Second

// TestNowWithoutOffset pins that Now tracks the wall clock when the
// offset variable is empty.
func TestNowWithoutOffset(t *testing.T) {
	t.Setenv("CONCORD_CLOCK_OFFSET", "")

	before := time.Now()
	got := Now()
	after := time.Now()
	if got.Before(before.Add(-nowTolerance)) || got.After(after.Add(nowTolerance)) {
		t.Fatalf("Now() = %v, want within %v of [%v, %v]", got, nowTolerance, before, after)
	}
}

// TestNowWithOffset pins that a numeric offset shifts Now by that many seconds.
func TestNowWithOffset(t *testing.T) {
	cases := []struct {
		name    string
		env     string
		seconds int
	}{
		{"plus one minute", "60", 60},
		{"minus thirty seconds", "-30", -30},
		{"zero", "0", 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CONCORD_CLOCK_OFFSET", tc.env)

			shift := time.Duration(tc.seconds) * time.Second
			before := time.Now()
			got := Now()
			diff := got.Sub(before.Add(shift))
			if diff < -nowTolerance || diff > nowTolerance {
				t.Fatalf("Now() = %v, want %v plus %v within %v", got, before, shift, nowTolerance)
			}
		})
	}
}

// TestNowInvalidOffset pins that a non-numeric offset falls back to the wall clock.
func TestNowInvalidOffset(t *testing.T) {
	for _, env := range []string{"abc", "1.5", "10x"} {
		t.Run(env, func(t *testing.T) {
			t.Setenv("CONCORD_CLOCK_OFFSET", env)

			before := time.Now()
			got := Now()
			after := time.Now()
			if got.Before(before.Add(-nowTolerance)) || got.After(after.Add(nowTolerance)) {
				t.Fatalf("Now() = %v, want within %v of [%v, %v]", got, nowTolerance, before, after)
			}
		})
	}
}

// TestNowClampsHugeOffset pins that an absurd offset clamps to thirty
// days instead of overflowing the Duration conversion and wrapping.
func TestNowClampsHugeOffset(t *testing.T) {
	t.Setenv("CONCORD_CLOCK_OFFSET", "9999999999")

	before := time.Now()
	got := Now()
	after := time.Now()
	low := before.Add(30*24*time.Hour - nowTolerance)
	high := after.Add(30*24*time.Hour + nowTolerance)
	if got.Before(low) || got.After(high) {
		t.Fatalf("Now() = %v, want within %v of +30d", got, nowTolerance)
	}
}

// TestNowMonotonic pins that successive Now calls never go backwards.
func TestNowMonotonic(t *testing.T) {
	t.Setenv("CONCORD_CLOCK_OFFSET", "")

	prev := Now()
	for range 100 {
		next := Now()
		if next.Before(prev) {
			t.Fatalf("Now() went backwards: %v then %v", prev, next)
		}
		prev = next
	}
}
