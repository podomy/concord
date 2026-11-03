// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package clock

import (
	"os"
	"strconv"
	"time"
)

// maxClockOffsetSeconds bounds CONCORD_CLOCK_OFFSET to thirty days
// either way. Simulation skews are minutes to hours; anything larger is
// a typo, and the seconds-to-Duration conversion would overflow past
// it. The bound is checked in seconds, before the multiplication that
// would wrap.
const maxClockOffsetSeconds = 30 * 24 * 60 * 60

// Now returns time, if an env variable of offset has been set,
// then the returned time is offset on return.
// It is needed for the regression and simulation tests in resonance.
// Offset env MUST NEVER BE USED IN PRODUCTION.
func Now() time.Time {
	offsetString := os.Getenv("CONCORD_CLOCK_OFFSET")
	if offsetString == "" {
		return time.Now()
	}
	offset, err := strconv.Atoi(offsetString)
	if err != nil {
		return time.Now()
	}
	if offset > maxClockOffsetSeconds {
		offset = maxClockOffsetSeconds
	}
	if offset < -maxClockOffsetSeconds {
		offset = -maxClockOffsetSeconds
	}
	return time.Now().Add(time.Duration(offset) * time.Second)
}
