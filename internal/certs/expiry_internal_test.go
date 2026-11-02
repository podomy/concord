// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package certs

import (
	"crypto/x509"
	"testing"
	"time"
)

// Inside the 30-day horizon the warning fires; outside it stays quiet.
func TestExpiringSoonThreshold(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cases := []struct {
		name  string
		after time.Time
		want  bool
	}{
		{"29 days warns", now.Add(29 * 24 * time.Hour), true},
		{"just under 30 days warns", now.Add(expiryWarnThreshold - time.Second), true},
		{"exactly 30 days quiet", now.Add(expiryWarnThreshold), false},
		{"31 days quiet", now.Add(31 * 24 * time.Hour), false},
		{"expired quiet (Ensure owns that error)", now.Add(-time.Hour), false},
	}
	for _, tc := range cases {
		cert := &x509.Certificate{NotAfter: tc.after}
		_, got := expiringSoon(cert, now)
		if got != tc.want {
			t.Fatalf("%s: warn = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// The warning reports whole days remaining.
func TestExpiringSoonDaysLeft(t *testing.T) {
	t.Parallel()

	now := time.Now()
	days, warn := expiringSoon(&x509.Certificate{NotAfter: now.Add(10 * 24 * time.Hour)}, now)
	if !warn || days != 10 {
		t.Fatalf("days = %d warn = %v, want 10 true", days, warn)
	}
}

// Nil logger and nil cert never panic: the warning is best-effort.
func TestLogExpiryWarningNilSafe(t *testing.T) {
	t.Parallel()

	now := time.Now()
	LogExpiryWarning(nil, &x509.Certificate{NotAfter: now.Add(time.Hour)}, now)
	LogExpiryWarning(nil, nil, now)
}
