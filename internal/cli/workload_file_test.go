// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cli

import (
	"testing"
)

// Flag shapes split without network: modes parse as octal, bad shapes
// fail before any push.
func TestParseFileFlag(t *testing.T) {
	t.Parallel()

	container, local, mode, err := parseFileFlag("/etc/app/c.yaml=/tmp/c.yaml:0644")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if container != "/etc/app/c.yaml" || local != "/tmp/c.yaml" || mode != 0o644 {
		t.Fatalf("got %q %q %o", container, local, mode)
	}

	if _, _, mode, err := parseFileFlag("/x=/y"); err != nil || mode != 0 {
		t.Fatalf("default mode = %o, %v", mode, err)
	}

	for _, flag := range []string{"noequals", "=nonempty", "container=", "/x=/y:9999", "/x=/y:notoctal", "/x=/y:07777"} {
		if _, _, _, err := parseFileFlag(flag); err == nil {
			t.Errorf("parseFileFlag(%q) = nil, want error", flag)
		}
	}
}
