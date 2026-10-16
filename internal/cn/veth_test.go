// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"strings"
	"testing"
)

func TestVethHostNameFitsIFNAMSIZ(t *testing.T) {
	id := "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	for _, end := range []VethEnd{VethA, VethB} {
		name := VethHostName(id, end)
		// Linux IFNAMSIZ is 16 bytes (including null terminator), so max length is 15.
		if len(name) > 15 {
			t.Fatalf("interface name %s exceeds IFNAMSIZ length limit of 15 chars", name)
		}
	}
	if got := VethHostName(id, VethA); got != "veth-a1b2c3d4a" {
		t.Fatalf("expected veth-a1b2c3d4a, got %s", got)
	}
	if got := VethHostName(id, VethB); got != "veth-a1b2c3d4b" {
		t.Fatalf("expected veth-a1b2c3d4b, got %s", got)
	}
}

func TestVethHostNameEndsDiffer(t *testing.T) {
	id := "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
	if VethHostName(id, VethA) == VethHostName(id, VethB) {
		t.Fatal("host and container ends must differ")
	}
}

func TestVethHostNameShortID(t *testing.T) {
	name := VethHostName("test-veth", VethA)
	if !strings.HasPrefix(name, "veth-") {
		t.Fatalf("expected veth- prefix, got %s", name)
	}
	if len(name) > 15 {
		t.Fatalf("interface name %s exceeds IFNAMSIZ length limit of 15 chars", name)
	}
}
