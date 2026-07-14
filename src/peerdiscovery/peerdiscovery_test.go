// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"context"
	"errors"
	"net/netip"
	"testing"
)

func TestMultiResolverMergesAndDeduplicates(t *testing.T) {
	t.Parallel()

	a := netip.MustParseAddrPort("10.0.0.1:7946")
	b := netip.MustParseAddrPort("10.0.0.2:7946")

	r1 := ResolverFunc(func(context.Context) ([]netip.AddrPort, error) {
		return []netip.AddrPort{a, b}, nil
	})
	r2 := ResolverFunc(func(context.Context) ([]netip.AddrPort, error) {
		return []netip.AddrPort{b}, nil
	})

	got, err := NewMultiResolver(r1, r2).Resolve(context.Background())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 unique addresses, got %d: %v", len(got), got)
	}

	seen := map[netip.AddrPort]struct{}{}
	for _, addr := range got {
		seen[addr] = struct{}{}
	}
	if _, ok := seen[a]; !ok {
		t.Fatalf("missing %v", a)
	}
	if _, ok := seen[b]; !ok {
		t.Fatalf("missing %v", b)
	}
}

func TestMultiResolverPartialFailure(t *testing.T) {
	t.Parallel()

	want := netip.MustParseAddrPort("10.0.0.1:7946")
	okResolver := ResolverFunc(func(context.Context) ([]netip.AddrPort, error) {
		return []netip.AddrPort{want}, nil
	})
	failResolver := ResolverFunc(func(context.Context) ([]netip.AddrPort, error) {
		return nil, errors.New("boom")
	})

	got, err := NewMultiResolver(failResolver, okResolver).Resolve(context.Background())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("got %v, want [%v]", got, want)
	}
}

func TestMultiResolverAllFail(t *testing.T) {
	t.Parallel()

	r1 := ResolverFunc(func(context.Context) ([]netip.AddrPort, error) {
		return nil, errors.New("one")
	})
	r2 := ResolverFunc(func(context.Context) ([]netip.AddrPort, error) {
		return nil, errors.New("two")
	})

	_, err := NewMultiResolver(r1, r2).Resolve(context.Background())
	if err == nil {
		t.Fatal("expected error when all resolvers fail")
	}
}

func TestMultiResolverCancelledContext(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := NewMultiResolver().Resolve(ctx)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
}

func TestResolverFunc(t *testing.T) {
	t.Parallel()

	want := netip.MustParseAddrPort("192.0.2.1:1")
	f := ResolverFunc(func(context.Context) ([]netip.AddrPort, error) {
		return []netip.AddrPort{want}, nil
	})
	got, err := f.Resolve(context.Background())
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("got %v, want [%v]", got, want)
	}
}

func TestAddrPortsToStrings(t *testing.T) {
	t.Parallel()

	addrs := []netip.AddrPort{
		netip.MustParseAddrPort("10.0.0.1:7946"),
		netip.MustParseAddrPort("[::1]:7946"),
	}
	got := addrPortsToStrings(addrs)
	if len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	if got[0] != "10.0.0.1:7946" {
		t.Fatalf("got %q", got[0])
	}
	if got[1] != "[::1]:7946" {
		t.Fatalf("got %q", got[1])
	}
}
