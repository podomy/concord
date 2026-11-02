// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/google/uuid"
	"github.com/vishvananda/netlink"

	"github.com/podomy/concord/internal/peerdiscovery"
)

const (
	bridgeName = "cn0"
)

var (
	nextContainerIP = netip.MustParseAddr("10.0.0.0")
	// nodeSubnet is the /24 this node may allocate from, set by
	// SetNodeSubnet from the deterministic memberlist index. AllocateIP
	// fails closed outside it so one node can never spill into another
	// node's subnet and overlap container IPs.
	nodeSubnet = netip.MustParsePrefix("10.0.0.0/24")
	// nodeSubnetIndex is the deterministic memberlist index SetNodeSubnet
	// derived nodeSubnet from. The reconciler compares it against the
	// live membership order every tick to detect subnet index drift.
	nodeSubnetIndex int
	ipMu            sync.Mutex
	// freeIPs holds released container IPs ready for reuse. AllocateIP
	// prefers them over advancing the cursor so destroy and recreate
	// cycles reuse addresses instead of marching toward exhaustion.
	freeIPs []netip.Addr
	// usedIPs tracks live container IPs: sequential allocations plus
	// adopted ones. The cursor skips them so a daemon restart that
	// resets the cursor never hands out an adopted container's IP twice.
	usedIPs = map[netip.Addr]struct{}{}
)

// Invariants
// 1. One bridge per node. cn0 exists as long as concord is running.
// It has to be idempotent (create-if-not exists).
// 2. One veth pair per container. Created on container start, destroyed on stopping.
// Bridge attachment though needs a manual cleanup.
// 3. IP assignment is deterministic per workload. A
// container gets 10.0.{index}.x from this node's /24.
// Rely on the veth's host-side MAC as a persistent
// identifier since workloads persist by identity.
// 4. Port mapping is a one shot add/remove. A DNAT rule in iptables for each
// HOSTPORT -> CONTAINERIP:CONTAINERPORT. Removed atomically on container stop.

// CreateBridge ensures the cn0 bridge exists, is up, and
// has 10.0.{nodeIndex}.1/24 assigned. It is idempotent,
// safe to call on every startup. Stale 10.0.x.1/24 addrs from a previous
// index reconcile away here, so an index change never leaves two gateway
// IPs on the bridge; EEXIST stays tolerated for the current addr.
func CreateBridge(ctx context.Context, nodeIndex int) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	ip := fmt.Sprintf("10.0.%d.1", nodeIndex)
	mask := "/24"

	// Create the bridge.
	bridge := &netlink.Bridge{
		LinkAttrs: netlink.LinkAttrs{
			Name: bridgeName,
		},
	}

	// Idempotently create the link device for the bridge.
	linkDevice, err := findOrCreateBridgeLinkDevice(ctx, bridge)
	if err != nil {
		return fmt.Errorf("find or create link device: %w", err)
	}

	// Enable the link device.
	err = netlink.LinkSetUp(linkDevice)
	if err != nil {
		return fmt.Errorf("netlink link set up: %w", err)
	}

	// Add an address to the link device.
	linkAddress, err := netlink.ParseAddr(ip + mask)
	if err != nil {
		return fmt.Errorf("netlink parseaddr: %w", err)
	}

	err = netlink.AddrAdd(linkDevice, linkAddress)
	if err != nil && !errors.Is(err, syscall.EEXIST) {
		return fmt.Errorf("netlink addr add: %w", err)
	}

	// Drop stale gateway addrs from a previous index. Best-effort: a
	// list failure leaves old addrs for the next call, never fails boot.
	if err := reconcileBridgeAddrs(linkDevice, ip+mask); err != nil {
		return fmt.Errorf("reconcile bridge addrs: %w", err)
	}

	// NAT setup. Setup of the masquerade to rewrite source ip
	// of the outgoing container packets to the ip of the host.
	err = SetupMasquerade(ctx)
	if err != nil {
		return fmt.Errorf("setup masquerade: %w", err)
	}

	return nil
}

// reconcileBridgeAddrs removes stale 10.0.x.1/24 gateway addrs that do not
// match the current subnet. Called on every CreateBridge so an index
// change converges without a reboot; every-N-ticks callers get the same
// effect by re-invoking CreateBridge.
func reconcileBridgeAddrs(link netlink.Link, current string) error {
	addrs, err := netlink.AddrList(link, netlink.FAMILY_V4)
	if err != nil {
		return fmt.Errorf("netlink addr list: %w", err)
	}
	stale := staleBridgeAddrs(addrStrings(addrs), current)
	for _, s := range stale {
		addr, err := netlink.ParseAddr(s)
		if err != nil {
			continue
		}
		err = netlink.AddrDel(link, addr)
		if err != nil {
			return fmt.Errorf("netlink addr del %s: %w", s, err)
		}
	}
	return nil
}

// addrStrings renders netlink addrs for pure stale filtering.
func addrStrings(addrs []netlink.Addr) []string {
	out := make([]string, 0, len(addrs))
	for _, a := range addrs {
		if a.IPNet != nil {
			out = append(out, a.IPNet.String())
		}
	}
	return out
}

// staleBridgeAddrs returns 10.0.x.1/24 entries that are not current. Other
// families, masks, and hosts never match: only the /24 gateway pattern
// reconciles, so unrelated addrs stay untouched.
func staleBridgeAddrs(have []string, current string) []string {
	var out []string
	for _, h := range have {
		if h == current {
			continue
		}
		ip, prefix, ok := splitAddrPrefix(h)
		if !ok {
			continue
		}
		if !isBridgeGateway(ip, prefix) {
			continue
		}
		out = append(out, h)
	}
	return out
}

// splitAddrPrefix cuts "10.0.1.1/24" into IP and mask bits.
func splitAddrPrefix(s string) (string, int, bool) {
	for i := range len(s) {
		if s[i] == '/' {
			n := 0
			for j := i + 1; j < len(s); j++ {
				c := s[j]
				if c < '0' || c > '9' {
					return "", 0, false
				}
				n = n*10 + int(c-'0')
			}
			return s[:i], n, true
		}
	}
	return "", 0, false
}

// isBridgeGateway reports the 10.0.x.1/24 gateway pattern.
func isBridgeGateway(ip string, mask int) bool {
	if mask != 24 {
		return false
	}
	var a, b, c, d int
	if _, err := fmt.Sscanf(ip, "%d.%d.%d.%d", &a, &b, &c, &d); err != nil {
		return false
	}
	return a == 10 && b == 0 && c >= 0 && c <= 255 && d == 1
}

// findOrCreateBridgeLinkDevice returns the existing cn0 link device or
// creates it via netlink if it does not already exist.
func findOrCreateBridgeLinkDevice(ctx context.Context, bridge *netlink.Bridge) (netlink.Link, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("context cancellation: %w", err)
	}

	// Search for the interface first, if it exists do not add it.
	linkDevice, errLinkByName := netlink.LinkByName(bridge.Name)
	if errLinkByName != nil {
		// Add the interface to the system.
		errLinkAdd := netlink.LinkAdd(bridge)
		if errLinkAdd != nil {
			return nil, fmt.Errorf("netlink link add: %w", errLinkAdd)
		}
		linkDevice = bridge
	}

	return linkDevice, nil
}

// BridgeExists reports whether the cn0 bridge device is present. The
// reconciler probes it every tick so a missing bridge fails the tick with
// a debug log and retries, instead of failing every container start.
func BridgeExists() bool {
	link, err := netlink.LinkByName(bridgeName)
	return err == nil && link != nil
}

// SetNodeSubnet configures the base container IP allocation pool for this node
// using its deterministic cluster-wide subnet index (10.0.<index>.2 onwards).
// It also records the index for drift detection and resets the free and
// used sets: a new index owns a disjoint /24, so previous-pool state must
// not carry over.
func SetNodeSubnet(index int) {
	ipMu.Lock()
	defer ipMu.Unlock()
	nodeSubnetIndex = index
	nodeSubnet = netip.MustParsePrefix(fmt.Sprintf("10.0.%d.0/24", index))
	nextContainerIP = netip.MustParseAddr(fmt.Sprintf("10.0.%d.2", index))
	freeIPs = nil
	usedIPs = map[netip.Addr]struct{}{}
}

// NodeSubnetIndex returns the deterministic memberlist index the node's
// /24 was derived from at boot. The reconciler compares it against the
// live order every tick; a mismatch is drift, never a re-pick.
func NodeSubnetIndex() int {
	ipMu.Lock()
	defer ipMu.Unlock()
	return nodeSubnetIndex
}

// SubnetIndexFor sorts members by ID string and returns nodeID's position.
// It mirrors runtime's boot-time index so the reconciler can detect drift
// without importing runtime (which would cycle: runtime imports cn).
// Absence is an error, never a silent 0: two nodes on 10.0.0.0/24 would
// hand out overlapping container IPs.
func SubnetIndexFor(members []peerdiscovery.Node, nodeID uuid.UUID) (int, error) {
	ordered := make([]peerdiscovery.Node, len(members))
	copy(ordered, members)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ID.String() < ordered[j].ID.String()
	})
	for i, m := range ordered {
		if m.ID == nodeID {
			return i, nil
		}
	}
	return 0, fmt.Errorf("node %s not in member list (%d members)", nodeID, len(members))
}

// AllocateIP allocates an available IP address and returns that as a string of IP + CIDR.
// Freed IPs reuse first, then the cursor advances past used (adopted)
// addresses. It fails closed with an exhaustion error once the node's /24
// is spent, instead of spilling Next() into the next node's subnet and
// overlapping container IPs across the cluster.
func AllocateIP() (string, error) {
	ipMu.Lock()
	defer ipMu.Unlock()
	for len(freeIPs) > 0 {
		ip := freeIPs[len(freeIPs)-1]
		freeIPs = freeIPs[:len(freeIPs)-1]
		if !nodeSubnet.Contains(ip) {
			continue
		}
		if _, dup := usedIPs[ip]; dup {
			continue
		}
		usedIPs[ip] = struct{}{}
		return ip.String() + "/24", nil
	}
	for nodeSubnet.Contains(nextContainerIP) {
		if _, dup := usedIPs[nextContainerIP]; dup {
			nextContainerIP = nextContainerIP.Next()
			continue
		}
		ip := nextContainerIP
		nextContainerIP = nextContainerIP.Next()
		usedIPs[ip] = struct{}{}
		return ip.String() + "/24", nil
	}
	return "", fmt.Errorf("node subnet %s exhausted, no free address left (cursor at %s): remove unused workloads or restart the node to reclaim addresses", nodeSubnet, nextContainerIP)
}

// ReleaseIP returns a container CIDR to the free pool for reuse. Unknown,
// unparsable, or out-of-subnet CIDRs are ignored: cleanup paths call this
// best-effort and must never fail container teardown on an IP parse.
func ReleaseIP(cidr string) {
	ip, ok := parsePoolAddr(cidr)
	if !ok {
		return
	}
	ipMu.Lock()
	defer ipMu.Unlock()
	if !nodeSubnet.Contains(ip) {
		return
	}
	delete(usedIPs, ip)
	if !slices.Contains(freeIPs, ip) {
		freeIPs = append(freeIPs, ip)
	}
}

// AdoptIP records an adopted container's CIDR as used so the cursor skips
// it instead of handing it out twice. Unknown or out-of-subnet CIDRs are
// ignored; adoption already warns those paths may leak on cleanup.
func AdoptIP(cidr string) {
	ip, ok := parsePoolAddr(cidr)
	if !ok {
		return
	}
	ipMu.Lock()
	defer ipMu.Unlock()
	if !nodeSubnet.Contains(ip) {
		return
	}
	usedIPs[ip] = struct{}{}
}

// parsePoolAddr cuts the IP half off an IP/CIDR string for pool accounting.
func parsePoolAddr(cidr string) (netip.Addr, bool) {
	ipStr, _, ok := strings.Cut(cidr, "/")
	if !ok {
		return netip.Addr{}, false
	}
	ip, err := netip.ParseAddr(ipStr)
	if err != nil {
		return netip.Addr{}, false
	}
	return ip, true
}
