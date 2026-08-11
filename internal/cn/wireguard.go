// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package cn

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vishvananda/netlink"
	"go.uber.org/zap"
	"golang.zx2c4.com/wireguard/wgctrl"
	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"

	"github.com/podomy/concord/internal/peerdiscovery"
)

const (
	// DefaultWGPort is the default UDP port for WireGuard overlay mesh tunnels.
	DefaultWGPort = 51820

	// wgInterfacePrefix is the prefix used for dynamic WireGuard interface names.
	wgInterfacePrefix = "wg-"
)

// Key holds a WireGuard base64-encoded private and public key pair.
type Key struct {
	Private string
	Public  string
}

// WGInterfaceName computes a deterministic interface name for a peer UUID
// that conforms to Linux's 15-character IFNAMSIZ limit (e.g., "wg-a1b2c3d4").
func WGInterfaceName(id uuid.UUID) string {
	return fmt.Sprintf("%s%s", wgInterfacePrefix, id.String()[:8])
}

// EnsureWGKeys reads or generates persistent WireGuard keys under ~/.config/concord/wireguard/wg.key.
func EnsureWGKeys() (Key, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return Key{}, fmt.Errorf("get user config dir: %w", err)
	}

	wgDir := filepath.Join(dir, "concord", "wireguard")
	err = os.MkdirAll(wgDir, 0o700)
	if err != nil {
		return Key{}, fmt.Errorf("create wireguard dir: %w", err)
	}

	keyPath := filepath.Join(wgDir, "wg.key")
	// #nosec G304 -- keyPath is within controlled user config dir
	data, err := os.ReadFile(keyPath)
	if err == nil {
		privStr := strings.TrimSpace(string(data))
		privKey, parseErr := wgtypes.ParseKey(privStr)
		if parseErr == nil {
			return Key{
				Private: privKey.String(),
				Public:  privKey.PublicKey().String(),
			}, nil
		}
	}

	privKey, err := wgtypes.GeneratePrivateKey()
	if err != nil {
		return Key{}, fmt.Errorf("generate private key: %w", err)
	}

	err = os.WriteFile(keyPath, []byte(privKey.String()+"\n"), 0o600)
	if err != nil {
		return Key{}, fmt.Errorf("write wireguard key: %w", err)
	}

	return Key{
		Private: privKey.String(),
		Public:  privKey.PublicKey().String(),
	}, nil
}

// RunTunnelManager continuously reconciles WireGuard overlay tunnels with cluster membership.
func RunTunnelManager(
	ctx context.Context,
	logger *zap.Logger,
	peerService *peerdiscovery.MemberService,
	nodeID uuid.UUID,
	myKey Key,
	listenPort int,
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Initial sync upon startup.
	err := SyncTunnels(ctx, logger, peerService, nodeID, myKey, listenPort)
	if err != nil {
		logger.Warn("initial wireguard tunnel sync failed", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			// Clean up all tunnel interfaces on context cancellation / shutdown.
			err := TeardownAllTunnels(logger)
			if err != nil {
				logger.Warn("teardown tunnels failed", zap.Error(err))
			}
			return
		case <-ticker.C:
			// Periodically synchronize local tunnels with current cluster membership.
			err := SyncTunnels(ctx, logger, peerService, nodeID, myKey, listenPort)
			if err != nil {
				logger.Warn("wireguard tunnel sync failed", zap.Error(err))
			}
		}
	}
}

// SyncTunnels discovers active cluster peers from memberlist, provisions point-to-point
// WireGuard links for each alive peer, configures routing into their assigned /24 subnets,
// and removes tunnels for departed peers.
func SyncTunnels(
	ctx context.Context,
	logger *zap.Logger,
	peerService *peerdiscovery.MemberService,
	nodeID uuid.UUID,
	myKey Key,
	listenPort int,
) error {
	if peerService == nil {
		return nil
	}

	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	members, err := peerService.Members()
	if err != nil {
		return fmt.Errorf("peer service members: %w", err)
	}

	// 1. Calculate the active peers and their deterministic subnet allocations.
	desired, desiredSubnets := findDesiredPeers(members, nodeID)

	// 2. Delete stale interfaces that belong to departed or failed nodes.
	err = cleanupStaleLinks(desired, logger)
	if err != nil {
		return err
	}

	if len(desired) == 0 {
		return nil
	}

	// 3. Provision or update point-to-point WireGuard links for all desired peers.
	return syncDesiredTunnels(ctx, logger, desired, desiredSubnets, myKey, listenPort)
}

// findDesiredPeers evaluates cluster members, sorts them deterministically by node UUID,
// and filters for alive remote peers that have published a WireGuard public key.
// It returns a mapping of interface names to peer nodes and their corresponding /24 subnet indices
// which is just an integer which gets used later.
func findDesiredPeers(members []peerdiscovery.Node, nodeID uuid.UUID) (map[string]peerdiscovery.Node, map[string]int) {
	// Sort in ascending order.
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID.String() < members[j].ID.String()
	})

	desired := make(map[string]peerdiscovery.Node)
	desiredSubnets := make(map[string]int)

	for idx, m := range members {
		// Ignore self, nodes without a WireGuard public key, or non-alive nodes.
		if m.ID == nodeID || m.Metadata.WireGuardPublicKey == "" || m.State != peerdiscovery.NodeStateAlive {
			continue
		}
		ifName := WGInterfaceName(m.ID)
		desired[ifName] = m
		desiredSubnets[ifName] = idx
	}

	return desired, desiredSubnets
}

// cleanupStaleLinks enumerates all host network interfaces with the "wg-" prefix and deletes
// any links that do not correspond to an active desired cluster peer.
func cleanupStaleLinks(desired map[string]peerdiscovery.Node, logger *zap.Logger) error {
	links, err := netlink.LinkList()
	if err != nil {
		return fmt.Errorf("list network links: %w", err)
	}

	for _, link := range links {
		name := link.Attrs().Name
		if !strings.HasPrefix(name, wgInterfacePrefix) {
			continue
		}
		// If the link is not in the desired set, remove it from the system.
		if _, ok := desired[name]; !ok {
			delErr := netlink.LinkDel(link)
			if delErr != nil {
				logger.Warn("failed to delete stale wireguard link", zap.String("link", name), zap.Error(delErr))
			} else {
				logger.Info("removed wireguard link", zap.String("link", name))
			}
		}
	}
	return nil
}

// syncDesiredTunnels provisions, binds, and configures kernel WireGuard devices and routes
// for all desired cluster peers.
func syncDesiredTunnels(
	ctx context.Context,
	logger *zap.Logger,
	desired map[string]peerdiscovery.Node,
	desiredSubnets map[string]int,
	myKey Key,
	listenPort int,
) error {
	wgClient, err := wgctrl.New()
	if err != nil {
		return fmt.Errorf("wgctrl new: %w", err)
	}
	defer wgClient.Close() //nolint:errcheck // best-effort client close

	privKey, err := wgtypes.ParseKey(myKey.Private)
	if err != nil {
		return fmt.Errorf("parse private key: %w", err)
	}

	if listenPort <= 0 {
		listenPort = DefaultWGPort
	}

	// Reconcile each individual peer tunnel.
	for ifName, peer := range desired {
		peerIdx := desiredSubnets[ifName]
		tunnelErr := syncSingleTunnel(ctx, logger, wgClient, ifName, peer, peerIdx, privKey, listenPort)
		if tunnelErr != nil {
			logger.Error("failed to sync wireguard tunnel", zap.String("link", ifName), zap.String("peer_id", peer.ID.String()), zap.Error(tunnelErr))
		}
	}
	return nil
}

// syncSingleTunnel provisions and configures a single WireGuard point-to-point interface.
func syncSingleTunnel(
	ctx context.Context,
	logger *zap.Logger,
	wgClient *wgctrl.Client,
	ifName string,
	peer peerdiscovery.Node,
	peerIdx int,
	privKey wgtypes.Key,
	listenPort int,
) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("context cancellation: %w", err)
	}

	peerPubKey, err := wgtypes.ParseKey(peer.Metadata.WireGuardPublicKey)
	if err != nil {
		return fmt.Errorf("parse peer public key %q: %w", peer.Metadata.WireGuardPublicKey, err)
	}

	// Idempotently retrieve or create the WireGuard netlink interface device.
	link, err := getOrCreateWGLink(ifName)
	if err != nil {
		return err
	}

	// Bring the interface up.
	err = netlink.LinkSetUp(link)
	if err != nil {
		return fmt.Errorf("link set up %s: %w", ifName, err)
	}

	// Configure WireGuard device crypto keys, remote UDP endpoint, keepalives, and kernel routes.
	return configureWGTunnelDevice(logger, wgClient, ifName, link, privKey, peerPubKey, peer.Address.Addr(), listenPort, peerIdx)
}

// getOrCreateWGLink searches for an existing interface by name or creates a new WireGuard link.
func getOrCreateWGLink(ifName string) (netlink.Link, error) {
	link, err := netlink.LinkByName(ifName)
	if err == nil {
		return link, nil
	}

	wgLink := &netlink.Wireguard{
		LinkAttrs: netlink.LinkAttrs{Name: ifName},
	}
	err = netlink.LinkAdd(wgLink)
	if err != nil {
		return nil, fmt.Errorf("link add %s: %w", ifName, err)
	}
	return wgLink, nil
}

// configureWGTunnelDevice configures cryptographic keys, peer endpoint, allowed IPs,
// and installs the destination subnet route into the host routing table.
func configureWGTunnelDevice(
	logger *zap.Logger,
	wgClient *wgctrl.Client,
	ifName string,
	link netlink.Link,
	privKey wgtypes.Key,
	peerPubKey wgtypes.Key,
	peerAddr netip.Addr,
	listenPort int,
	peerIdx int,
) error {
	peerUDPAddr := &net.UDPAddr{
		IP:   peerAddr.AsSlice(),
		Port: listenPort,
	}

	// Remote node's assigned /24 container subnet.
	_, peerSubnetNet, err := net.ParseCIDR(fmt.Sprintf("10.0.%d.0/24", peerIdx))
	if err != nil {
		return fmt.Errorf("parse peer cidr: %w", err)
	}

	cfg := wgtypes.Config{
		PrivateKey:   &privKey,
		ReplacePeers: true,
		Peers: []wgtypes.PeerConfig{
			{
				PublicKey:         peerPubKey,
				Endpoint:          peerUDPAddr,
				AllowedIPs:        []net.IPNet{*peerSubnetNet},
				ReplaceAllowedIPs: true,
				PersistentKeepaliveInterval: func() *time.Duration {
					d := 25 * time.Second
					return &d
				}(),
			},
		},
	}

	// Apply configuration to the WireGuard device in the kernel.
	err = wgClient.ConfigureDevice(ifName, cfg)
	if err != nil {
		return fmt.Errorf("configure device %s: %w", ifName, err)
	}

	// Install a route directing traffic for the remote subnet into this WireGuard interface.
	route := &netlink.Route{
		LinkIndex: link.Attrs().Index,
		Dst:       peerSubnetNet,
	}
	err = netlink.RouteAdd(route)
	if err != nil && !os.IsExist(err) {
		logger.Debug("wireguard route add notice", zap.String("link", ifName), zap.Error(err))
	}

	return nil
}

// TeardownAllTunnels removes all active WireGuard interfaces starting with the "wg-" prefix.
func TeardownAllTunnels(logger *zap.Logger) error {
	links, err := netlink.LinkList()
	if err != nil {
		return fmt.Errorf("list network links: %w", err)
	}

	for _, link := range links {
		name := link.Attrs().Name
		if strings.HasPrefix(name, wgInterfacePrefix) {
			delErr := netlink.LinkDel(link)
			if delErr != nil {
				if logger != nil {
					logger.Warn("failed to delete wireguard link during teardown", zap.String("link", name), zap.Error(delErr))
				}
			}
		}
	}

	return nil
}
