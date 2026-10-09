// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"fmt"
	"net/netip"
	"sort"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/certs"
	"github.com/podomy/concord/internal/cn"
	"github.com/podomy/concord/internal/cr"
	"github.com/podomy/concord/internal/dnsserver"
	"github.com/podomy/concord/internal/geo"
	"github.com/podomy/concord/internal/ipc"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/node"
	"github.com/podomy/concord/internal/or"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/peersync"
	"github.com/podomy/concord/internal/reconciler"
	"github.com/podomy/concord/internal/transport"
)

// Run performs application startup, blocks for the process
// lifetime, and handles graceful shutdown.
func Run(ctx context.Context, logger *zap.Logger) error {
	// Load persistent identity for this node, creating one
	// if none exists.
	nodeConfig, err := initNodeConfig()
	if err != nil {
		return err
	}

	st, err := openStores()
	if err != nil {
		// error was wrapped inside open stores.
		return err
	}
	defer closeStores(logger, st)

	eventsByID, _, workloads, pinned, views, err := setupViews(
		ctx,
		st.kv,
	)
	if err != nil {
		return fmt.Errorf("setup views: %w", err)
	}

	// Create a startup event and persist it before
	// announcing readiness.
	err = journalview.RecordNodeStarted(
		ctx,
		logger,
		st.journal,
		views,
		nodeConfig.ID,
		nodeConfig.MemberlistAddress,
	)
	if err != nil {
		return fmt.Errorf("record node started: %w", err)
	}

	// Ensure all node key material: WireGuard for the overlay mesh, Noise
	// static key plus CA-signed identity for the sync transport.
	wgKey, staticKey, noiseIdentity, err := ensureNodeKeys(*nodeConfig)
	if err != nil {
		return err
	}

	stopMDNS, err := startMDNSAdvertise(
		ctx,
		logger,
		nodeConfig,
	)
	if err != nil {
		return err
	}
	defer stopMDNS()

	peerService, err := startPeerService(
		logger,
		nodeConfig,
		nil,
		wgKey.Public,
		noiseIdentity,
	)
	if err != nil {
		return err
	}
	defer shutdownPeerService(logger, peerService)

	// The sampler is shared between the reconciler fast beat (writes),
	// the discovery beat (position trail), and the IPC server (reads).
	sampler := node.NewSampler()

	// Publish our configured position for geo-scoped discovery and the
	// fleet map. Absent when the operator provisioned none.
	publishPosition(nodeConfig, peerService)
	peerService.SetAnchor(nodeConfig.Anchor)

	// Peerdiscovery is split: ObserveMemberlistPeers is
	// passive, it only polls the already-joined memberlist
	// and records peer.seen/updated/lost. runDiscoveryLoop
	// is active, it re-queries mDNS and anchors for new
	// candidates and calls Join. Without the loop a node
	// that booted alone would never discover later peers.
	go peerdiscovery.ObserveMemberlistPeers(
		ctx,
		logger,
		nodeConfig.ID,
		peerService,
		st.journal,
		views,
	)
	go runDiscoveryLoop(ctx, logger, peerService, nodeConfig.Anchors, sampler)

	err = dnsserver.Start(ctx, peerService, logger, "")
	if err != nil {
		return fmt.Errorf(
			"dns server start failed: %w",
			err,
		)
	}
	logger.Info("DNS server started")

	client, err := setupSyncTransport(ctx, logger, *nodeConfig, staticKey, noiseIdentity, st.journal, views, pinned, st.kv)
	if err != nil {
		return err
	}
	// Reconciliation loop: pull peers and apply events into
	// local journal/views. Cursors persist in the kv store so
	// restarts resume mid-history.
	go peersync.RunPullLoop(
		ctx,
		logger,
		nodeConfig.ID,
		peerService,
		client,
		st.journal,
		views,
		eventsByID,
		peersync.NewCursorStore(st.kv),
	)
	logger.Info("peer sync pull loop started")

	// Start the workload infrastructure and network.
	ocireg, err := startWorkloadAndNetwork(
		ctx,
		nodeConfig.ID,
		peerService,
		logger,
		st,
		workloads,
		views,
		wgKey,
		sampler,
	)
	if err != nil {
		return fmt.Errorf(
			"start workload and network: %w",
			err,
		)
	}
	defer ocireg.Stop()

	// Start local IPC server for CLI and SDK access.
	ipcServer := ipc.NewServer(
		nodeConfig.ID,
		st.journal,
		views,
		workloads,
		peerService,
		logger,
		sampler,
	)
	if err := ipcServer.Start(ctx, ""); err != nil {
		return fmt.Errorf("start local ipc server: %w", err)
	}
	defer shutdownIPCServer(ctx, logger, ipcServer)

	// Block until the OS delivers a shutdown signal.
	<-ctx.Done()
	logger.Info(
		"shutting down",
		zap.String("node_id", nodeConfig.ID.String()),
	)

	// Clean up wireguard tunnels and network masquerade.
	teardownNetworking(logger)
	return nil
}

func initNodeConfig() (*node.NodeConfig, error) {
	nodeConfig, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		return nil, fmt.Errorf("load node config: %w", err)
	}
	// Fallback to the standard memberlist gossip port on
	// all interfaces if no bind address is configured.
	if !nodeConfig.MemberlistAddress.IsValid() {
		nodeConfig.MemberlistAddress = netip.MustParseAddrPort(
			"0.0.0.0:7946",
		)
	}
	return nodeConfig, nil
}

// publishPosition gossips the configured position, if any. Absent
// position means LAN-only identity: the node still discovers over mDNS,
// it just never orders anchors by distance.
func publishPosition(nodeConfig *node.NodeConfig, peerService *peerdiscovery.MemberService) {
	if nodeConfig.Position == nil {
		return
	}
	peerService.SetPosition(nodeConfig.Position.Lat, nodeConfig.Position.Lon)
}

func teardownNetworking(logger *zap.Logger) {
	// Clean up wireguard tunnels.
	err := cn.TeardownAllTunnels(logger)
	if err != nil {
		logger.Warn(
			"teardown wireguard tunnels failed",
			zap.Error(err),
		)
	}
	// Clean up the masquerade.
	err = cn.TeardownMasquerade()
	if err != nil {
		logger.Warn(
			"teardown masquerade failed",
			zap.Error(err),
		)
	}
}

func setupNetwork(ctx context.Context, nodeIndex int) error {
	// Create the network bridge.
	err := cn.CreateBridge(ctx, nodeIndex)
	if err != nil {
		return fmt.Errorf("create bridge: %w", err)
	}

	err = cn.SetupMasquerade(ctx)
	if err != nil {
		return fmt.Errorf("setup masquerade: %w", err)
	}

	return nil
}

func startWorkloadInfrastructure(
	ctx context.Context,
	nodeID uuid.UUID,
	peerService *peerdiscovery.MemberService,
	logger *zap.Logger,
	st *stores,
	workloads *journalview.Workloads,
	views []journalview.View,
	sampler *node.Sampler,
) (*or.Registry, error) {
	// Start the OCI registry.
	ocireg, err := startOCIRegistry(
		ctx,
		nodeID,
		peerService,
		logger,
	)
	if err != nil {
		return nil, err
	}
	logger.Info(
		"oci registry started",
		zap.Int("port", or.Port),
	)

	// Start the workload reconciler loop.
	puller := cr.NewImagePuller()
	crRuntime, err := cr.NewRuntime()
	if err != nil {
		return nil, fmt.Errorf("container runtime: %w", err)
	}
	go reconciler.RunLoop(
		ctx,
		logger,
		nodeID,
		puller,
		crRuntime,
		st.journal,
		workloads,
		views,
		peerService,
		sampler,
	)
	logger.Info("workload reconciler started")

	return ocireg, nil
}

func startWorkloadAndNetwork(
	ctx context.Context,
	nodeID uuid.UUID,
	peerService *peerdiscovery.MemberService,
	logger *zap.Logger,
	st *stores,
	workloads *journalview.Workloads,
	views []journalview.View,
	wgKey cn.Key,
	sampler *node.Sampler,
) (*or.Registry, error) {
	ocireg, err := startWorkloadInfrastructure(
		ctx,
		nodeID,
		peerService,
		logger,
		st,
		workloads,
		views,
		sampler,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"start workload infrastructure: %w",
			err,
		)
	}

	idx, err := nodeIndex(ctx, nodeID, peerService)
	if err != nil {
		idx = 0
	}
	cn.SetNodeSubnet(idx)

	err = setupNetwork(ctx, idx)
	if err != nil {
		return nil, fmt.Errorf("setup network: %w", err)
	}

	// Start WireGuard tunnel manager loop.
	go cn.RunTunnelManager(
		ctx,
		logger,
		peerService,
		nodeID,
		wgKey,
		cn.DefaultWGPort,
	)

	return ocireg, nil
}

func nodeIndex(
	ctx context.Context,
	nodeID uuid.UUID,
	peerService *peerdiscovery.MemberService,
) (int, error) {
	err := ctx.Err()
	if err != nil {
		return 0, fmt.Errorf("context cancelation: %w", err)
	}

	members, err := peerService.Members()
	if err != nil {
		return 0, fmt.Errorf("peerservice members: %w", err)
	}

	// Sort the members in ascending order.
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID.String() < members[j].ID.String()
	})

	// Find ourselves in the list and then return the index.
	for i, m := range members {
		if m.ID == nodeID {
			return i, nil
		}
	}
	return 0, nil
}

func startTransport(
	ctx context.Context,
	logger *zap.Logger,
	nodeConfig node.NodeConfig,
	static transport.StaticKey,
	identity peerdiscovery.NoiseIdentity,
	verify transport.Verifier,
	kv *kvstore.KVStore,
) (*transport.Client, error) {
	parcel := transport.EncodeParcel(nodeConfig.ID, identity.Generation, identity.Signature)

	offsetIndex := transport.NewOffsetIndex(kv)

	err := transport.Start(
		ctx,
		logger,
		static,
		parcel,
		verify,
		offsetIndex,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"noise server failed to start: %w",
			err,
		)
	}

	client := transport.NewClient(
		static,
		parcel,
		verify,
	)

	logger.Info(
		"noise transport started",
		zap.String("addr", ":"+transport.Port),
	)

	return client, nil
}

// setupSyncTransport builds the pinning verifier and starts the Noise
// transport plus sync client. The serving side seeks by cursor offset
// through an index on the shared kv store.
func setupSyncTransport(
	ctx context.Context,
	logger *zap.Logger,
	nodeConfig node.NodeConfig,
	static transport.StaticKey,
	identity peerdiscovery.NoiseIdentity,
	j journal.Journal,
	views []journalview.View,
	pinned *journalview.PinnedKeys,
	kv *kvstore.KVStore,
) (*transport.Client, error) {
	// First-seen pinning wraps the CA check for both handshake directions.
	// Pins persist in the journal, so restarts re-pin from replay.
	verify, err := newKeyPinner(ctx, logger, nodeConfig.ID, j, views, pinned)
	if err != nil {
		return nil, err
	}

	return startTransport(ctx, logger, nodeConfig, static, identity, verify, kv)
}

// ensureNodeKeys ensures every key this node needs: the WireGuard pair for
// the overlay mesh, and the Noise static key plus its CA-signed identity for
// the sync transport.
func ensureNodeKeys(nodeConfig node.NodeConfig) (cn.Key, transport.StaticKey, peerdiscovery.NoiseIdentity, error) {
	wgKey, err := cn.EnsureWGKeys()
	if err != nil {
		return cn.Key{}, transport.StaticKey{}, peerdiscovery.NoiseIdentity{}, fmt.Errorf("ensure wireguard keys: %w", err)
	}

	static, identity, err := prepareNoiseIdentity(nodeConfig)
	if err != nil {
		return cn.Key{}, transport.StaticKey{}, peerdiscovery.NoiseIdentity{}, err
	}

	return wgKey, static, identity, nil
}

// prepareNoiseIdentity ensures this node's Noise static key and rotation
// generation, then signs the identity parcel the node gossips to peers.
// certs.Ensure runs first so a missing CA fails with the provisioning error.
func prepareNoiseIdentity(nodeConfig node.NodeConfig) (transport.StaticKey, peerdiscovery.NoiseIdentity, error) {
	_, err := certs.Ensure()
	if err != nil {
		return transport.StaticKey{}, peerdiscovery.NoiseIdentity{}, fmt.Errorf("ensure certs: %w", err)
	}

	static, err := transport.EnsureStaticKey()
	if err != nil {
		return transport.StaticKey{}, peerdiscovery.NoiseIdentity{}, fmt.Errorf("ensure noise key: %w", err)
	}

	generation, err := transport.EnsureGenerationCounter()
	if err != nil {
		return transport.StaticKey{}, peerdiscovery.NoiseIdentity{}, fmt.Errorf("ensure noise generation: %w", err)
	}

	sig, err := certs.SignNodeKey(nodeConfig.ID, generation, static.Public)
	if err != nil {
		return transport.StaticKey{}, peerdiscovery.NoiseIdentity{}, fmt.Errorf("sign noise key: %w", err)
	}

	identity := peerdiscovery.NoiseIdentity{
		Pub:        static.Public,
		Signature:  sig,
		Generation: generation,
	}

	return static, identity, nil
}

func closeStores(logger *zap.Logger, st *stores) {
	err := st.kv.Close()
	if err != nil {
		logger.Error("close kv store", zap.Error(err))
	}
	err = st.journal.Close()
	if err != nil {
		logger.Error("close journal", zap.Error(err))
	}
}

func shutdownPeerService(
	logger *zap.Logger,
	ps *peerdiscovery.MemberService,
) {
	err := ps.Shutdown()
	if err != nil {
		logger.Error(
			"shutdown peer service",
			zap.Error(err),
		)
	}
}

func shutdownIPCServer(
	ctx context.Context,
	logger *zap.Logger,
	s *ipc.Server,
) {
	shutdownCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx),
		3*time.Second,
	)
	defer cancel()
	err := s.Shutdown(shutdownCtx)
	if err != nil {
		logger.Warn("shutdown ipc server", zap.Error(err))
	}
}

func setupViews(
	ctx context.Context,
	kv *kvstore.KVStore,
) (*journalview.EventsByID, *journalview.EventsByType, *journalview.Workloads, *journalview.PinnedKeys, []journalview.View, error) {
	eventsByID := journalview.NewEventsByID(kv)
	eventsByNode := journalview.NewEventsByNode(kv)
	eventsByType := journalview.NewEventsByType(kv)
	workloads := journalview.NewWorkloads(kv)
	pinned := journalview.NewPinnedKeys(kv)
	views := []journalview.View{
		eventsByID,
		eventsByNode,
		eventsByType,
		workloads,
		pinned,
	}

	err := journalview.RebuildViews(ctx, views)
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf(
			"rebuild views: %w",
			err,
		)
	}

	return eventsByID, eventsByType, workloads, pinned, views, nil
}

// runDiscoveryLoop is the active discovery path. It
// periodically queries mDNS and anchors for bootstrap
// candidates that are not yet in the memberlist and
// attempts to join them. It complements
// ObserveMemberlistPeers, which only watches already-joined
// members.
func runDiscoveryLoop(
	ctx context.Context,
	logger *zap.Logger,
	peerService *peerdiscovery.MemberService,
	anchors []node.AnchorEntry,
	sampler *node.Sampler,
) {
	discoverAndJoin(ctx, logger, peerService, anchors, sampler)
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			discoverAndJoin(ctx, logger, peerService, anchors, sampler)
		}
	}
}

// refreshDiscoveryState reloads geographic position and anchor list from
// node config and gossips the position. The file is the single source:
// whoever moves us rewrites it, and the next discovery round publishes.
// Load failures keep the previous position and fall back to the boot
// anchors. Absent position publishes nothing. It returns the loaded
// position for the sampler trail and the anchor list for this round, so
// config edits to either take effect without a restart.
func refreshDiscoveryState(logger *zap.Logger, peerService *peerdiscovery.MemberService, fallbackAnchors []node.AnchorEntry) (*geo.Point, []node.AnchorEntry) {
	config, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		logger.Warn("reload node config for discovery", zap.Error(err))
		return nil, fallbackAnchors
	}
	if config.Position != nil {
		peerService.SetPosition(config.Position.Lat, config.Position.Lon)
		return config.Position, config.Anchors
	}
	// Deliberately absent position clears gossip to unknown. Load failures
	// above keep the previous fix; a clean decode without one means the
	// operator removed it, and the old coordinates must stop converging.
	peerService.ClearPosition()
	return nil, config.Anchors
}

// discoverAndJoin performs one discovery round: mDNS LAN candidates plus
// configured anchors ordered nearest-first, filtered, and Join. It also
// refreshes position and anchors from config, so roaming members and
// reprovisioned anchor lists update without a restart: whoever rewrites the
// file (operator, autonomy stack) sees it gossiped within one round. The
// 5s poll latency and the 10m trail threshold are the write path until a
// dedicated position feed exists; see docs/trail.md.
func discoverAndJoin(
	ctx context.Context,
	logger *zap.Logger,
	peerService *peerdiscovery.MemberService,
	anchors []node.AnchorEntry,
	sampler *node.Sampler,
) {
	// Config loads fresh every round: gossip, trail, and anchor ordering
	// all follow file rewrites together, never a boot snapshot.
	pos, liveAnchors := refreshDiscoveryState(logger, peerService, anchors)
	if pos != nil {
		sampler.RecordPosition(*pos)
	}
	if err := peerService.Publish(); err != nil {
		logger.Debug("publish metadata", zap.Error(err))
	}

	localAddress, err := peerService.LocalAddr()
	if err != nil {
		logger.Warn("peer service local addr failed",
			zap.Error(err))
		return
	}

	members, err := peerService.Members()
	if err != nil {
		logger.Warn(
			"peer service members failed",
			zap.Error(err),
		)
		return
	}

	// Each source fails independently: mDNS fails off-LAN by design, and
	// anchors must work exactly there, so neither failure stops the other.
	var addrs []netip.AddrPort
	mdnsResolver := peerdiscovery.MDNSResolver{
		Timeout: 5 * time.Second,
	}
	mdnsAddrs, err := mdnsResolver.Resolve(ctx)
	if err != nil {
		// Debug, not warn: mDNS fails off-LAN by design on every round,
		// and a warning that fires forever warns about nothing.
		logger.Debug(
			"mdns resolve failed",
			zap.Error(err),
		)
	} else {
		addrs = append(addrs, mdnsAddrs...)
	}
	anchorAddrs, err := peerdiscovery.AnchorResolver{Self: pos, Anchors: liveAnchors}.Resolve(ctx)
	if err != nil {
		logger.Warn(
			"anchor resolve failed",
			zap.Error(err),
		)
		return
	}
	addrs = append(addrs, anchorAddrs...)
	addrs = dedupeAddrs(addrs)
	if len(addrs) == 0 {
		logger.Debug("peer discovery: no candidates")
		logMemberlist(logger, peerService)
		return
	}

	addrs = filterJoinCandidates(
		addrs,
		localAddress,
		members,
	)
	if len(addrs) == 0 {
		logger.Debug("peer discovery: no new candidates")
		logMemberlist(logger, peerService)
		return
	}

	strs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		strs = append(strs, a.String())
	}
	// Candidates list at debug: unjoined addresses retry every round, so
	// info here would repeat forever. The success line below stays info.
	logger.Debug(
		"peer discovery candidates",
		zap.Strings("candidates", strs),
	)
	n, err := peerService.Join(addrs)
	if err != nil {
		logger.Warn(
			"peer join failed",
			zap.Error(err),
			zap.Strings("candidates", strs),
		)
		logMemberlist(logger, peerService)
		return
	}
	if n > 0 {
		logger.Info(
			"peer join succeeded",
			zap.Int("joined", n),
			zap.Strings("candidates", strs),
		)
	}
	logMemberlist(logger, peerService)
}

// logMemberlist writes the current memberlist view: id, address, state.
func logMemberlist(
	logger *zap.Logger,
	peerService *peerdiscovery.MemberService,
) {
	nodes, err := peerService.Members()
	if err != nil {
		logger.Warn(
			"peer service members failed",
			zap.Error(err),
		)
		return
	}
	strs := make([]string, 0, len(nodes))
	for _, node := range nodes {
		strs = append(
			strs,
			node.ID.String()+" "+
				node.Address.String()+" "+
				string(node.State),
		)
	}
	logger.Info(
		"memberlist content",
		zap.Strings("members", strs),
	)
}

// dedupeAddrs drops duplicate candidates from merged sources: mDNS and
// anchors overlap on LAN, and redialing the same address every round
// wastes the Join.
func dedupeAddrs(addrs []netip.AddrPort) []netip.AddrPort {
	seen := make(map[netip.AddrPort]struct{}, len(addrs))
	out := addrs[:0]
	for _, addr := range addrs {
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	return out
}

// filterJoinCandidates drops addresses we must not Join:
// this node's advertise address, and addresses already in
// the memberlist as alive or suspect. Failed, dead, or left
// members stay joinable so reunion can happen.
func filterJoinCandidates(
	addrs []netip.AddrPort,
	localAddress netip.AddrPort,
	members []peerdiscovery.Node,
) []netip.AddrPort {
	skip := make(map[netip.AddrPort]bool, len(members)+1)
	skip[localAddress] = true

	// We must not skip members of the list unless,
	// the node is alive or the node is suspect.
	// If we skip all of them regardless of status,
	// reconnection doesn't happen when a node dies.
	for _, m := range members {
		if m.State == peerdiscovery.NodeStateAlive ||
			m.State == peerdiscovery.NodeStateSuspect {
			skip[m.Address] = true
		}
	}

	out := make([]netip.AddrPort, 0, len(addrs))
	for _, a := range addrs {
		if skip[a] {
			continue
		}
		out = append(out, a)
	}
	return out
}

func startMDNSAdvertise(
	ctx context.Context,
	logger *zap.Logger,
	nodeConfig *node.NodeConfig,
) (func(), error) {
	mdnsServer, err := peerdiscovery.MDNSAdvertise(
		ctx,
		nodeConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("mdns advertise: %w", err)
	}
	logger.Info("mDNS advertise started")
	return func() {
		err := mdnsServer.Shutdown()
		if err != nil {
			logger.Error(
				"mdns advertise shutdown",
				zap.Error(err),
			)
		}
	}, nil
}

func startOCIRegistry(
	ctx context.Context,
	nodeID uuid.UUID,
	peerService *peerdiscovery.MemberService,
	logger *zap.Logger,
) (*or.Registry, error) {
	ocireg, err := or.New(nodeID)
	if err != nil {
		return nil, fmt.Errorf("oci registry new: %w", err)
	}
	err = ocireg.Start(ctx, peerService, logger)
	if err != nil {
		return nil, fmt.Errorf(
			"oci registry start: %w",
			err,
		)
	}
	return ocireg, nil
}

func startPeerService(
	logger *zap.Logger,
	nodeConfig *node.NodeConfig,
	join []netip.AddrPort,
	wgPublicKey string,
	identity peerdiscovery.NoiseIdentity,
) (*peerdiscovery.MemberService, error) {
	localNode := peerdiscovery.Node{
		ID: nodeConfig.ID,
		Address: netip.MustParseAddrPort(
			nodeConfig.MemberlistAddress.String(),
		),
		Metadata: peerdiscovery.NodeMetadata{
			WireGuardPublicKey: wgPublicKey,
			NoisePublicKey:     identity.Pub,
			NoiseGeneration:    identity.Generation,
		},
	}
	peerService, err := peerdiscovery.Start(
		logger,
		localNode,
		join,
		nodeConfig.AdvertiseAddress,
		identity,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"start peer discovery: %w",
			err,
		)
	}
	localAddr, err := peerService.LocalAddr()
	if err != nil {
		return nil, fmt.Errorf("get local address: %w", err)
	}
	logger.Info(
		"peer discovery started",
		zap.String(
			"bind",
			nodeConfig.MemberlistAddress.String(),
		),
		zap.String("advertise", localAddr.String()),
	)

	return peerService, nil
}
