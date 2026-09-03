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
	"github.com/podomy/concord/internal/ipc"
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

	eventsByID, _, workloads, views, err := setupViews(
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

	// Ensure WireGuard key material for overlay mesh.
	wgKey, err := cn.EnsureWGKeys()
	if err != nil {
		return fmt.Errorf("ensure wireguard keys: %w", err)
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
	)
	if err != nil {
		return err
	}
	defer shutdownPeerService(logger, peerService)

	// Perform an initial discovery and joining of peers into
	// the memberlist.
	discoverAndJoin(ctx, logger, peerService)

	// Peerdiscovery is split: ObserveMemberlistPeers is
	// passive, it only polls the already-joined memberlist
	// and records peer.seen/updated/lost. runDiscoveryLoop
	// is active, it re-queries mDNS and DNS SRV for new
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
	go runDiscoveryLoop(ctx, logger, peerService)

	err = dnsserver.Start(ctx, peerService, logger, "")
	if err != nil {
		return fmt.Errorf(
			"dns server start failed: %w",
			err,
		)
	}
	logger.Info("DNS server started")

	client, err := startTransport(ctx, logger, *nodeConfig)
	if err != nil {
		return err
	}
	// Reconciliation loop: pull peers and apply events into
	// local journal/views.
	go peersync.RunPullLoop(
		ctx,
		logger,
		nodeConfig.ID,
		peerService,
		client,
		st.journal,
		views,
		eventsByID,
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

func setupNetwork(ctx context.Context) error {
	// Create the network bridge.
	err := cn.CreateBridge(ctx)
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
) (*or.Registry, error) {
	ocireg, err := startWorkloadInfrastructure(
		ctx,
		nodeID,
		peerService,
		logger,
		st,
		workloads,
		views,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"start workload infrastructure: %w",
			err,
		)
	}

	// Set node subnet.
	idx, err := nodeIndex(ctx, nodeID, peerService)
	if err == nil {
		cn.SetNodeSubnet(idx)
	}

	err = setupNetwork(ctx)
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
) (*transport.Client, error) {
	// Same IP resolution memberlist uses, so node cert IP
	// SANs match how peers dial.
	resolved := peerdiscovery.ResolveAdvertise(
		nodeConfig.MemberlistAddress,
		nodeConfig.AdvertiseAddress,
	)
	advertise := netip.Addr{}
	if resolved.IsValid() {
		advertise = resolved.Addr()
	}

	paths, err := certs.Ensure(nodeConfig.ID, advertise)
	if err != nil {
		return nil, fmt.Errorf("ensure certs: %w", err)
	}

	err = transport.Start(
		ctx,
		logger,
		paths.CA,
		paths.Cert,
		paths.Key,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"http/2 server failed to start: %w",
			err,
		)
	}

	client, err := transport.NewClient(
		paths.CA,
		paths.Cert,
		paths.Key,
	)
	if err != nil {
		return nil, fmt.Errorf("new http client: %w", err)
	}

	logger.Info(
		"https server started",
		zap.String("addr", ":"+transport.Port),
	)

	return client, nil
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
) (*journalview.EventsByID, *journalview.EventsByType, *journalview.Workloads, []journalview.View, error) {
	eventsByID := journalview.NewEventsByID(kv)
	eventsByNode := journalview.NewEventsByNode(kv)
	eventsByType := journalview.NewEventsByType(kv)
	workloads := journalview.NewWorkloads(kv)
	views := []journalview.View{
		eventsByID,
		eventsByNode,
		eventsByType,
		workloads,
	}

	err := journalview.RebuildViews(ctx, views)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf(
			"rebuild views: %w",
			err,
		)
	}

	return eventsByID, eventsByType, workloads, views, nil
}

// runDiscoveryLoop is the active discovery path. It
// periodically queries mDNS and DNS SRV for bootstrap
// candidates that are not yet in the memberlist and
// attempts to join them. It complements
// ObserveMemberlistPeers, which only watches already-joined
// members.
func runDiscoveryLoop(
	ctx context.Context,
	logger *zap.Logger,
	peerService *peerdiscovery.MemberService,
) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			discoverAndJoin(ctx, logger, peerService)
		}
	}
}

// discoverAndJoin performs one discovery round: resolve via
// MultiResolver (mDNS + DNS SRV), log candidates, and Join.
func discoverAndJoin(
	ctx context.Context,
	logger *zap.Logger,
	peerService *peerdiscovery.MemberService,
) {
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

	mdnsResolver := peerdiscovery.MDNSResolver{
		Timeout: 5 * time.Second,
	}
	dnsSrvResolver := peerdiscovery.DNSSRVResolver{
		Timeout: 5 * time.Second,
	}
	multi := peerdiscovery.NewMultiResolver(
		&mdnsResolver,
		&dnsSrvResolver,
	)
	addrs, err := multi.Resolve(ctx)
	if err != nil {
		logger.Warn(
			"peer discovery resolve failed",
			zap.Error(err),
		)
		return
	}
	if len(addrs) == 0 {
		logger.Debug("peer discovery: no candidates")
		return
	}

	addrs = filterJoinCandidates(
		addrs,
		localAddress,
		members,
	)
	if len(addrs) == 0 {
		logger.Debug("peer discovery: no new candidates")
		return
	}

	strs := make([]string, 0, len(addrs))
	for _, a := range addrs {
		strs = append(strs, a.String())
	}
	logger.Info(
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
		return
	}
	if n > 0 {
		logger.Info(
			"peer join succeeded",
			zap.Int("joined", n),
			zap.Strings("candidates", strs),
		)
	}
}

// filterJoinCandidates drops addresses we must not Join:
// this node's advertise address, and every address already
// in the memberlist (alive, suspect, or failed). Re-joining
// those retriggers self-peering and unexpected-node pings.
func filterJoinCandidates(
	addrs []netip.AddrPort,
	localAddress netip.AddrPort,
	members []peerdiscovery.Node,
) []netip.AddrPort {
	skip := make(map[netip.AddrPort]bool, len(members)+1)
	skip[localAddress] = true
	for _, m := range members {
		skip[m.Address] = true
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
) (*peerdiscovery.MemberService, error) {
	localNode := peerdiscovery.Node{
		ID: nodeConfig.ID,
		Address: netip.MustParseAddrPort(
			nodeConfig.MemberlistAddress.String(),
		),
		Metadata: peerdiscovery.NodeMetadata{
			WireGuardPublicKey: wgPublicKey,
		},
	}
	peerService, err := peerdiscovery.Start(
		logger,
		localNode,
		join,
		nodeConfig.AdvertiseAddress,
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
