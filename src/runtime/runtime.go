// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"go.uber.org/zap"

	"github.com/podomy/concord/src/certs"
	"github.com/podomy/concord/src/cr"
	"github.com/podomy/concord/src/dnsserver"
	"github.com/podomy/concord/src/journalview"
	"github.com/podomy/concord/src/kvstore"
	"github.com/podomy/concord/src/node"
	"github.com/podomy/concord/src/or"
	"github.com/podomy/concord/src/peerdiscovery"
	"github.com/podomy/concord/src/peersync"
	"github.com/podomy/concord/src/reconciler"
	"github.com/podomy/concord/src/transport"
)

// Run performs application startup, blocks for the process lifetime, and handles graceful shutdown.
func Run(ctx context.Context, logger *zap.Logger) error {
	// Load persistent identity for this node, creating one if none exists.
	nodeConfig, err := node.LoadOrCreateNodeConfig()
	if err != nil {
		return fmt.Errorf("load node config: %w", err)
	}

	// The ip address and port
	if !nodeConfig.MemberlistAddress.IsValid() {
		nodeConfig.MemberlistAddress = netip.MustParseAddrPort("0.0.0.0:7946")
	}

	st, err := openStores()
	if err != nil {
		// error was wrapped inside open stores
		return err
	}
	defer closeStores(logger, st)

	eventsByID, eventsByType, views, err := setupViews(ctx, st.kv)
	if err != nil {
		return fmt.Errorf("setup views: %w", err)
	}

	// Create a startup event and persist it before announcing readiness.
	if err = journalview.RecordNodeStarted(ctx, logger, st.journal, views, nodeConfig.ID, nodeConfig.MemberlistAddress); err != nil {
		return fmt.Errorf("record node started: %w", err)
	}

	addresses := resolvePeersOrEmpty(ctx, logger)

	peerService, err := startPeerService(logger, nodeConfig, addresses)
	if err != nil {
		return err
	}
	defer shutdownPeerService(logger, peerService)
	go peerdiscovery.ObservePeers(ctx, logger, nodeConfig.ID, peerService, st.journal, views)

	stopMDNS, err := startMDNSAdvertise(ctx, logger, nodeConfig)
	if err != nil {
		return err
	}
	defer stopMDNS()

	err = dnsserver.Start(ctx, peerService, logger, "")
	if err != nil {
		return fmt.Errorf("dns server start failed: %w", err)
	}
	logger.Info("DNS server started")

	client, err := startTransport(ctx, logger, *nodeConfig)
	if err != nil {
		return err
	}
	// Reconciliation loop: pull peers and apply events into local journal/views.
	go peersync.RunPullLoop(ctx, logger, nodeConfig.ID, peerService, client, st.journal, views, eventsByID)
	logger.Info("peer sync pull loop started")

	// Start the OCI registry
	ocireg, err := startOCIRegistry(ctx)
	if err != nil {
		return err
	}
	defer ocireg.Stop()
	logger.Info("oci registry started", zap.Int("port", or.Port))

	// Start the workload reconciler loop.
	puller := cr.NewImagePuller()
	crRuntime, err := cr.NewRuntime()
	if err != nil {
		return fmt.Errorf("container runtime: %w", err)
	}
	go reconciler.RunLoop(ctx, logger, nodeConfig.ID, puller, crRuntime, st.journal, eventsByType)
	logger.Info("workload reconciler started")

	// Block until the OS delivers a shutdown signal.
	<-ctx.Done()
	logger.Info("shutting down", zap.String("node_id", nodeConfig.ID.String()))

	return nil
}

func resolvePeersOrEmpty(ctx context.Context, logger *zap.Logger) []netip.AddrPort {
	addresses, err := startResolvers(ctx, logger)
	if err != nil {
		// Soft-fail: discovery noise must not kill the node.
		logger.Warn("peer resolve failed; continuing alone", zap.Error(err))
		return nil
	}
	return addresses
}

func startTransport(ctx context.Context, logger *zap.Logger, nodeConfig node.NodeConfig) (*transport.Client, error) {
	// Same IP resolution memberlist uses, so node cert IP SANs match how peers dial.
	resolved := peerdiscovery.ResolveAdvertise(nodeConfig.MemberlistAddress, nodeConfig.AdvertiseAddress)
	advertise := netip.Addr{}
	if resolved.IsValid() {
		advertise = resolved.Addr()
	}

	paths, err := certs.Ensure(nodeConfig.ID, advertise)
	if err != nil {
		return nil, fmt.Errorf("ensure certs: %w", err)
	}

	if err := transport.Start(ctx, logger, paths.CA, paths.Cert, paths.Key); err != nil {
		return nil, fmt.Errorf("http/2 server failed to start: %w", err)
	}

	client, err := transport.NewClient(paths.CA, paths.Cert, paths.Key)
	if err != nil {
		return nil, fmt.Errorf("new http client: %w", err)
	}

	logger.Info("https server started", zap.String("addr", ":"+transport.Port))

	return client, nil
}

func closeStores(logger *zap.Logger, st *stores) {
	if err := st.kv.Close(); err != nil {
		logger.Error("close kv store", zap.Error(err))
	}
	if err := st.journal.Close(); err != nil {
		logger.Error("close journal", zap.Error(err))
	}
}

func shutdownPeerService(logger *zap.Logger, ps *peerdiscovery.MemberService) {
	if err := ps.Shutdown(); err != nil {
		logger.Error("shutdown peer service", zap.Error(err))
	}
}

func setupViews(ctx context.Context, kv *kvstore.KVStore) (*journalview.EventsByID, *journalview.EventsByType, []journalview.View, error) {
	eventsByID := journalview.NewEventsByID(kv)
	eventsByNode := journalview.NewEventsByNode(kv)
	eventsByType := journalview.NewEventsByType(kv)
	views := []journalview.View{eventsByID, eventsByNode, eventsByType}

	if err := journalview.RebuildViews(ctx, views); err != nil {
		return nil, nil, nil, fmt.Errorf("rebuild views: %w", err)
	}

	return eventsByID, eventsByType, views, nil
}

func startResolvers(ctx context.Context, logger *zap.Logger) ([]netip.AddrPort, error) {
	mdnsResolver := peerdiscovery.MDNSResolver{Timeout: 5 * time.Second}
	dnsSrvResolver := peerdiscovery.DNSSRVResolver{Timeout: 5 * time.Second}

	multiResolver := peerdiscovery.NewMultiResolver(&mdnsResolver, &dnsSrvResolver)
	addrs, err := multiResolver.Resolve(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve: %w", err)
	}
	logger.Info("peer resolvers finished", zap.Int("addresses", len(addrs)))
	return addrs, nil
}

func startMDNSAdvertise(ctx context.Context, logger *zap.Logger, nodeConfig *node.NodeConfig) (func(), error) {
	mdnsServer, err := peerdiscovery.MDNSAdvertise(ctx, nodeConfig)
	if err != nil {
		return nil, fmt.Errorf("mdns advertise: %w", err)
	}
	logger.Info("mDNS advertise started")
	return func() {
		if err := mdnsServer.Shutdown(); err != nil {
			logger.Error("mdns advertise shutdown", zap.Error(err))
		}
	}, nil
}

func startOCIRegistry(ctx context.Context) (*or.Registry, error) {
	ocireg, err := or.New()
	if err != nil {
		return nil, fmt.Errorf("oci registry new: %w", err)
	}
	if err := ocireg.Start(ctx); err != nil {
		return nil, fmt.Errorf("oci registry start: %w", err)
	}
	return ocireg, nil
}

func startPeerService(logger *zap.Logger, nodeConfig *node.NodeConfig, join []netip.AddrPort) (*peerdiscovery.MemberService, error) {
	localNode := peerdiscovery.Node{
		ID:      nodeConfig.ID,
		Address: netip.MustParseAddrPort(nodeConfig.MemberlistAddress.String()),
	}
	peerService, err := peerdiscovery.Start(logger, localNode, join, nodeConfig.AdvertiseAddress)
	if err != nil {
		return nil, fmt.Errorf("start peer discovery: %w", err)
	}
	localAddr, err := peerService.LocalAddr()
	if err != nil {
		return nil, fmt.Errorf("get local address: %w", err)
	}
	logger.Info("peer discovery started",
		zap.String("bind", nodeConfig.MemberlistAddress.String()),
		zap.String("advertise", localAddr.String()),
	)

	return peerService, nil
}
