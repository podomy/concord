// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package peerdiscovery

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/memberlist"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/geo"
	nodepackage "github.com/podomy/concord/internal/node"
)

// OverlayPrefix is the container and WireGuard mesh.
// Memberlist must not advertise or Join addresses in this
// range. It must stay disjoint from the underlay NIC.
var OverlayPrefix = netip.MustParsePrefix("10.0.0.0/16")

// MemberService wraps memberlist membership tracking for
// Concord peer discovery.
type MemberService struct {
	list     *memberlist.Memberlist
	delegate *nodeMetadataDelegate
	logger   *logs.Logger
}

// Adapter for the message levels coming from the memberlist
// library
type MemberlistLogWriter struct {
	logger *logs.Logger
}

// stripMemberlistToken cuts one [TAG] prefix plus the
// memberlist: token memberlist prepends to every line.
func stripMemberlistToken(s, tag string) string {
	rest := strings.TrimSpace(strings.TrimPrefix(s, tag))
	return strings.TrimSpace(
		strings.TrimPrefix(rest, "memberlist:"),
	)
}

func (w *MemberlistLogWriter) Write(p []byte) (int, error) {
	s := strings.TrimSpace(string(p))

	if strings.HasPrefix(s, "[WARN]") {
		rest := stripMemberlistToken(s, "[WARN]")
		w.logger.Warn(rest)

		return len(p), nil
	}

	if strings.HasPrefix(s, "[ERR]") {
		rest := stripMemberlistToken(s, "[ERR]")
		w.logger.Error(rest)

		return len(p), nil
	}

	if strings.HasPrefix(s, "[DEBUG]") {
		rest := stripMemberlistToken(s, "[DEBUG]")
		w.logger.Debug(rest)

		return len(p), nil
	}

	if strings.HasPrefix(s, "[INFO]") {
		rest := stripMemberlistToken(s, "[INFO]")
		w.logger.Info(rest)

		return len(p), nil
	}

	// Fallback to info logger.
	w.logger.Info(s)

	return len(p), nil
}

// Start creates a memberlist-backed peer discovery service
// for node.
//
// The node ID is used as memberlist's node name so
// discovered members can be mapped back to stable Concord
// identities. The node address is the local bind endpoint
// for memberlist traffic. Port 0 asks the OS for an
// ephemeral port;
// use LocalAddr after Start to learn the bound endpoint.
//
// advertise is an optional IP hint from config overriding
// which IP to publish (see ResolveAdvertise). A zero Addr
// means auto-detect.
//
// If join is non-empty, Start attempts to join those
// bootstrap addresses. A failed join does not abort
// startup: the node keeps running as a solo member
// (membership = this node only). Discovery may have
// returned a non-Concord host; soft-fail avoids taking the
// whole process down. Callers
// can retry Join later.
//
// identity is this node's Noise identity for gossip. An empty identity (nil
// Pub) advertises no Noise key; peers then cannot open Noise sessions to this
// node until it restarts with an identity.
func Start(
	logger *logs.Logger,
	node Node,
	join []netip.AddrPort,
	advertise netip.Addr,
	identity NoiseIdentity,
) (*MemberService, error) {
	config := memberlist.DefaultLocalConfig()
	config.Name = node.ID.String()
	config.BindAddr = node.Address.Addr().String()
	config.BindPort = int(node.Address.Port())

	// Production runs on real WLANs, not loopback: relax the probe path
	// toward LAN values so jitter spikes stop false-suspecting healthy
	// peers (see docs/scaling). Deliberately not the full LAN preset:
	// TCPTimeout stays 1s so dead anchors and TCP pings fail fast,
	// SuspicionMult stays 3 for quick orphan failover, push/pull stays
	// 15s for fast healing.
	config.ProbeTimeout = 500 * time.Millisecond
	config.IndirectChecks = 3
	config.RetransmitMult = 4
	config.GossipInterval = 200 * time.Millisecond

	// Gossip encryption uses a provisioned cluster-wide secret, identical
	// on every node. Both verify flags enforce encrypted-only gossip in
	// their direction. Key rotation has no protocol here: the key is fixed
	// at provision time and node-key rotation rides NoiseGeneration
	// (highest generation wins), not a gossip rekey.
	gossipKey, err := loadGossipKey()
	if err != nil {
		return nil, fmt.Errorf("load gossip key: %w", err)
	}
	config.SecretKey = gossipKey
	config.GossipVerifyIncoming = true
	config.GossipVerifyOutgoing = true
	if logger != nil {
		sum := sha256.Sum256(gossipKey)
		logger.Info("gossip key loaded", logs.String("sha256", hex.EncodeToString(sum[:])))
	}

	resolved := ResolveAdvertise(node.Address, advertise)
	if resolved.IsValid() {
		config.AdvertiseAddr = resolved.Addr().String()
		if resolved.Port() != 0 {
			config.AdvertisePort = int(resolved.Port())
		}
	}
	// Route memberlist's stdlib logger through logs (JSON
	// with the rest of the app).
	if logger != nil {
		config.Logger = log.New(
			&MemberlistLogWriter{
				logger: logger.Named("memberlist"),
			}, "", 0,
		)
	}

	cpuMHz, memoryMB, err := sampleCapacity()
	if err != nil {
		return nil, err
	}

	delegate := &nodeMetadataDelegate{}
	delegate.meta = func() NodeMetadata {
		return NodeMetadata{
			WireGuardPublicKey: node.Metadata.WireGuardPublicKey,
			CPUMHz:             cpuMHz,
			MemoryMB:           memoryMB,
			Workloads: int(
				delegate.workloads.Load(),
			),
			CPUPercent:      clampPressure(delegate.cpu.Load()),
			MemPercent:      clampPressure(delegate.mem.Load()),
			DiskPercent:     clampPressure(delegate.disk.Load()),
			Lat:             microToDeg(delegate.lat.Load()),
			Lon:             microToDeg(delegate.lon.Load()),
			Anchor:          delegate.anchor.Load(),
			NoisePublicKey:  identity.Pub,
			NoiseGeneration: identity.Generation,
		}
	}
	// Seed published with the initial snapshot so the first Publish is a
	// no-op: memberlist already gossips this meta on join, an immediate
	// UpdateNode would rebroadcast identical bytes.
	if err := seedPublished(delegate); err != nil {
		return nil, err
	}

	config.Delegate = delegate

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf(
			"memberlist bind %s: %w",
			node.Address,
			err,
		)
	}

	// Soft-fail join: bad discovery candidates must not
	// kill Concord. The local memberlist stays up;
	// membership is just this node until a
	// later successful Join or inbound gossip.
	if len(join) > 0 {
		if _, err := list.Join(addrPortsToStrings(join)); err != nil {
			if logger != nil {
				logger.Warn(
					"memberlist join failed; continuing alone",
					logs.Error(err),
					logs.Int("candidates", len(join)),
				)
			}
		}
	}

	memberService := MemberService{
		list:     list,
		delegate: delegate,
		logger:   logger,
	}

	return &memberService, nil
}

// sampleCapacity reads the static capacity snapshot gossiped as
// NodeMetadata: CPU frequency and total memory.
func sampleCapacity() (float64, uint64, error) {
	cpuMHz, err := nodepackage.GetCPUFreqMHz()
	if err != nil {
		return 0, 0, fmt.Errorf("get cpu freq: %w", err)
	}

	memoryMB, err := nodepackage.GetTotalMemoryMB()
	if err != nil {
		return 0, 0, fmt.Errorf("get total memory mb: %w", err)
	}

	return cpuMHz, memoryMB, nil
}

// loadGossipKey reads the provisioned cluster-wide gossip encryption key.
// The key file must hold 16, 24, or 32 raw bytes (AES-128/192/256) and be
// identical on every node; it is never generated here. The file must be
// mode 0600 or stricter: fail closed on group or other access, matching
// ca.key handling in certs.
func loadGossipKey() ([]byte, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return nil, fmt.Errorf("user config dir: %w", err)
	}

	keyPath := filepath.Join(dir, "concord", "memberservice", "secret.key")
	info, err := os.Stat(keyPath)
	if err != nil {
		return nil, fmt.Errorf("stat gossip key %s: %w", keyPath, err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		return nil, fmt.Errorf(
			"refusing gossip key %s with mode %04o: want 0600 (group and others must have no access)",
			keyPath,
			perm,
		)
	}
	// #nosec G304: path is local runtime configuration, not user-controlled input.
	key, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read gossip key %s: %w", keyPath, err)
	}

	if len(key) != 16 && len(key) != 24 && len(key) != 32 {
		return nil, fmt.Errorf(
			"invalid gossip key length: got %d bytes, want 16, 24, or 32 raw bytes (a trailing newline from echo or an editor makes a valid key invalid; write raw bytes with no newline)",
			len(key),
		)
	}

	return key, nil
}

// SetWorkloadCount updates the count of active local
// workloads reported in node gossip metadata.
func (m *MemberService) SetWorkloadCount(n int) {
	if m == nil || m.delegate == nil {
		return
	}
	// #nosec G115 -- workload count n fits safely within
	// int32 range
	m.delegate.workloads.Store(int32(n))
}

// SetPressure updates the utilization trio reported in node gossip
// metadata. Values are 0-100 percents sampled on the fast beat.
func (m *MemberService) SetPressure(cpu, mem, disk uint8) {
	if m == nil || m.delegate == nil {
		return
	}
	m.delegate.cpu.Store(uint32(cpu))
	m.delegate.mem.Store(uint32(mem))
	m.delegate.disk.Store(uint32(disk))
}

// SetPosition updates the geographic position reported in node gossip
// metadata. Out-of-planet coordinates are ignored, keeping the previous
// position: callers retry with a real fix next beat. This moves gossip
// only; history needs Sampler.RecordPosition too. The IPC position
// setter applies both together.
func (m *MemberService) SetPosition(lat, lon float64) {
	if m == nil || m.delegate == nil {
		return
	}
	p := geo.Point{Lat: lat, Lon: lon}
	if !p.Valid() {
		return
	}
	m.delegate.lat.Store(degToMicro(lat))
	m.delegate.lon.Store(degToMicro(lon))
}

// Publish pushes current metadata to the mesh when anything volatile
// changed since the last push. Static identity never differs, so it is
// not compared; see equalVolatile for the compared set. Unchanged values
// skip the broadcast entirely. Callers are the fast beat (after sampling)
// and the discovery beat (after config refresh), hence the mutex: Set*
// stay lock-free atomics for the gossip hot path while Publish serializes
// the read-compare-broadcast sequence.
func (m *MemberService) Publish() error {
	if m == nil || m.delegate == nil || m.list == nil {
		return nil
	}
	d := m.delegate
	d.pubMu.Lock()
	defer d.pubMu.Unlock()

	current := d.meta()
	if current.equalVolatile(d.published) {
		return nil
	}
	// Pre-flight the gossip cap: UpdateNode panics past MetaMaxSize, so
	// oversize metadata fails here with an error instead. See
	// nodeMetaLimit.
	if err := checkMetaSize(current); err != nil {
		return err
	}
	err := m.list.UpdateNode(publishTimeout)
	if err != nil {
		return fmt.Errorf("update node metadata: %w", err)
	}
	d.published = current
	return nil
}

// degToMicro stores decimal degrees as microdegrees for atomic storage.
func degToMicro(v float64) int32 {
	return int32(math.Round(v * 1e6))
}

// microToDeg renders stored microdegrees back to decimal degrees.
func microToDeg(v int32) float64 {
	return float64(v) / 1e6
}

// SetAnchor marks this node as a rendezvous anchor in gossip metadata.
func (m *MemberService) SetAnchor(anchored bool) {
	if m == nil || m.delegate == nil {
		return
	}
	m.delegate.anchor.Store(anchored)
}

// clampPressure narrows a stored 0-100 utilization to uint8.
func clampPressure(v uint32) uint8 {
	if v > 100 {
		return 100
	}
	return uint8(v)
}

// ResolveAdvertise picks the address other peers should
// dial when contacting this node. Callers pass the
// memberlist bind address and an optional config
// override.
//
// advertise is the operator-supplied IP hint
// (AdvertiseAddress from config). Empty (zero Addr) means
// the runtime picks one from the bind address or a
// non-loopback interface. The port always comes from bind
// regardless.
//
// Resolution order:
// 1. Advertise hint if set (IP from config, port from
// bind). 2. The bind address if it is already a real IP
// peers can dial. 3. Otherwise the first usable host
// interface IP, with the bind port.
//
// Returns the zero AddrPort only if no usable address
// exists; memberlist will
// then choose an interface on its own.
func ResolveAdvertise(
	bind netip.AddrPort,
	advertise netip.Addr,
) netip.AddrPort {
	// Operator override: which IP to publish. Port always
	// matches bind so
	// peers dial the same port memberlist is listening on.
	if advertise.IsValid() {
		return netip.AddrPortFrom(advertise, bind.Port())
	}

	// Bind is already a specific IP (not 0.0.0.0 / ::).
	// Peers can dial that same address, so advertise it
	// as-is. IsUnspecified() is true only for the wildcard
	// addresses that mean "listen on every interface".
	if bind.Addr().IsValid() &&
		!bind.Addr().IsUnspecified() {
		return bind
	}

	// Bind is a wildcard (listen everywhere). Peers cannot
	// dial 0.0.0.0, so take the first non-loopback underlay
	// IP and attach the bind port.
	return firstRoutableAddrPort(bind.Port())
}

// skipAdvertiseIface reports overlay interfaces. cn0 and
// wg-*
// carry 10.0.0.0/16; advertising them makes Join hit the
// local bridge instead of a peer.
func skipAdvertiseIface(name string) bool {
	return name == "cn0" || strings.HasPrefix(name, "wg-")
}

// firstRoutableAddrPort returns the first underlay host
// address with the given port. Skips loopback, cn0, wg-*,
// and 10.0.0.0/16. Used when bind is 0.0.0.0.
func firstRoutableAddrPort(port uint16) netip.AddrPort {
	ifaces, err := net.Interfaces()
	if err != nil {
		return netip.AddrPort{}
	}
	for _, iface := range ifaces {
		if skipAdvertiseIface(iface.Name) {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, ia := range addrs {
			addr, ok := addrFromInterface(ia)
			if !ok {
				continue
			}
			if OverlayPrefix.Contains(addr) {
				continue
			}
			return netip.AddrPortFrom(addr, port)
		}
	}
	return netip.AddrPort{}
}

// addrFromInterface converts a net.Addr from
// net.InterfaceAddrs into a dialable netip.Addr. Returns
// false for anything peers should not use: non-IP nets, nil
// IPs, loopback, link-local, or unspecified addresses.
func addrFromInterface(ia net.Addr) (netip.Addr, bool) {
	// InterfaceAddrs yields *net.IPNet for IP
	// configurations.
	ipNet, ok := ia.(*net.IPNet)
	if !ok || ipNet.IP == nil {
		return netip.Addr{}, false
	}

	ip := ipNet.IP
	// Filter before conversion: loopback (127.0.0.1),
	// link-local (169.254.x / fe80::), and unspecified
	// (0.0.0.0) are not useful cluster endpoints.
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() ||
		ip.IsUnspecified() {
		return netip.Addr{}, false
	}

	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.Addr{}, false
	}

	// Unmap turns IPv4-mapped IPv6 (::ffff:a.b.c.d) into
	// plain IPv4 so peers
	// see a normal address form.
	addr = addr.Unmap()
	// Re-check after Unmap in case mapping produced a
	// still-unusable address.
	if !addr.IsValid() || addr.IsLoopback() ||
		addr.IsLinkLocalUnicast() {
		return netip.Addr{}, false
	}
	return addr, true
}

// LocalAddr returns the address memberlist is advertising
// for this node.
func (m *MemberService) LocalAddr() (netip.AddrPort, error) {
	n := m.list.LocalNode()
	addr, ok := netip.AddrFromSlice(n.Addr)
	if !ok {
		return netip.AddrPort{}, fmt.Errorf(
			"invalid local member address: %v",
			n.Addr,
		)
	}
	return netip.AddrPortFrom(addr.Unmap(), n.Port), nil
}

// Join asks the running memberlist service to join existing
// peer addresses.
//
// The addresses are bootstrap endpoints, not durable peer
// identities. After a successful join, memberlist exchanges
// membership state and Members can return
// the reachable nodes known to the local process.
func (m *MemberService) Join(
	existingMembers []netip.AddrPort,
) (int, error) {
	n, err := m.list.Join(
		addrPortsToStrings(existingMembers),
	)
	if err != nil {
		return 0, fmt.Errorf("list join: %w", err)
	}

	return n, nil
}

// Members returns the current memberlist view as Concord
// peer discovery nodes.
//
// Memberlist stores each member's identity in Node.Name.
// Concord writes UUIDs into that field when starting
// memberlist, so this method parses the name back into a
// uuid.UUID and pairs it with the member's advertised
// address.
//
// One corrupt record never fails the whole view: an unparseable name, an
// undecodable address, and bad metadata each skip that member with a
// warning and the remaining members are still returned. Remote gossip is
// untrusted input, so every record is re-validated here; see
// sanitizeNodeMeta. The local write path (SetPressure, SetPosition,
// Publish) stays authoritative for what this node sends.
func (m *MemberService) Members() ([]Node, error) {
	members := m.list.Members()

	out := make([]Node, 0, len(members))

	for _, member := range members {
		id, err := uuid.Parse(member.Name)
		if err != nil {
			m.warnSkip("skip member with bad id", err, member.Name)
			continue
		}

		addr, ok := netip.AddrFromSlice(member.Addr)
		if !ok {
			m.warnSkip("skip member with bad address", nil, member.Name)
			continue
		}

		var meta NodeMetadata
		if len(member.Meta) > 0 {
			err = json.Unmarshal(member.Meta, &meta)
			if err != nil {
				m.warnSkip("skip member with bad metadata", err, member.Name)
				continue
			}
		}

		out = append(out, Node{
			ID:       id,
			Address:  netip.AddrPortFrom(addr, member.Port),
			State:    memberState(member.State),
			Metadata: sanitizeNodeMeta(meta),
		})
	}

	return out, nil
}

// warnSkip logs a skipped-member warning when a logger is configured.
func (m *MemberService) warnSkip(msg string, err error, name string) {
	if m == nil || m.logger == nil {
		return
	}
	if err != nil {
		m.logger.Warn(msg, logs.Error(err), logs.String("name", name))
		return
	}
	m.logger.Warn(msg, logs.String("name", name))
}

// sanitizeNodeMeta re-validates remote gossip metadata on the Members read
// path and returns a sanitized copy. Pressure percents clamp to 0-100,
// negative workload counts floor at zero, and a position that fails Valid
// resets to unknown (0, 0). The delegate snapshot is never mutated; the
// local write path stays authoritative for what this node sends.
func sanitizeNodeMeta(meta NodeMetadata) NodeMetadata {
	if meta.CPUPercent > 100 {
		meta.CPUPercent = 100
	}
	if meta.MemPercent > 100 {
		meta.MemPercent = 100
	}
	if meta.DiskPercent > 100 {
		meta.DiskPercent = 100
	}
	if meta.Workloads < 0 {
		meta.Workloads = 0
	}
	if !(geo.Point{Lat: meta.Lat, Lon: meta.Lon}.Valid()) {
		meta.Lat = 0
		meta.Lon = 0
	}
	return meta
}

// Shutdown stops the memberlist service and releases its
// network resources.
func (m *MemberService) Shutdown() error {
	err := m.list.Shutdown()
	if err != nil {
		return fmt.Errorf("memberservice shutdown: %w", err)
	}

	return nil
}

// addrPortsToStrings converts strongly typed endpoints into
// the string format
// expected by memberlist.Join.
func addrPortsToStrings(
	addresses []netip.AddrPort,
) []string {
	out := make([]string, 0, len(addresses))
	for _, address := range addresses {
		out = append(out, address.String())
	}

	return out
}

// memberState maps memberlist's liveness states into
// Concord peer discovery states.
func memberState(state memberlist.NodeStateType) NodeState {
	switch state {
	case memberlist.StateAlive:
		return NodeStateAlive
	case memberlist.StateSuspect:
		return NodeStateSuspect
	case memberlist.StateDead:
		return NodeStateDead
	case memberlist.StateLeft:
		return NodeStateLeft
	default:
		return NodeStateUnknown
	}
}

// nodeMetadataDelegate implements memberlist.Delegate to serialize and
// gossip local node metadata (pressure trio, workload count, position, and
// anchor role) across cluster peers. Volatile fields stay lock-free
// atomics so memberlist's gossip thread (NodeMeta) never blocks; pubMu
// serializes only Publish's read-compare-broadcast, which runs on the fast
// beat and the discovery beat concurrently.
type nodeMetadataDelegate struct {
	meta      func() NodeMetadata
	workloads atomic.Int32
	cpu       atomic.Uint32
	mem       atomic.Uint32
	disk      atomic.Uint32
	lat       atomic.Int32
	lon       atomic.Int32
	anchor    atomic.Bool
	// pubMu guards published only.
	pubMu sync.Mutex
	// published is the last metadata pushed to the mesh. Compared with
	// equalVolatile on every Publish so unchanged values never rebroadcast.
	published NodeMetadata
}

// publishTimeout bounds one metadata broadcast. The wait covers queueing
// the alive message, not a full gossip round.
const publishTimeout = time.Second

// nodeMetaLimit caps gossiped metadata at memberlist's MetaMaxSize. The
// library panics past the cap, so Start and Publish pre-flight the length
// and fail with an error instead. Wire shape unchanged: still JSON
// NodeMetadata under the same cap.
const nodeMetaLimit = 512

// seedPublished snapshots the initial metadata so the first Publish is a
// no-op, and fails when the snapshot already exceeds the gossip cap: Create
// panics past MetaMaxSize, so an error here names the cause instead of
// crashing the process. See nodeMetaLimit.
func seedPublished(delegate *nodeMetadataDelegate) error {
	delegate.published = delegate.meta()
	return checkMetaSize(delegate.published)
}

// checkMetaSize fails when meta exceeds the gossip cap, before the bytes
// reach memberlist (which panics past MetaMaxSize). Used by Start for the
// initial snapshot and by Publish before UpdateNode.
func checkMetaSize(meta NodeMetadata) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal node metadata: %w", err)
	}
	if len(metaBytes) > nodeMetaLimit {
		return fmt.Errorf(
			"node metadata %d bytes exceeds %d byte gossip cap",
			len(metaBytes),
			nodeMetaLimit,
		)
	}
	return nil
}

// NodeMeta produces JSON-serialized metadata for this node
// to be included
// in memberlist ping and gossip packets. Over-limit output panics inside
// the library; callers that push new metadata (Start, Publish) pre-flight
// against nodeMetaLimit so this never fires.
func (d *nodeMetadataDelegate) NodeMeta(limit int) []byte {
	bytes, err := json.Marshal(d.meta())
	if err != nil {
		return nil
	}
	return bytes
}

// NotifyMsg receives user-data messages from cluster peers
// (unused by Concord).
func (d *nodeMetadataDelegate) NotifyMsg([]byte) {}

// GetBroadcasts returns user broadcast messages to
// piggyback on gossip rounds (unused by Concord).
func (d *nodeMetadataDelegate) GetBroadcasts(
	int,
	int,
) [][]byte {
	return nil
}

// LocalState returns arbitrary local state for full
// push/pull state sync (unused by Concord).
func (d *nodeMetadataDelegate) LocalState(
	bool,
) []byte {
	return nil
}

// MergeRemoteState processes full push/pull state sync
// received from peers (unused by Concord).
func (d *nodeMetadataDelegate) MergeRemoteState(
	[]byte,
	bool,
) {
}
