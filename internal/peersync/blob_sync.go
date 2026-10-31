// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package peersync replicates blobs peer to peer with want/have anti-entropy.
// Wanted digests come from specs; peer tag listings show what each peer has.
// Missing roots fetch as manifest plus chunks or as raw bytes converted to chunks.
// Cursors track next chunk per peer and root so restarts resume. Workers stagger
// starts and cap concurrency so ticks stay flat. AP by design: progress on
// contact, block on absence, resume without rework.
package peersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/blob"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/peerdiscovery"
	"github.com/podomy/concord/internal/registry"
	"github.com/podomy/concord/internal/transport"
	"github.com/podomy/concord/internal/workload"
)

// BlobManifest is one content-addressed blob as an ordered chunk list.
// It aliases the blob package contract so the wire format has exactly
// one definition.
type BlobManifest = blob.Manifest

// blobIface is the local blob store the syncer replicates into. It
// lists exactly the *blob.Store methods the syncer needs, so the
// registry store satisfies it with no adapter: the compile-time
// assertion below proves the runtime wiring. Chunks ride PutRaw (each
// at most the fixed chunk size, so memory stays flat); manifests ride
// PutRaw plus PutTag under the sync repo, which doubles as the
// completion record and the publish index peers discover.
type blobIface interface {
	HasBlob(digest string) bool
	HasRaw(digest string) bool
	OpenBlob(digest string) (io.ReadCloser, int64, error)
	PutRaw(ctx context.Context, digest string, data []byte) error
	GetRaw(digest string) ([]byte, error)
	PutTag(repo, tag, digest string) error
	ResolveTag(repo, tag string) (string, bool)
	ListTags(repo string) []string
}

// blobStoreConformance proves *blob.Store satisfies blobIface, so the
// runtime passes the registry store straight into New.
var _ blobIface = (*blob.Store)(nil)

// BlobSyncer replicates blobs peer to peer to replace the blob sync
// extension. The journal stays pointers-only: specs carry digests, and
// this loop fetches the bytes from peer registries over plain HTTP to
// the registry port. Verification is by digest, so the mesh needs no
// extra trust beyond the prior sync. It blocks instead of
// half-provisioning: Ensure reports an error until every wanted digest
// is complete, and the reconciler must not start a workload before
// Ensure returns nil.
//
// Two shapes replicate. Chunked roots arrive as manifest plus chunks
// and resume per chunk from persisted cursors. Raw content digests
// (file bytes, image layers) arrive as one stream, split locally into
// fixed pieces, and publish as an alias manifest so later ticks and
// peers use the resumable chunked path. Anti-entropy runs every
// blobPullInterval, mirroring the journal pull loop cadence. Each tick
// tries wanted digests first, then peer-published tags, against alive
// peers in a rotated order with staggered worker starts and at most
// four concurrent fetches. One failure soft-fails and retries next
// tick; partitions resume monotonically because cursors never retreat.
// There is no all to all exchange: each digest comes from the first
// peer that serves it.
type BlobSyncer struct {
	store   blobIface
	members *peerdiscovery.MemberService
	logger  *logs.Logger
	client  *transport.BlobClient
	mu      sync.Mutex
	wanted  map[string]struct{}
	cursors *blobCursorStore
	ticks   uint64
}

// Blob pull cadence mirrors the journal loop defaultPullInterval so blob
// and journal convergence share one beat.
const blobPullInterval = 5 * time.Second

// blobMaxConcurrent caps concurrent root fetches per tick. Memory stays
// flat because each fetch streams at most one chunk into memory.
const blobMaxConcurrent = 4

// blobWorkerStagger spaces worker starts so concurrent roots do not
// open connections in the same instant. Each worker adds 0-200ms of
// random jitter on top of its stagger slot, so restarts do not burst
// in lockstep. Workers stay at 4 and roots at 8 per tick.
const blobWorkerStagger = 100 * time.Millisecond

// blobWorkerJitterMax bounds the per worker start jitter.
const blobWorkerJitterMax = 200 * time.Millisecond

// blobMaxRootsPerTick bounds per-tick fetch work so one far-behind
// peer cannot starve the loop, mirroring the journal loop page cap.
// Wanted digests go first; discovered tags fill the remainder with a
// rotating start for fairness.
const blobMaxRootsPerTick = 8

// blobPeerOrPortDefault is the registry port fallback. The live value
// comes from registry.Port; this constant only covers a zero or garbage value.
const blobPeerOrPortDefault = 8444

// blobSyncRepo is the registry repository holding sync metadata: one
// tag per replicated root pointing at its manifest JSON. Blob bytes
// themselves are repo-independent on the wire.
const blobSyncRepo = transport.BlobRepo

// blobFileRepo is the file artifact repo holding workload file bytes.
// Blobs are repo-independent on the wire, so fetching via either repo
// reaches the same bytes. Discovery lists tags from both repos so file
// and image artifacts converge even when only one repo publishes.
const blobFileRepo = "concord-files"

// maxBlobRawBytes caps one raw content fetch. It matches the manifest
// total size cap so no single blob can exhaust disk.
const maxBlobRawBytes = 32 << 30

// maxManifestStored caps manifest JSON read back from the local store.
// Manifests are small by construction; anything larger is corrupt.
const maxManifestStored = 1 << 20

// New creates a blob syncer. Cursors start memory only; call
// AttachCursorStore before Run for bbolt persistence across restarts.
// A nil logger becomes a no-op logger.
func New(store blobIface, members *peerdiscovery.MemberService, logger *logs.Logger) *BlobSyncer {
	if logger == nil {
		logger = logs.NewNop()
	}
	return &BlobSyncer{
		store:   store,
		members: members,
		logger:  logger,
		client:  transport.NewBlobClient(),
		wanted:  make(map[string]struct{}),
		cursors: newBlobCursorStore(nil),
	}
}

// AttachCursorStore wires bbolt persistence for fetch cursors. Call once
// before Run with the shared KVStore; a nil argument keeps RAM-only
// cursors for tests. Entries saved between New and Attach carry over
// into the new store instead of being discarded. It holds b.mu so a
// concurrent tick cannot race the pointer swap. Loads consult the
// database on memory miss, so a stale zero never hides a larger
// persisted cursor.
func (b *BlobSyncer) AttachCursorStore(kv *kvstore.KVStore) {
	if kv == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	next := newBlobCursorStore(kv)
	b.cursors.mu.Lock()
	cloned := make(map[string]uint64, len(b.cursors.mem))
	maps.Copy(cloned, b.cursors.mem)
	b.cursors.mu.Unlock()
	next.mem = cloned
	b.cursors = next
}

// cursorStore returns the active cursor store under b.mu so Attach
// cannot race tick fetches on the pointer itself. The store keeps its
// own lock for entry access.
func (b *BlobSyncer) cursorStore() *blobCursorStore {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.cursors
}

// WantedFromSpecs returns the deduped file digests a set of workload
// specs needs, first seen order. The reconciler must call Ensure with
// these file digests before start. Image config and layer digests are
// not in Spec: the caller resolves the image reference against the
// local registry manifest (like the CR puller does) and merges those
// digests via WantedWithImages before Ensure. Image tags stay local:
// tag mappings are mutable and only the bytes replicate, so workloads
// should pin image digests.
func WantedFromSpecs(specs []workload.Spec) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, spec := range specs {
		for _, file := range spec.Files {
			if !transport.ValidBlobDigest(file.Digest) {
				continue
			}
			if _, ok := seen[file.Digest]; ok {
				continue
			}
			seen[file.Digest] = struct{}{}
			out = append(out, file.Digest)
		}
	}
	return out
}

// WantedWithImages merges file digests from specs with known image
// config and layer digests, deduped first seen order. Files come
// first so file misses report first. Invalid image entries are
// skipped, same as WantedFromSpecs skips bad file entries.
func WantedWithImages(specs []workload.Spec, imageDigests []string) []string {
	files := WantedFromSpecs(specs)
	seen := make(map[string]struct{}, len(files)+len(imageDigests))
	var out []string
	for _, digest := range files {
		if _, ok := seen[digest]; ok {
			continue
		}
		seen[digest] = struct{}{}
		out = append(out, digest)
	}
	for _, digest := range imageDigests {
		if !transport.ValidBlobDigest(digest) {
			continue
		}
		if _, ok := seen[digest]; ok {
			continue
		}
		seen[digest] = struct{}{}
		out = append(out, digest)
	}
	return out
}

// Run starts the anti-entropy loop and blocks until ctx is cancelled.
// Every tick lists members for tag discovery; fetch work is skipped
// when there is nothing wanted or published.
func (b *BlobSyncer) Run(ctx context.Context) {
	ticker := time.NewTicker(blobPullInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.tickBlobs(ctx)
		}
	}
}

// Ensure records digests as wanted and fetches them now from alive
// peers. It returns nil only when every digest is locally complete, so
// the reconciler path gates container start on it: a missing blob fails
// the start and the next tick retries, never a half-provisioned rootfs.
// Blobs are repo-independent on the wire, so one fetch via the sync
// repo reaches bytes pushed to either concord-blobs or concord-files.
// Call with file digests from WantedFromSpecs plus image config and
// layers via WantedWithImages when known.
func (b *BlobSyncer) Ensure(ctx context.Context, digests []string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}
	if b.store == nil {
		return fmt.Errorf("blob syncer has no store") //nolint:perfsprint // plain error, no wrap target
	}
	clean, err := cleanDigests(digests)
	if err != nil {
		return err
	}
	b.rememberWanted(clean)
	if len(clean) == 0 {
		return nil
	}
	peers, err := b.collectBlobPeers()
	if err != nil {
		return err
	}
	failed := b.fetchWantedRoots(ctx, clean, peers)
	if len(failed) == 0 {
		return nil
	}
	return fmt.Errorf("blob sync: %d of %d digests still missing: %s", len(failed), len(clean), missingSummary(failed))
}

// EnsureRepoDigests lists tags from peers for both sync repos and
// fetches missing roots. It covers concord-blobs and concord-files so
// file and image artifacts converge even when only one repo publishes.
// Fetch uses the repo-independent blob namespace, so one fetch path
// serves both. It returns the roots still incomplete with last errors.
func (b *BlobSyncer) EnsureRepoDigests(ctx context.Context, peers []blobPeer) map[string]error {
	failed := make(map[string]error)
	if err := ctx.Err(); err != nil {
		return failed
	}
	if b.store == nil {
		return failed
	}
	tags := b.collectRepoTags(ctx, peers)
	clean := filterValidDigests(tags)
	if len(clean) == 0 {
		return failed
	}
	return b.fetchWantedRoots(ctx, clean, peers)
}

// collectRepoTags lists tags from both sync repos across peers.
func (b *BlobSyncer) collectRepoTags(ctx context.Context, peers []blobPeer) []string {
	var tags []string
	for _, repo := range []string{blobSyncRepo, blobFileRepo} {
		for _, peer := range peers {
			if ctx.Err() != nil {
				return tags
			}
			tags = append(tags, b.peerRepoTags(ctx, peer, repo)...)
		}
	}
	slices.Sort(tags)
	return slices.Compact(tags)
}

// peerRepoTags fetches one peer and repo tag list as digests.
// Failures log at debug and yield nothing; the next peer or tick retries.
func (b *BlobSyncer) peerRepoTags(ctx context.Context, peer blobPeer, repo string) []string {
	listed, err := b.client.FetchTags(ctx, peer.base, repo)
	if err != nil {
		b.logger.Debug("blob tag discovery failed",
			logs.String("peer_id", peer.id.String()),
			logs.String("repo", repo),
			logs.Error(err),
		)
		return nil
	}
	out := make([]string, 0, len(listed))
	for _, tag := range listed {
		out = append(out, "sha256:"+tag)
	}
	return out
}

// filterValidDigests keeps well formed digests in order.
func filterValidDigests(tags []string) []string {
	clean := make([]string, 0, len(tags))
	for _, digest := range tags {
		if !transport.ValidBlobDigest(digest) {
			continue
		}
		clean = append(clean, digest)
	}
	return clean
}

// cleanDigests dedups digests preserving first seen order. Any malformed
// entry fails the whole call: digests come from the journal, and a bad
// one is a caller bug to fix, not to skip silently.
func cleanDigests(digests []string) ([]string, error) {
	seen := make(map[string]struct{})
	var out []string
	for _, digest := range digests {
		if !transport.ValidBlobDigest(digest) {
			return nil, fmt.Errorf("invalid blob digest %q", digest)
		}
		if _, ok := seen[digest]; ok {
			continue
		}
		seen[digest] = struct{}{}
		out = append(out, digest)
	}
	return out, nil
}

// rememberWanted adds roots to the wanted set the tick loop
// converges. The set only grows and lives in memory: Ensure
// re-registers on every call, so a restart resumes fetching as soon as
// the reconciler asks again. Registry-side garbage collection is
// operator owned, like track rotation.
func (b *BlobSyncer) rememberWanted(digests []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, digest := range digests {
		b.wanted[digest] = struct{}{}
	}
}

// snapshotWanted copies the wanted set for one tick so Ensure can grow
// the set concurrently without locking the fetch path.
func (b *BlobSyncer) snapshotWanted() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.wanted))
	for digest := range b.wanted {
		out = append(out, digest)
	}
	slices.Sort(out)
	return out
}

// tickBlobs runs one anti-entropy pass: wanted digests first, then
// peer-published tags, capped per tick. Failures log and retry next
// tick.
func (b *BlobSyncer) tickBlobs(ctx context.Context) {
	if b.store == nil {
		return
	}
	wanted := b.snapshotWanted()
	peers, err := b.collectBlobPeers()
	if err != nil {
		b.logger.Warn("blob peers unavailable", logs.Error(err))
		return
	}
	discovered := b.discoverRoots(ctx, peers)
	work := b.selectTickWork(wanted, discovered)
	if len(work) == 0 {
		return
	}
	b.bumpTicks()
	failed := b.fetchWantedRoots(ctx, work, peers)
	if len(failed) > 0 {
		b.logger.Warn("blob tick incomplete",
			logs.Int("missing", len(failed)),
			logs.Int("wanted", len(work)),
			logs.String("sample", missingSummary(failed)),
		)
		return
	}
	b.logger.Info("blob tick ok", logs.Int("roots", len(work)))
}

// tickOffset returns the rotating start offset for this tick.
func (b *BlobSyncer) tickOffset() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.ticks
}

// selectTickWork picks this tick's fetch list: all wanted digests
// first (caller-demanded, never starved), then discovered tags up to
// the per-tick cap with a rotating start for fairness. An oversized
// wanted set rotates on its own so every entry converges.
func (b *BlobSyncer) selectTickWork(wanted, discovered []string) []string {
	offset := b.tickOffset()
	if len(wanted) >= blobMaxRootsPerTick {
		return rotateStrings(wanted, offset)[:blobMaxRootsPerTick]
	}
	work := slices.Clone(wanted)
	rest := difference(discovered, wanted)
	rest = rotateStrings(rest, offset)
	work = append(work, rest[:min(len(rest), blobMaxRootsPerTick-len(work))]...)
	return work
}

// difference returns discovered entries not already wanted, in order.
func difference(discovered, wanted []string) []string {
	inWanted := make(map[string]struct{}, len(wanted))
	for _, digest := range wanted {
		inWanted[digest] = struct{}{}
	}
	var out []string
	for _, digest := range discovered {
		if _, ok := inWanted[digest]; ok {
			continue
		}
		out = append(out, digest)
	}
	return out
}

// bumpTicks advances the tick counter for rotation fairness.
func (b *BlobSyncer) bumpTicks() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.ticks++
}

// rotateStrings rotates the start offset so successive capped ticks
// cover different roots instead of reconverging on the first page.
func rotateStrings(in []string, n uint64) []string {
	if len(in) == 0 {
		return in
	}
	//nolint:gosec // modulo by len bounds the value below len, so it fits int
	shift := int(n % uint64(len(in)))
	if shift == 0 {
		return in
	}
	out := make([]string, 0, len(in))
	out = append(out, in[shift:]...)
	out = append(out, in[:shift]...)
	return out
}

// discoverRoots lists peer-published tags from both sync repos and
// returns the ones shaped like digests. Each repo holds one tag per
// replicated root, so the union across repos and alive peers is the
// fleet publish index. Blobs stay repo-independent on fetch, so one
// fetch path serves tags found in either repo.
func (b *BlobSyncer) discoverRoots(ctx context.Context, peers []blobPeer) []string {
	var out []string
	for _, peer := range peers {
		cerr := ctx.Err()
		if cerr != nil {
			return out
		}
		for _, repo := range []string{blobSyncRepo, blobFileRepo} {
			tags, ferr := b.client.FetchTags(ctx, peer.base, repo)
			if ferr != nil {
				b.logger.Debug("blob tag discovery failed",
					logs.String("peer_id", peer.id.String()),
					logs.String("repo", repo),
					logs.Error(ferr),
				)
				continue
			}
			for _, tag := range tags {
				out = append(out, "sha256:"+tag)
			}
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// blobPeer pairs a member ID with its registry base URL.
type blobPeer struct {
	id   uuid.UUID
	base string
}

// collectBlobPeers lists alive members and maps them to registry base
// URLs. The IP comes from member.Address.Addr with the registry port,
// never member.Address itself: that port is memberlist gossip, and
// dialing it was the prior anomaly this syncer replaces.
func (b *BlobSyncer) collectBlobPeers() ([]blobPeer, error) {
	if b.members == nil {
		return nil, fmt.Errorf("blob syncer has no member service") //nolint:perfsprint // plain error, no wrap target
	}
	members, err := b.members.Members()
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	port := blobRegistryPort()
	var peers []blobPeer
	for _, member := range members {
		if member.State != peerdiscovery.NodeStateAlive {
			continue
		}
		base := transport.PeerRegistryBase(member.Address.Addr(), port)
		if base == "" {
			continue
		}
		peers = append(peers, blobPeer{id: member.ID, base: base})
	}
	slices.SortFunc(peers, func(a, c blobPeer) int {
		return strings.Compare(a.id.String(), c.id.String())
	})
	return peers, nil
}

// blobRegistryPort returns the live registry port, falling back to the
// default on garbage. It reads registry.Port per call so tests that override
// the variable observe the override.
func blobRegistryPort() int {
	port := registry.Port
	if port <= 0 || port > 65535 {
		return blobPeerOrPortDefault
	}
	return port
}

// fetchWantedRoots fetches roots with a four worker pool. Workers start
// staggered, and each root tries peers in an order rotated by the job
// index so load spreads across peers instead of converging on the
// sorted first peer. It returns the roots still incomplete with their
// last errors.
func (b *BlobSyncer) fetchWantedRoots(ctx context.Context, wanted []string, peers []blobPeer) map[string]error {
	if len(wanted) == 0 || len(peers) == 0 {
		return b.missingWithoutPeers(ctx, wanted)
	}
	jobs := make(chan string)
	var wg sync.WaitGroup
	var mu sync.Mutex
	failed := make(map[string]error)
	workers := min(blobMaxConcurrent, len(wanted))
	for w := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			b.blobWorker(ctx, worker, peers, jobs, &mu, failed)
		}(w)
	}
	go func() {
		defer close(jobs)
		for _, root := range wanted {
			select {
			case <-ctx.Done():
				return
			case jobs <- root:
			}
		}
	}()
	wg.Wait()
	return failed
}

// missingWithoutPeers marks wanted roots incomplete when no peers can
// serve them. Local completeness still counts, so a node with the bytes
// reports no failure.
func (b *BlobSyncer) missingWithoutPeers(ctx context.Context, wanted []string) map[string]error {
	failed := make(map[string]error)
	for _, root := range wanted {
		complete, err := b.isComplete(ctx, root)
		if err != nil {
			b.logger.Warn("blob completeness check failed", logs.String("digest", root), logs.Error(err))
		}
		if !complete {
			failed[root] = fmt.Errorf("no peers available for %s", root)
		}
	}
	return failed
}

// blobWorker fetches queued roots, trying peers in an order rotated by
// the job index so load spreads across peers instead of converging on
// the sorted first peer. Start staggers by worker slot plus 0-200ms
// jitter so restarts do not burst in lockstep.
func (b *BlobSyncer) blobWorker(ctx context.Context, worker int, peers []blobPeer, jobs <-chan string, mu *sync.Mutex, failed map[string]error) {
	slept := false
	index := 0
	for root := range jobs {
		if err := ctx.Err(); err != nil {
			recordWorkerFailure(mu, failed, root, err)
			return
		}
		if !slept {
			if err := awaitWorkerStagger(ctx, worker); err != nil {
				recordWorkerFailure(mu, failed, root, err)
				return
			}
			slept = true
		}
		ordered := rotateBlobPeers(peers, index)
		index++
		if b.fetchOneRoot(ctx, root, ordered) {
			continue
		}
		b.markRootIncomplete(ctx, root, ordered, mu, failed)
	}
}

// awaitWorkerStagger delays one worker start by its slot plus jitter.
// A cancelled context aborts the delay with the context error.
func awaitWorkerStagger(ctx context.Context, worker int) error {
	base := time.Duration(worker) * blobWorkerStagger
	//nolint:gosec // start jitter only, not security randomness
	jitterNanos := rand.Int64N(int64(blobWorkerJitterMax))
	timer := time.NewTimer(base + time.Duration(jitterNanos))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

// recordWorkerFailure records a context cancellation for root.
func recordWorkerFailure(mu *sync.Mutex, failed map[string]error, root string, err error) {
	mu.Lock()
	defer mu.Unlock()
	failed[root] = fmt.Errorf("context cancelled: %w", err)
}

// markRootIncomplete checks completeness after a failed fetch and
// records roots still missing.
func (b *BlobSyncer) markRootIncomplete(ctx context.Context, root string, ordered []blobPeer, mu *sync.Mutex, failed map[string]error) {
	complete, err := b.isComplete(ctx, root)
	if err != nil {
		b.logger.Warn("blob completeness check failed", logs.String("digest", root), logs.Error(err))
	}
	if complete {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	if _, ok := failed[root]; !ok {
		failed[root] = fmt.Errorf("blob %s still missing after %d peers", root, len(ordered))
	}
}

// rotateBlobPeers rotates the peer order by n so successive roots start
// at different peers. Rotation is deterministic per tick, which spreads
// load without coordination or extra control traffic.
func rotateBlobPeers(peers []blobPeer, n int) []blobPeer {
	if len(peers) == 0 {
		return peers
	}
	shift := n % len(peers)
	if shift == 0 {
		return peers
	}
	out := make([]blobPeer, 0, len(peers))
	out = append(out, peers[shift:]...)
	out = append(out, peers[:shift]...)
	return out
}

// fetchOneRoot tries the local store first, then peers in order until
// root is complete. Per peer failures stay debug level because the next
// peer or tick retries them.
func (b *BlobSyncer) fetchOneRoot(ctx context.Context, root string, peers []blobPeer) bool {
	complete, err := b.isComplete(ctx, root)
	if err != nil {
		b.logger.Warn("blob completeness check failed", logs.String("digest", root), logs.Error(err))
	}
	if complete {
		b.clearRootCursors(peers, root)
		return true
	}
	for _, peer := range peers {
		if err := ctx.Err(); err != nil {
			return false
		}
		fetchErr := b.fetchRootFromPeer(ctx, peer, root)
		if fetchErr == nil {
			b.clearRootCursors(peers, root)
			return true
		}
		b.logger.Debug("blob fetch failed, trying next peer",
			logs.String("digest", root),
			logs.String("peer_id", peer.id.String()),
			logs.Error(fetchErr),
		)
	}
	complete, err = b.isComplete(ctx, root)
	if err != nil {
		b.logger.Warn("blob completeness check failed", logs.String("digest", root), logs.Error(err))
		return false
	}
	if complete {
		b.clearRootCursors(peers, root)
	}
	return complete
}

// clearRootCursors drops persisted offsets for root across peers once
// the root is complete. Stale offsets are harmless because HasRaw
// skips present chunks, but removal keeps the bucket bounded. Delete
// failure only logs at debug; the next complete tick retries it.
func (b *BlobSyncer) clearRootCursors(peers []blobPeer, root string) {
	store := b.cursorStore()
	for _, peer := range peers {
		key := blobCursorKey(peer.id, root)
		err := store.Delete(key)
		if err != nil {
			b.logger.Debug("blob cursor clear failed",
				logs.String("digest", root),
				logs.String("peer_id", peer.id.String()),
				logs.Error(err),
			)
		}
	}
}

// fetchRootFromPeer serves root from one peer, preferring the published
// manifest tag and falling back to the raw blob for untagged content.
// Both errors join when neither serves.
func (b *BlobSyncer) fetchRootFromPeer(ctx context.Context, peer blobPeer, root string) error {
	tag := strings.TrimPrefix(root, "sha256:")
	raw, dm, err := b.client.FetchManifestTag(ctx, peer.base, blobSyncRepo, tag)
	if err == nil {
		return b.fetchManifestRoot(ctx, peer, root, raw, dm)
	}
	manifestErr := err
	err = b.fetchRawDigest(ctx, peer, root)
	if err == nil {
		return nil
	}
	return fmt.Errorf("fetch root %s: %w", root, errors.Join(manifestErr, err))
}

// fetchManifestRoot fetches the published manifest's missing chunks,
// resuming from the persisted cursor. The manifest tag stores last, so
// its presence means the root is complete and later ticks skip the
// peer. Alias tags (manifest root differs from the tag) prove by
// content hash instead of root binding.
func (b *BlobSyncer) fetchManifestRoot(ctx context.Context, peer blobPeer, root string, raw []byte, dm string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}
	m, err := transport.ParseBlobManifest(raw)
	if err != nil {
		return fmt.Errorf("parse manifest: %w", err)
	}
	store := b.cursorStore()
	start, serr := store.load(peer.id, root)
	if serr != nil {
		b.logger.Warn("blob cursor load failed, restarting blob",
			logs.String("digest", root),
			logs.String("peer_id", peer.id.String()),
			logs.Error(serr),
		)
		start = 0
	}
	// Cursor offsets bind to one chunk list. Root equality implies an
	// identical chunk list outside a hash collision, so the saved offset
	// stays valid only when roots match. A different root or a saved
	// offset past the current chunk count resets to zero; HasRaw skips
	// chunks already stored. Equal length alone never reuses the offset.
	if m.Root != root {
		start = 0
	} else if start > uint64(len(m.Chunks)) {
		start = 0
	}
	ferr := b.fetchMissingChunks(ctx, peer, m.Chunks[start:], start, root)
	if ferr != nil {
		return ferr
	}
	if m.Root != root {
		verr := verifyBlobContent(ctx, b.store, m.Chunks, root)
		if verr != nil {
			return verr
		}
	}
	perr := b.store.PutRaw(ctx, dm, raw)
	if perr != nil {
		return fmt.Errorf("store manifest %s: %w", dm, perr)
	}
	terr := b.store.PutTag(blobSyncRepo, strings.TrimPrefix(root, "sha256:"), dm)
	if terr != nil {
		return fmt.Errorf("publish manifest tag %s: %w", root, terr)
	}
	return nil
}

// fetchMissingChunks stores every chunk from chunks that is not already
// local, advancing the persisted cursor past each stored chunk. The
// cursor only moves after the store accepts the bytes, so a crash
// resumes at the first uncertain chunk and idempotent restores fill
// exactly the gap. HasRaw skips are existence only for the already
// stored prefix; the caller still hash verifies alias content before
// publishing, and isComplete hash verifies all roots before reporting
// complete. Cursor access goes through cursorStore so Attach cannot
// race the pointer swap.
func (b *BlobSyncer) fetchMissingChunks(ctx context.Context, peer blobPeer, chunks []string, start uint64, root string) error {
	store := b.cursorStore()
	for i, chunk := range chunks {
		cerr := ctx.Err()
		if cerr != nil {
			return fmt.Errorf("context cancelled: %w", cerr)
		}
		if !b.store.HasRaw(chunk) {
			serr := b.storeOneChunk(ctx, peer, chunk)
			if serr != nil {
				return serr
			}
		}
		serr := store.save(peer.id, root, start+uint64(i)+1)
		if serr != nil {
			return serr
		}
	}
	return nil
}

// storeOneChunk fetches one chunk into a bounded buffer, verifies its
// digest in transit, and stores it. The buffer never exceeds one chunk
// plus one byte, so memory stays flat no matter the blob size.
func (b *BlobSyncer) storeOneChunk(ctx context.Context, peer blobPeer, chunk string) error {
	var buf bytes.Buffer
	_, err := b.client.FetchBlob(ctx, peer.base, blobSyncRepo, chunk, &buf, int64(blob.ChunkSize)+1)
	if err != nil {
		return fmt.Errorf("fetch chunk %s: %w", chunk, err)
	}
	err = b.store.PutRaw(ctx, chunk, buf.Bytes())
	if err != nil {
		return fmt.Errorf("store chunk %s: %w", chunk, err)
	}
	return nil
}

// fetchRawDigest converts untagged raw content into the chunked shape:
// stream the bytes once, split into fixed pieces, store each, then
// publish the alias manifest under the content tag plus the true root
// tag. Later ticks and peers use the resumable chunked path. Pieces
// already stored stay stored, so a retry only re-flows bytes, and the
// final content hash proves the whole before anything publishes. On
// error the stored prefix pieces remain for resume and stay idempotent;
// the manifest and tags publish last only on success, so a partial
// prefix never looks complete. No rollback is needed: leftover pieces
// are content addressed and resume is idempotent.
func (b *BlobSyncer) fetchRawDigest(ctx context.Context, peer blobPeer, root string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}
	sw := newSplitWriter(ctx, b.store)
	_, err := b.client.FetchBlob(ctx, peer.base, blobSyncRepo, root, sw, maxBlobRawBytes)
	if err != nil {
		return fmt.Errorf("fetch raw %s: %w", root, err)
	}
	chunks, total, err := sw.finish(ctx)
	if err != nil {
		return err
	}
	trueRoot, err := transport.BlobRoot(chunks)
	if err != nil {
		return fmt.Errorf("bind raw %s: %w", root, err)
	}
	m := transport.BlobManifest{
		Version:   1,
		ChunkSize: blob.ChunkSize,
		TotalSize: total,
		Chunks:    chunks,
		Root:      trueRoot,
	}
	raw, dm, err := transport.ManifestBytes(m)
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}
	if _, err := transport.ParseBlobManifest(raw); err != nil {
		return fmt.Errorf("validate manifest: %w", err)
	}
	serr := b.store.PutRaw(ctx, dm, raw)
	if serr != nil {
		return fmt.Errorf("store manifest %s: %w", dm, serr)
	}
	terr := b.publishTag(root, dm)
	if terr != nil {
		return terr
	}
	if trueRoot != root {
		terr = b.publishTag(trueRoot, dm)
		if terr != nil {
			return terr
		}
	}
	return nil
}

// publishTag records the manifest digest under the root hex tag in the
// local store, which both marks completion and publishes for peers.
func (b *BlobSyncer) publishTag(root, dm string) error {
	tag := strings.TrimPrefix(root, "sha256:")
	err := b.store.PutTag(blobSyncRepo, tag, dm)
	if err != nil {
		return fmt.Errorf("publish manifest tag %s: %w", root, err)
	}
	return nil
}

// verifyBlobContent proves alias tag content by hashing the stored
// chunks in order. Total cap is enforced before the loop; per chunk cap
// is enforced inside the loop before hashing. Each chunk loads bounded
// (one fixed chunk at a time) with a running hash, so memory stays flat
// and a multi gigabyte blob never sits in RAM. True roots use
// verifyTrueChunks instead: their binding is structural, not a content
// hash over bytes.
func verifyBlobContent(ctx context.Context, store blobIface, chunks []string, want string) error {
	if int64(len(chunks)) > maxBlobRawBytes/int64(blob.ChunkSize)+1 {
		return fmt.Errorf("blob verify lists %d chunks, over cap", len(chunks))
	}
	hasher := sha256.New()
	for _, chunk := range chunks {
		cerr := ctx.Err()
		if cerr != nil {
			return fmt.Errorf("context cancelled: %w", cerr)
		}
		buf, gerr := store.GetRaw(chunk)
		if gerr != nil {
			return fmt.Errorf("read chunk %s: %w", chunk, gerr)
		}
		if len(buf) > blob.ChunkSize {
			return fmt.Errorf("oversize chunk %s: %d", chunk, len(buf))
		}
		_, herr := hasher.Write(buf)
		if herr != nil {
			return fmt.Errorf("hash chunk %s: %w", chunk, herr)
		}
	}
	sum := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if sum != want {
		return fmt.Errorf("content digest mismatch for %s", want)
	}
	return nil
}

// verifyTrueChunks proves a true root by verifying every chunk hash.
// Total cap is enforced before the loop; per chunk cap is enforced
// inside the loop. GetRaw verifies each chunk digest, so a corrupt
// same size file fails here even though HasRaw passes. One chunk loads
// at a time, so memory stays flat.
func (b *BlobSyncer) verifyTrueChunks(ctx context.Context, chunks []string) error {
	if int64(len(chunks)) > maxBlobRawBytes/int64(blob.ChunkSize)+1 {
		return fmt.Errorf("blob verify lists %d chunks, over cap", len(chunks))
	}
	for _, chunk := range chunks {
		cerr := ctx.Err()
		if cerr != nil {
			return fmt.Errorf("context cancelled: %w", cerr)
		}
		buf, gerr := b.store.GetRaw(chunk)
		if gerr != nil {
			return fmt.Errorf("read chunk %s: %w", chunk, gerr)
		}
		if len(buf) > blob.ChunkSize {
			return fmt.Errorf("oversize chunk %s: %d", chunk, len(buf))
		}
	}
	return nil
}

// verifyTrueBlob proves a HasBlob hit by streaming verified bytes.
// OpenBlob checks every chunk hash before serving bytes, so a corrupt
// same size file fails here even though HasBlob passes. Copy to discard
// keeps memory flat.
func (b *BlobSyncer) verifyTrueBlob(ctx context.Context, digest string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}
	rc, _, oerr := b.store.OpenBlob(digest)
	if oerr != nil {
		return fmt.Errorf("open blob %s: %w", digest, oerr)
	}
	_, cerr := io.Copy(io.Discard, rc)
	clerr := rc.Close()
	if cerr != nil {
		return fmt.Errorf("verify blob %s: %w", digest, cerr)
	}
	if clerr != nil {
		return fmt.Errorf("close verified blob %s: %w", digest, clerr)
	}
	return nil
}

// splitWriter splits one verified stream into fixed pieces and stores
// each as it fills. Pieces verify individually by content digest on
// store, and the transport layer proves the whole stream digest, so
// the split needs no second pass.
type splitWriter struct {
	getCtx func() context.Context
	store  blobIface
	buf    []byte
	chunks []string
	total  int64
	failed error
}

// newSplitWriter creates a splitter writing pieces to store.
func newSplitWriter(ctx context.Context, store blobIface) *splitWriter {
	return &splitWriter{getCtx: func() context.Context { return ctx }, store: store}
}

// Write buffers p and stores every filled piece. Store errors stick:
// later writes and finish report the first failure.
func (w *splitWriter) Write(p []byte) (int, error) {
	if w.failed != nil {
		return 0, w.failed
	}
	ctx := w.getCtx()
	if err := ctx.Err(); err != nil {
		w.failed = fmt.Errorf("context cancelled: %w", err)
		return 0, w.failed
	}
	w.buf = append(w.buf, p...)
	for len(w.buf) >= blob.ChunkSize {
		if err := w.flushPiece(ctx, w.buf[:blob.ChunkSize]); err != nil {
			w.failed = err
			return 0, err
		}
		w.buf = append([]byte(nil), w.buf[blob.ChunkSize:]...)
	}
	return len(p), nil
}

// flushPiece stores one full piece under its content digest.
func (w *splitWriter) flushPiece(ctx context.Context, piece []byte) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled: %w", err)
	}
	sum := sha256.Sum256(piece)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	err := w.store.PutRaw(ctx, digest, piece)
	if err != nil {
		return fmt.Errorf("store piece %s: %w", digest, err)
	}
	w.chunks = append(w.chunks, digest)
	w.total += int64(len(piece))
	return nil
}

// finish stores the final partial piece and reports the chunk list and
// total size. Empty input yields no chunks and total zero, which the
// manifest validation accepts as the empty blob.
func (w *splitWriter) finish(ctx context.Context) ([]string, int64, error) {
	if w.failed != nil {
		return nil, 0, w.failed
	}
	if len(w.buf) > 0 {
		if err := w.flushPiece(ctx, w.buf); err != nil {
			w.failed = err
			return nil, 0, err
		}
		w.buf = nil
	}
	return w.chunks, w.total, nil
}

// isComplete reports whether digest needs no fetch. HasBlob and HasRaw
// are existence only, so each hit hash verifies before returning true:
// OpenBlob streams true blobs and GetRaw verifies raw bytes. A corrupt
// same size file therefore reports false plus an error instead of true.
// A tag resolves to a manifest whose chunks must all be present; alias
// tags hash verify content while true roots verify each chunk hash.
// An oversize stored manifest reports an error so the caller surfaces
// it instead of retrying as incomplete; tick callers log warn once per
// tick and retry next tick.
func (b *BlobSyncer) isComplete(ctx context.Context, digest string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("context cancelled: %w", err)
	}
	if b.store.HasBlob(digest) {
		if err := b.verifyTrueBlob(ctx, digest); err != nil {
			return false, err
		}
		return true, nil
	}
	if b.store.HasRaw(digest) {
		if _, err := b.store.GetRaw(digest); err != nil {
			return false, fmt.Errorf("verify raw %s: %w", digest, err)
		}
		return true, nil
	}
	dm, ok := b.store.ResolveTag(blobSyncRepo, strings.TrimPrefix(digest, "sha256:"))
	if !ok {
		return false, nil
	}
	return b.isTagComplete(ctx, digest, dm)
}

// isTagComplete verifies a tag resolved manifest for digest.
func (b *BlobSyncer) isTagComplete(ctx context.Context, digest, dm string) (bool, error) {
	m, ok, err := b.loadTagManifest(dm)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	if !b.haveAllChunks(m.Chunks) {
		return false, nil
	}
	return b.verifyTagContent(ctx, digest, m)
}

// loadTagManifest reads and validates the stored manifest dm. It
// reports ok false when the manifest is missing or unparsable, which
// the caller treats as incomplete without an error.
func (b *BlobSyncer) loadTagManifest(dm string) (BlobManifest, bool, error) {
	raw, err := b.store.GetRaw(dm)
	if err != nil {
		return BlobManifest{}, false, fmt.Errorf("read manifest %s: %w", dm, err)
	}
	if len(raw) > maxManifestStored {
		return BlobManifest{}, false, fmt.Errorf("blob manifest %s oversize: %d bytes", dm, len(raw))
	}
	m, err := transport.ParseBlobManifest(raw)
	if err != nil {
		return BlobManifest{}, false, nil //nolint:nilerr // corrupt manifest means incomplete, not an error
	}
	return m, true, nil
}

// haveAllChunks reports whether every chunk is present locally.
func (b *BlobSyncer) haveAllChunks(chunks []string) bool {
	for _, chunk := range chunks {
		if !b.store.HasRaw(chunk) {
			return false
		}
	}
	return true
}

// verifyTagContent hash verifies alias tags by content and true roots
// by chunk hash.
func (b *BlobSyncer) verifyTagContent(ctx context.Context, digest string, m BlobManifest) (bool, error) {
	if m.Root != digest {
		if err := verifyBlobContent(ctx, b.store, m.Chunks, digest); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := b.verifyTrueChunks(ctx, m.Chunks); err != nil {
		return false, err
	}
	return true, nil
}

// missingSummary names up to three missing digests for log and error
// lines. Full lists would grow with the wanted set; three identify the
// stall while staying bounded.
func missingSummary(failed map[string]error) string {
	keys := make([]string, 0, len(failed))
	for root := range failed {
		keys = append(keys, root)
	}
	slices.Sort(keys)
	if len(keys) > 3 {
		keys = keys[:3]
	}
	return strings.Join(keys, ", ")
}
