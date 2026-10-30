// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package registry runs a blob-backed OCI distribution registry on loopback.
//
// Each Concord node serves container image layers and file artifacts
// from localhost so workloads pull without external network access.
// Content is content addressed by sha256 digest in a blob store under
// ~/.config/concord/zot/blobs (historic name kept for data compat).
// Peer sync lives in peersync; this package only serves the local HTTP
// surface.
// Wire stays monolithic OCI for compat: HEAD and GET blobs, POST
// uploads with digest query, PUT and GET manifests, GET tags list.
// Clients use 127.0.0.1 loopback. Start binds 0.0.0.0 on Port so peers
// sync over the mesh on the same port; the bind is intentional.
// Status codes follow distribution conventions: 200 on hit, 201 on
// upload or manifest put, 400 on bad digest or tag, 404 on unknown blob
// or manifest, 405 on resumed upload or bad method, 413 over size caps,
// 500 on store failure.
package registry

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/podomy/concord/internal/blob"
	"github.com/podomy/concord/internal/peerdiscovery"
)

// Port is the TCP port the embedded registry listens on.
// It is a variable so tests and deployments can override it.
var Port = 8444

// maxManifestBytes caps manifest PUT and GET bodies.
const maxManifestBytes = 10 << 20

// maxBlobBytes caps one blob upload or raw blob fetch. It matches
// transport maxBlobFetchBytes and peersync maxBlobRawBytes so no
// single blob can exhaust disk. Chunked blobs stream via OpenBlob
// and never sit fully in RAM.
const maxBlobBytes = 32 << 30

// Registry serves the OCI distribution subset over plain HTTP.
type Registry struct {
	srv      *http.Server
	store    *blob.Store
	logger   *zap.Logger
	nodeID   uuid.UUID
	storeDir string
	stopOnce sync.Once
}

// rootDirPath returns the on-disk storage directory for the local registry.
func rootDirPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}
	return filepath.Join(dir, "concord", "zot"), nil
}

// New creates a new Registry backed by a blob store under the shared
// data directory. It does not start serving.
func New(nodeID uuid.UUID) (*Registry, error) {
	root, err := rootDirPath()
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(root, 0o700)
	if err != nil {
		return nil, fmt.Errorf("create registry directory: %w", err)
	}
	storeDir := filepath.Join(root, "blobs")
	store, err := blob.Open(storeDir)
	if err != nil {
		return nil, fmt.Errorf("open blob store: %w", err)
	}
	return &Registry{
		store:    store,
		nodeID:   nodeID,
		storeDir: storeDir,
	}, nil
}

// Store exposes the underlying content store for peer sync.
func (r *Registry) Store() *blob.Store {
	return r.store
}

// Start binds all interfaces and serves in the background. Loopback
// clients use 127.0.0.1 and peer sync dials this same port from other
// nodes, so the 0.0.0.0 bind is intentional. When ctx is cancelled the
// server shuts down.
func (r *Registry) Start(ctx context.Context, memberService *peerdiscovery.MemberService, logger *zap.Logger) error {
	if logger == nil {
		logger = zap.NewNop()
	}
	r.logger = logger
	mux := http.NewServeMux()
	mux.HandleFunc("/v2", r.serveV2)
	mux.HandleFunc("/v2/", r.serveV2)
	// ReadHeaderTimeout bounds header reads. ReadTimeout is zero so
	// large blob uploads are not cut mid body. The request context
	// bounds the transfer instead. WriteTimeout is unset so GB blob
	// downloads over slow links are not cut. IdleTimeout reaps idle
	// keep-alives.
	r.srv = &http.Server{
		Addr:              "0.0.0.0:" + strconv.Itoa(Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       0,
		WriteTimeout:      0,
		IdleTimeout:       120 * time.Second,
	}
	lc := &net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", r.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen registry: %w", err)
	}
	go func() {
		err := r.srv.Serve(ln)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("registry serve", zap.Error(err))
		}
	}()
	go r.monitorPeers(ctx, memberService, logger)
	go func() {
		<-ctx.Done()
		r.shutdown(ctx)
	}()
	return nil
}

// shutdown stops the server with a timeout derived from ctx.
// Caller ctx may already be cancelled, so WithoutCancel isolates the timeout.
func (r *Registry) shutdown(ctx context.Context) {
	r.stopOnce.Do(func() {
		if r.srv == nil {
			return
		}
		timeoutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		err := r.srv.Shutdown(timeoutCtx)
		if err != nil {
			_ = r.srv.Close() //nolint:errcheck // fallback close on shutdown error
		}
	})
}

// Stop shuts down the registry server. Safe to call multiple times.
// Stop has no caller ctx, so it uses Background with a timeout.
// Start passes its ctx to shutdown so cancellation values propagate.
// Background is intentional here because Stop is called from defer
// and from a goroutine where the request ctx is already done, and
// Shutdown requires a live ctx with timeout.
func (r *Registry) Stop() {
	r.shutdown(context.Background())
}

// reloadSyncConfig notes the current peer set. Blob sync itself lives
// in peersync; the registry only records membership for its logs.
func (r *Registry) reloadSyncConfig(nodes []peerdiscovery.Node) {
	peers := 0
	for _, n := range nodes {
		if n.ID == r.nodeID {
			continue
		}
		peers++
	}
	if r.logger == nil {
		return
	}
	r.logger.Debug("registry peers", zap.Int("peers", peers))
}

// monitorPeers polls the member service and notes membership changes.
func (r *Registry) monitorPeers(ctx context.Context, memberService *peerdiscovery.MemberService, logger *zap.Logger) {
	if memberService == nil {
		return
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	var previous []peerdiscovery.Node
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		current, err := memberService.Members()
		if err != nil {
			logger.Error("member service", zap.Error(err))
			continue
		}
		if !nodesEqual(previous, current) {
			logger.Info("peer list changed", zap.Int("peers", len(current)))
			r.reloadSyncConfig(current)
			previous = current
		}
	}
}

// nodesEqual compares two node slices for equality (order-independent).
func nodesEqual(a, b []peerdiscovery.Node) bool {
	if len(a) != len(b) {
		return false
	}
	seen := nodeIDSet(a)
	for _, n := range b {
		if _, ok := seen[n.ID]; !ok {
			return false
		}
	}
	return true
}

// nodeIDSet builds a set of node IDs for membership comparison.
func nodeIDSet(nodes []peerdiscovery.Node) map[uuid.UUID]struct{} {
	m := make(map[uuid.UUID]struct{}, len(nodes))
	for _, n := range nodes {
		m[n.ID] = struct{}{}
	}
	return m
}

// ociError is one entry in the distribution error envelope.
type ociError struct {
	Code    string `json:"code"`
	Message string `json:"message,omitempty"`
}

// writeError renders a distribution style error envelope.
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, err := json.Marshal(map[string][]ociError{"errors": {{Code: code, Message: message}}}) //nolint:errchkjson // checked for symmetry with fallible encoders
	if err != nil {
		_, _ = w.Write([]byte(`{"errors":[{"code":"UNKNOWN"}]}`)) //nolint:errcheck // fallback error body
		return
	}
	_, _ = w.Write(body) //nolint:errcheck // best effort error body
}

// isLowerAlnum reports whether c is a-z or 0-9.
func isLowerAlnum(c byte) bool {
	if c >= 'a' && c <= 'z' {
		return true
	}
	return c >= '0' && c <= '9'
}

// isRepoChar reports whether c is allowed inside a repo segment.
func isRepoChar(c byte) bool {
	if isLowerAlnum(c) {
		return true
	}
	return c == '.' || c == '_' || c == '-'
}

// isTagChar reports whether c is allowed in a tag.
func isTagChar(c byte) bool {
	if c >= 'a' && c <= 'z' {
		return true
	}
	if c >= 'A' && c <= 'Z' {
		return true
	}
	if c >= '0' && c <= '9' {
		return true
	}
	return c == '_' || c == '.' || c == '-'
}

// isHexDigit reports whether c is hex in any case.
func isHexDigit(c byte) bool {
	if c >= '0' && c <= '9' {
		return true
	}
	if c >= 'a' && c <= 'f' {
		return true
	}
	return c >= 'A' && c <= 'F'
}

// isLowerHexDigit reports whether c is lower case hex.
func isLowerHexDigit(c byte) bool {
	if c >= '0' && c <= '9' {
		return true
	}
	return c >= 'a' && c <= 'f'
}

// isHexString reports whether s is all hex digits in any case.
func isHexString(s string) bool {
	for i := range len(s) {
		if !isHexDigit(s[i]) {
			return false
		}
	}
	return true
}

// isLowerHexString reports whether s is all lower case hex digits.
func isLowerHexString(s string) bool {
	for i := range len(s) {
		if !isLowerHexDigit(s[i]) {
			return false
		}
	}
	return true
}

// hasValidRepoChars reports whether s uses only repo chars.
func hasValidRepoChars(s string) bool {
	for i := range len(s) {
		if !isRepoChar(s[i]) {
			return false
		}
	}
	return true
}

// hasValidTagChars reports whether s uses only tag chars.
func hasValidTagChars(s string) bool {
	for i := range len(s) {
		if !isTagChar(s[i]) {
			return false
		}
	}
	return true
}

// parseDigest normalizes s to lowercase sha256 hex form. Uppercase hex
// is accepted then lowercased, then strictly validated so the store
// key is always canonical lowercase as blob.Store requires.
func parseDigest(s string) (string, bool) {
	hexPart, ok := strings.CutPrefix(s, "sha256:")
	if !ok {
		return "", false
	}
	if len(hexPart) != 64 {
		return "", false
	}
	if !isHexString(hexPart) {
		return "", false
	}
	return "sha256:" + strings.ToLower(hexPart), true
}

// validRepoSegment reports whether s is one path segment of a repo name.
func validRepoSegment(s string) bool {
	if s == "" {
		return false
	}
	if len(s) > 128 {
		return false
	}
	if !isLowerAlnum(s[0]) {
		return false
	}
	return hasValidRepoChars(s)
}

// validRepo reports whether repo is a safe repository path.
func validRepo(repo string) bool {
	if repo == "" {
		return false
	}
	if len(repo) > 255 {
		return false
	}
	if repo != path.Clean(repo) {
		return false
	}
	if strings.Contains(repo, "..") {
		return false
	}
	for part := range strings.SplitSeq(repo, "/") {
		if !validRepoSegment(part) {
			return false
		}
	}
	return true
}

// validTag reports whether tag is a safe OCI tag.
func validTag(tag string) bool {
	if tag == "" {
		return false
	}
	if len(tag) > 128 {
		return false
	}
	return hasValidTagChars(tag)
}

// isHexTag reports whether s is 64 hex digits in any case.
func isHexTag(s string) bool {
	if len(s) != 64 {
		return false
	}
	return isHexString(s)
}

// splitRef sorts a manifest reference into digest or tag form. Hex tags
// normalize to lowercase so sync hex tags match peersync cleanDigests.
func splitRef(ref string) (string, string, bool, bool) {
	if strings.Contains(ref, ":") {
		digest, ok := parseDigest(ref)
		if !ok {
			return "", "", true, false
		}
		return digest, "", true, true
	}
	if !validTag(ref) {
		return "", "", false, false
	}
	if isHexTag(ref) {
		return "", strings.ToLower(ref), false, true
	}
	return "", ref, false, true
}

// manifestContentType picks the response media type for stored bytes.
func manifestContentType(raw []byte) string {
	var doc struct {
		MediaType string           `json:"mediaType"`
		Manifests []map[string]any `json:"manifests,omitempty"`
	}
	err := json.Unmarshal(raw, &doc)
	if err != nil {
		return "application/vnd.oci.image.manifest.v1+json"
	}
	if doc.MediaType != "" {
		return doc.MediaType
	}
	if len(doc.Manifests) > 0 {
		return "application/vnd.oci.image.index.v1+json"
	}
	return "application/vnd.oci.image.manifest.v1+json"
}

// cutUploadsRepo strips a trailing blobs uploads suffix.
func cutUploadsRepo(rest string) (string, bool) {
	if repo, ok := strings.CutSuffix(rest, "/blobs/uploads"); ok {
		return repo, true
	}
	if repo, ok := strings.CutSuffix(rest, "/blobs/uploads/"); ok {
		return repo, true
	}
	return "", false
}

// cutTagsRepo strips a trailing tags list suffix.
func cutTagsRepo(rest string) (string, bool) {
	if repo, ok := strings.CutSuffix(rest, "/tags/list"); ok {
		return repo, true
	}
	return "", false
}

// isResumedUpload reports whether rest targets a resumed upload chunk.
func isResumedUpload(rest string) bool {
	idx := strings.LastIndex(rest, "/blobs/uploads/")
	if idx < 0 {
		return false
	}
	tail := rest[idx+len("/blobs/uploads/"):]
	return tail != ""
}

// serveV2 routes one registry request to its handler. The repo may
// contain slashes and even segments like blobs or manifests, so the
// route marker closest to the end wins. Uploads and tags match by
// suffix; blobs and manifests match by the rightmost marker with a
// slash free tail, falling back to the other marker when the
// rightmost tail is not slash free.
func (r *Registry) serveV2(w http.ResponseWriter, req *http.Request) {
	p := req.URL.Path
	if p == "/v2" || p == "/v2/" {
		r.servePing(w, req)
		return
	}
	rest, ok := strings.CutPrefix(p, "/v2/")
	if !ok {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unknown route")
		return
	}
	if rest == "" {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unknown route")
		return
	}
	if r.serveV2Special(w, req, rest) {
		return
	}
	r.serveV2BlobOrManifest(w, req, rest)
}

// serveV2Special handles uploads, resumed uploads, and tags list.
// It reports whether the request was handled.
func (r *Registry) serveV2Special(w http.ResponseWriter, req *http.Request, rest string) bool {
	if repo, ok := cutUploadsRepo(rest); ok {
		r.serveBlobUpload(w, req, repo)
		return true
	}
	if isResumedUpload(rest) {
		writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "resumed uploads not supported, use monolithic POST with digest")
		return true
	}
	if repo, ok := cutTagsRepo(rest); ok {
		r.serveTagsList(w, req, repo)
		return true
	}
	return false
}

// serveV2BlobOrManifest routes blob and manifest paths by rightmost marker.
func (r *Registry) serveV2BlobOrManifest(w http.ResponseWriter, req *http.Request, rest string) {
	idxBlobs := strings.LastIndex(rest, "/blobs/")
	idxManifests := strings.LastIndex(rest, "/manifests/")
	if idxBlobs < 0 && idxManifests < 0 {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unknown route")
		return
	}
	if idxManifests > idxBlobs {
		r.serveV2ManifestFirst(w, req, rest, idxBlobs, idxManifests)
		return
	}
	r.serveV2BlobFirst(w, req, rest, idxBlobs, idxManifests)
}

// serveV2ManifestFirst handles routes where the manifest marker is rightmost.
func (r *Registry) serveV2ManifestFirst(w http.ResponseWriter, req *http.Request, rest string, idxBlobs, idxManifests int) {
	repoM := rest[:idxManifests]
	refM := rest[idxManifests+len("/manifests/"):]
	if isSlashFreeRef(refM) {
		r.serveManifest(w, req, repoM, refM)
		return
	}
	if idxBlobs < 0 {
		r.serveManifest(w, req, repoM, refM)
		return
	}
	repoB, remB := splitBlobRest(rest, idxBlobs)
	if isSlashFreeRef(remB) {
		r.serveBlob(w, req, repoB, remB)
		return
	}
	r.serveManifest(w, req, repoM, refM)
}

// serveV2BlobFirst handles routes where the blob marker is rightmost.
func (r *Registry) serveV2BlobFirst(w http.ResponseWriter, req *http.Request, rest string, idxBlobs, idxManifests int) {
	repoB, remB := splitBlobRest(rest, idxBlobs)
	if isSlashFreeRef(remB) {
		r.serveBlob(w, req, repoB, remB)
		return
	}
	if idxManifests < 0 {
		r.serveBlob(w, req, repoB, remB)
		return
	}
	repoM := rest[:idxManifests]
	refM := rest[idxManifests+len("/manifests/"):]
	if isSlashFreeRef(refM) {
		r.serveManifest(w, req, repoM, refM)
		return
	}
	r.serveBlob(w, req, repoB, remB)
}

// isSlashFreeRef reports whether ref is a non-empty single path segment.
func isSlashFreeRef(ref string) bool {
	if ref == "" {
		return false
	}
	return !strings.Contains(ref, "/")
}

// splitBlobRest splits rest at the blob marker index.
func splitBlobRest(rest string, idxBlobs int) (string, string) {
	repo := rest[:idxBlobs]
	rem := rest[idxBlobs+len("/blobs/"):]
	return repo, rem
}

// servePing answers the registry version check.
func (r *Registry) servePing(w http.ResponseWriter, req *http.Request) {
	defer req.Body.Close() //nolint:errcheck // close request body, no content needed
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}
	w.Header().Set("Docker-Distribution-API-Version", "registry/2.0")
	w.WriteHeader(http.StatusOK)
	if req.Method == http.MethodGet {
		_, _ = w.Write([]byte("{}")) //nolint:errcheck // best effort version body
	}
}

// checkUploadRequest validates method, repo, digest, and size header.
func checkUploadRequest(w http.ResponseWriter, req *http.Request, repo string) (string, bool) {
	if req.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "resumed uploads not supported, use monolithic POST with digest")
		return "", false
	}
	if !validRepo(repo) {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unknown repository")
		return "", false
	}
	digest, ok := parseDigest(req.URL.Query().Get("digest"))
	if !ok {
		writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "missing or invalid digest")
		return "", false
	}
	if req.ContentLength > maxBlobBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "BLOB_UPLOAD_INVALID", "blob too large")
		return "", false
	}
	return digest, true
}

// createUploadTemp creates a temp file for one upload in dir.
func createUploadTemp(w http.ResponseWriter, dir string) (*os.File, string, bool) {
	tmp, err := os.CreateTemp(dir, ".tmp-upload-*")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "temp file")
		return nil, "", false
	}
	return tmp, tmp.Name(), true
}

// copyUploadBody streams the request body to tmp and verifies the digest.
// It closes tmp on all paths and reports whether the body was accepted.
func copyUploadBody(w http.ResponseWriter, req *http.Request, tmp *os.File, digest string) bool {
	sum := sha256.New()
	n, err := io.Copy(tmp, io.TeeReader(io.LimitReader(req.Body, maxBlobBytes+1), sum))
	if err != nil {
		_ = tmp.Close() //nolint:errcheck // close error folded into read error
		writeError(w, http.StatusBadRequest, "BLOB_UPLOAD_INVALID", "read upload body")
		return false
	}
	if n > maxBlobBytes {
		_ = tmp.Close() //nolint:errcheck // close before cleanup
		writeError(w, http.StatusRequestEntityTooLarge, "BLOB_UPLOAD_INVALID", "blob too large")
		return false
	}
	computed := "sha256:" + hex.EncodeToString(sum.Sum(nil))
	if computed != digest {
		_ = tmp.Close() //nolint:errcheck // close before cleanup
		writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "digest mismatch")
		return false
	}
	err = tmp.Sync()
	if err != nil {
		_ = tmp.Close() //nolint:errcheck // close before cleanup
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "sync temp file")
		return false
	}
	err = tmp.Close()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "close temp file")
		return false
	}
	err = req.Context().Err()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "context cancelled")
		return false
	}
	return true
}

// storeSmallUpload stores a small temp file via the capped store call.
func (r *Registry) storeSmallUpload(w http.ResponseWriter, ctx context.Context, digest, tmpName string) bool {
	data, err := os.ReadFile(tmpName) //nolint:gosec // tmp file created by this upload in store dir
	if err != nil {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "read temp file")
		return false
	}
	err = r.store.PutRaw(ctx, digest, data)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "store blob")
		return false
	}
	return true
}

// storeLargeUpload installs a large temp file by rename.
func (r *Registry) storeLargeUpload(w http.ResponseWriter, digest, tmpName string) bool {
	err := r.installRaw(tmpName, digest)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "store blob")
		return false
	}
	return true
}

// commitUpload stats tmpName and stores it, then answers 201.
func (r *Registry) commitUpload(w http.ResponseWriter, req *http.Request, repo, digest, tmpName string) {
	info, err := os.Stat(tmpName)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "stat temp file")
		return
	}
	if info.Size() > maxBlobBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "BLOB_UPLOAD_INVALID", "blob too large")
		return
	}
	if info.Size() <= maxManifestBytes {
		if !r.storeSmallUpload(w, req.Context(), digest, tmpName) {
			return
		}
	} else {
		if !r.storeLargeUpload(w, digest, tmpName) {
			return
		}
	}
	w.Header().Set("Location", "/v2/"+repo+"/blobs/"+digest)
	w.Header().Set("Docker-Content-Digest", digest)
	w.WriteHeader(http.StatusCreated)
}

// serveBlobUpload stores one monolithic blob upload addressed by digest.
// The body streams to a temp file inside the store dir with flat memory.
// Small blobs use PutRaw. Large blobs install via rename to avoid RAM.
// The store has no streaming ingest, so the large path writes the raw
// file layout directly. Resumed uploads are not supported.
func (r *Registry) serveBlobUpload(w http.ResponseWriter, req *http.Request, repo string) {
	defer req.Body.Close() //nolint:errcheck // close upload body after read
	digest, ok := checkUploadRequest(w, req, repo)
	if !ok {
		return
	}
	dir := r.storeDirFor()
	if dir == "" {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "store dir")
		return
	}
	err := os.MkdirAll(dir, 0o700)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "BLOB_UPLOAD_INVALID", "temp dir")
		return
	}
	tmp, tmpName, ok := createUploadTemp(w, dir)
	if !ok {
		return
	}
	defer func() {
		_ = os.Remove(tmpName) //nolint:errcheck // best effort temp cleanup
	}()
	if !copyUploadBody(w, req, tmp, digest) {
		return
	}
	r.commitUpload(w, req, repo, digest, tmpName)
}

// checkBlobRequest validates method, repo, and digest for blob fetch.
func checkBlobRequest(w http.ResponseWriter, req *http.Request, repo, remainder string) (string, bool) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return "", false
	}
	if !validRepo(repo) {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unknown repository")
		return "", false
	}
	if strings.Contains(remainder, "/") {
		writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "invalid digest")
		return "", false
	}
	digest, ok := parseDigest(remainder)
	if !ok {
		writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "invalid digest")
		return "", false
	}
	return digest, true
}

// serveBlob answers blob HEAD and GET by digest. Chunked roots stream
// via OpenBlob with flat memory. Raw blobs stream from the raw file
// with flat memory. Only blobs over maxBlobBytes return 413.
func (r *Registry) serveBlob(w http.ResponseWriter, req *http.Request, repo, remainder string) {
	defer req.Body.Close() //nolint:errcheck // close request body, GET has none
	digest, ok := checkBlobRequest(w, req, repo, remainder)
	if !ok {
		return
	}
	if req.Method == http.MethodHead {
		r.serveBlobHead(w, digest)
		return
	}
	rc, size, err := r.store.OpenBlob(digest)
	if err == nil {
		r.writeChunkedBlob(w, rc, digest, size)
		return
	}
	r.serveRawGet(w, digest)
}

// writeChunkedBlob streams a chunked blob with headers.
func (r *Registry) writeChunkedBlob(w http.ResponseWriter, rc io.ReadCloser, digest string, size int64) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc) //nolint:errcheck // best effort blob body
	_ = rc.Close()        //nolint:errcheck // close chunk reader
}

// serveBlobHead answers blob HEAD with the same headers as GET.
func (r *Registry) serveBlobHead(w http.ResponseWriter, digest string) {
	if r.store.HasBlob(digest) {
		r.serveChunkHead(w, digest)
		return
	}
	r.serveRawHead(w, digest)
}

// serveChunkHead answers HEAD for a chunked blob.
func (r *Registry) serveChunkHead(w http.ResponseWriter, digest string) {
	size, ok := r.store.GetBlobSize(digest)
	if !ok {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return
	}
	writeBlobHeaders(w, digest, size)
}

// serveRawHead answers HEAD for a raw blob file.
func (r *Registry) serveRawHead(w http.ResponseWriter, digest string) {
	if !r.store.HasRaw(digest) {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return
	}
	dest, ok := rawBlobPath(r.storeDirFor(), digest)
	if !ok {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return
	}
	size, ok := statBlobFile(dest)
	if !ok {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return
	}
	if size > maxBlobBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "BLOB_UNKNOWN", "blob too large")
		return
	}
	writeBlobHeaders(w, digest, size)
}

// writeBlobHeaders writes HEAD headers for a blob of size.
func writeBlobHeaders(w http.ResponseWriter, digest string, size int64) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
}

// statBlobFile stats a blob file and reports size.
func statBlobFile(dest string) (int64, bool) {
	info, err := os.Stat(dest)
	if err != nil {
		return 0, false
	}
	return info.Size(), true
}

// checkRawReady validates raw blob presence and size.
func (r *Registry) checkRawReady(w http.ResponseWriter, digest string) (string, int64, bool) {
	if !r.store.HasRaw(digest) {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return "", 0, false
	}
	dest, ok := rawBlobPath(r.storeDirFor(), digest)
	if !ok {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return "", 0, false
	}
	size, ok := statBlobFile(dest)
	if !ok {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return "", 0, false
	}
	if size > maxBlobBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "BLOB_UNKNOWN", "blob too large")
		return "", 0, false
	}
	return dest, size, true
}

// serveRawGet streams a raw blob file with flat memory. Small files use
// the capped store read. Large files verify by streaming hash then
// stream to the client. Only sizes over maxBlobBytes return 413.
func (r *Registry) serveRawGet(w http.ResponseWriter, digest string) {
	dest, size, ok := r.checkRawReady(w, digest)
	if !ok {
		return
	}
	if size <= maxManifestBytes {
		r.serveRawSmall(w, digest)
		return
	}
	r.serveRawLarge(w, digest, dest, size)
}

// serveRawSmall serves a small raw blob from the capped store.
func (r *Registry) serveRawSmall(w http.ResponseWriter, digest string) {
	data, err := r.store.GetRawCapped(digest, maxManifestBytes+1)
	if err != nil {
		if strings.Contains(err.Error(), "too large") {
			writeError(w, http.StatusRequestEntityTooLarge, "BLOB_UNKNOWN", "blob too large")
			return
		}
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(data) //nolint:errcheck // best effort blob body
}

// serveRawLarge verifies then streams a large raw blob file.
func (r *Registry) serveRawLarge(w http.ResponseWriter, digest, dest string, size int64) {
	err := verifyRawFile(dest, digest)
	if err != nil {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return
	}
	f, err := os.Open(dest) //nolint:gosec // dest is a validated digest path under the store dir
	if err != nil {
		writeError(w, http.StatusNotFound, "BLOB_UNKNOWN", "unknown blob")
		return
	}
	defer func() {
		_ = f.Close() //nolint:errcheck // close raw file after serve
	}()
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, f) //nolint:errcheck // best effort blob body
}

// checkManifestRequest validates method, repo, and ref for manifests.
func checkManifestRequest(w http.ResponseWriter, req *http.Request, repo, ref string) (string, string, bool, bool) {
	if req.Method != http.MethodGet && req.Method != http.MethodHead && req.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return "", "", false, false
	}
	if !validRepo(repo) {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unknown repository")
		return "", "", false, false
	}
	if ref == "" || strings.Contains(ref, "/") {
		writeError(w, http.StatusBadRequest, "TAG_INVALID", "invalid reference")
		return "", "", false, false
	}
	digest, tag, isDigest, ok := splitRef(ref)
	if !ok {
		writeError(w, http.StatusBadRequest, "TAG_INVALID", "invalid reference")
		return "", "", false, false
	}
	return digest, tag, isDigest, true
}

// serveManifest answers manifest GET, HEAD, and PUT by tag or digest.
func (r *Registry) serveManifest(w http.ResponseWriter, req *http.Request, repo, ref string) {
	defer req.Body.Close() //nolint:errcheck // close request body for GET and HEAD
	digest, tag, isDigest, ok := checkManifestRequest(w, req, repo, ref)
	if !ok {
		return
	}
	if req.Method == http.MethodPut {
		r.serveManifestPut(w, req, repo, tag, digest, isDigest)
		return
	}
	r.serveManifestGet(w, req, repo, tag, digest, isDigest)
}

// readManifestBody reads and checks size and emptiness of a PUT body.
func readManifestBody(w http.ResponseWriter, req *http.Request) ([]byte, bool) {
	raw, err := io.ReadAll(io.LimitReader(req.Body, maxManifestBytes+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "MANIFEST_INVALID", "read manifest body")
		return nil, false
	}
	if int64(len(raw)) > maxManifestBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "MANIFEST_INVALID", "manifest too large")
		return nil, false
	}
	if len(raw) == 0 {
		writeError(w, http.StatusBadRequest, "MANIFEST_INVALID", "empty manifest")
		return nil, false
	}
	return raw, true
}

// checkManifestJSON reports whether raw is valid JSON.
func checkManifestJSON(w http.ResponseWriter, raw []byte) bool {
	var probe map[string]any
	err := json.Unmarshal(raw, &probe)
	if err != nil {
		writeError(w, http.StatusBadRequest, "MANIFEST_INVALID", "invalid manifest json")
		return false
	}
	return true
}

// storeManifestBytes stores manifest bytes and tag, then answers 201.
func (r *Registry) storeManifestBytes(w http.ResponseWriter, ctx context.Context, repo, tag, digest string, raw []byte) {
	sum := sha256.Sum256(raw)
	computed := "sha256:" + hex.EncodeToString(sum[:])
	if digest != "" && computed != digest {
		writeError(w, http.StatusBadRequest, "DIGEST_INVALID", "digest mismatch")
		return
	}
	err := r.store.PutRaw(ctx, computed, raw)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "MANIFEST_INVALID", "store manifest")
		return
	}
	if tag != "" {
		err = r.store.PutTag(repo, tag, computed)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "MANIFEST_INVALID", "store tag")
			return
		}
	}
	r.writeManifestPutHeaders(w, repo, tag, digest, computed)
}

// writeManifestPutHeaders writes PUT success headers.
func (r *Registry) writeManifestPutHeaders(w http.ResponseWriter, repo, tag, digest, computed string) {
	_ = r
	w.Header().Set("Location", "/v2/"+repo+"/manifests/"+tag+digest)
	w.Header().Set("Docker-Content-Digest", computed)
	w.WriteHeader(http.StatusCreated)
}

// serveManifestPut stores a manifest under a tag or verifies a digest PUT.
func (r *Registry) serveManifestPut(w http.ResponseWriter, req *http.Request, repo, tag, digest string, isDigest bool) {
	raw, ok := readManifestBody(w, req)
	if !ok {
		return
	}
	if !checkManifestJSON(w, raw) {
		return
	}
	if isDigest {
		r.storeManifestBytes(w, req.Context(), repo, "", digest, raw)
		return
	}
	r.storeManifestBytes(w, req.Context(), repo, tag, "", raw)
}

// serveManifestGet answers manifest GET and HEAD by tag or digest.
// Reads use the capped store call so oversize files never load fully.
func (r *Registry) serveManifestGet(w http.ResponseWriter, req *http.Request, repo, tag, digest string, isDigest bool) {
	if !isDigest {
		resolved, ok := r.store.ResolveTag(repo, tag)
		if !ok {
			writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "unknown manifest")
			return
		}
		digest = resolved
	}
	r.writeManifestGet(w, req, digest)
}

// writeManifestGet fetches manifest bytes and writes the response.
func (r *Registry) writeManifestGet(w http.ResponseWriter, req *http.Request, digest string) {
	raw, err := r.store.GetRawCapped(digest, maxManifestBytes+1)
	if err != nil {
		if strings.Contains(err.Error(), "too large") {
			writeError(w, http.StatusRequestEntityTooLarge, "MANIFEST_INVALID", "manifest too large")
			return
		}
		writeError(w, http.StatusNotFound, "MANIFEST_UNKNOWN", "unknown manifest")
		return
	}
	if int64(len(raw)) > maxManifestBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "MANIFEST_INVALID", "manifest too large")
		return
	}
	w.Header().Set("Content-Type", manifestContentType(raw))
	w.Header().Set("Docker-Content-Digest", digest)
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.WriteHeader(http.StatusOK)
	if req.Method == http.MethodGet {
		_, _ = w.Write(raw) //nolint:errcheck,gosec // best effort manifest body, JSON not HTML
	}
}

// serveTagsList answers a tag listing for one repository.
// Unknown repos return 404. The tag dir must exist on disk.
func (r *Registry) serveTagsList(w http.ResponseWriter, req *http.Request, repo string) {
	defer req.Body.Close() //nolint:errcheck // close request body, no content needed
	if req.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "method not allowed")
		return
	}
	if !validRepo(repo) {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unknown repository")
		return
	}
	if !r.hasRepoDir(repo) {
		writeError(w, http.StatusNotFound, "NAME_UNKNOWN", "unknown repository")
		return
	}
	r.writeTagsList(w, repo)
}

// writeTagsList encodes the tag list for repo.
func (r *Registry) writeTagsList(w http.ResponseWriter, repo string) {
	tags := r.store.ListTags(repo)
	if tags == nil {
		tags = []string{}
	}
	body, err := json.Marshal(map[string]any{"name": repo, "tags": tags})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "UNKNOWN", "encode tags")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body) //nolint:errcheck // best effort tags body
}

// storeDirFor returns the blob store dir for temp files and direct
// raw access. It prefers the stored dir and falls back to the shared
// config path for registries built without New.
func (r *Registry) storeDirFor() string {
	if r.storeDir != "" {
		return r.storeDir
	}
	root, err := rootDirPath()
	if err != nil {
		return ""
	}
	return filepath.Join(root, "blobs")
}

// rawBlobPath maps a normalized digest to its raw chunk file.
func rawBlobPath(storeDir, digest string) (string, bool) {
	hexPart, ok := strings.CutPrefix(digest, "sha256:")
	if !ok {
		return "", false
	}
	if len(hexPart) != 64 {
		return "", false
	}
	if !isLowerHexString(hexPart) {
		return "", false
	}
	if storeDir == "" {
		return "", false
	}
	return filepath.Join(storeDir, "chunks", hexPart[:2], hexPart[2:]), true
}

// syncStoreDir fsyncs a dir so renames inside it are durable.
func syncStoreDir(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // dir is a store dir under config, not user input
	if err != nil {
		return fmt.Errorf("open dir for sync: %w", err)
	}
	err = f.Sync()
	if err != nil {
		_ = f.Close() //nolint:errcheck // close after failed sync
		return fmt.Errorf("sync dir: %w", err)
	}
	err = f.Close()
	if err != nil {
		return fmt.Errorf("close dir: %w", err)
	}
	return nil
}

// installRaw moves a verified temp file to its raw location. The temp
// file must already hold the digest bytes and be synced and closed.
// Rename keeps memory flat and stays on one filesystem because the
// temp file lives inside the store dir.
func (r *Registry) installRaw(tmpName, digest string) error {
	dest, ok := rawBlobPath(r.storeDirFor(), digest)
	if !ok {
		return fmt.Errorf("bad digest for raw install: %s", digest)
	}
	err := os.MkdirAll(filepath.Dir(dest), 0o700)
	if err != nil {
		return fmt.Errorf("create raw parent: %w", err)
	}
	err = os.Rename(tmpName, dest)
	if err != nil {
		return fmt.Errorf("rename raw file: %w", err)
	}
	err = syncStoreDir(filepath.Dir(dest))
	if err != nil {
		return err
	}
	return nil
}

// verifyRawFile hashes a raw file in 256KB pieces and compares it to
// the expected digest. Memory stays flat regardless of file size.
func verifyRawFile(path, digest string) error {
	f, err := os.Open(path) //nolint:gosec // path is a validated digest file under the store dir
	if err != nil {
		return fmt.Errorf("open raw for verify: %w", err)
	}
	defer func() {
		_ = f.Close() //nolint:errcheck // close after verify
	}()
	sum := sha256.New()
	buf := make([]byte, 256<<10)
	_, err = io.CopyBuffer(sum, f, buf)
	if err != nil {
		return fmt.Errorf("hash raw file: %w", err)
	}
	computed := "sha256:" + hex.EncodeToString(sum.Sum(nil))
	if computed != digest {
		return fmt.Errorf("raw hash mismatch: %s", digest)
	}
	return nil
}

// hasRepoDir reports whether the tag dir for repo exists on disk.
func (r *Registry) hasRepoDir(repo string) bool {
	dir := r.storeDirFor()
	if dir == "" {
		return false
	}
	tagsDir := filepath.Join(dir, "tags", filepath.FromSlash(repo))
	info, err := os.Stat(tagsDir) //nolint:gosec // tagsDir built from validated repo under store dir
	if err != nil {
		return false
	}
	return info.IsDir()
}
