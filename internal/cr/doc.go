// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package cr pulls OCI images from the local registry and builds containers.
// ImagePuller fetches manifests, configs, and layers then extracts a rootfs.
// BundleBuilder maps pull result plus spec to isolation, mounts, and limits.
// A local fast path reads raw blobs from disk with digest recheck and HTTP
// fallback. Extraction honors OCI whiteouts and rejects paths, symlinks, and
// hardlinks that escape the rootfs.
package cr
