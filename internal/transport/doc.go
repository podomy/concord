// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

// Package transport fetches blob manifests and bytes from peer registries.
// PeerRegistryBase builds the v2 base URL from underlay IP and registry port.
// BlobClient offers FetchManifestTag, FetchBlob, and FetchTags plus
// ParseBlobManifest validation. Caps bound one peer: 1MB manifests, 32MB
// in-memory raw, 32GB streaming wire cap. Every byte is hash verified.
package transport
