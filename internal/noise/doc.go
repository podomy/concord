// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: BUSL-1.1

// Package noise is a clean-room Noise IK handshake for the Concord mesh.
// IK hides the initiator static key until the responder authenticates,
// binds the responder key from gossip into the handshake hash, and splits
// into per-direction transports after Split. Transports serialize one
// direction at a time; Rekey points must be agreed by the application.
// A nonce exhaustion or failed decrypt terminates that direction.
package noise
