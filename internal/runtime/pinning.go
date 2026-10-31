// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/certs"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/transport"
)

// KeyPinner implements transport.Verifier with first-seen key pinning. The
// first valid key seen for a node ID sticks: later handshakes presenting a
// different key at the same generation are rejected with a loud log, and the
// operator recovers by bumping the generation, which is an intentional act.
// Pins persist in the journal, so restarts re-pin from replay instead of
// trusting whoever arrives first.
//
// The steady path, a handshake presenting the pinned key, is one view read
// and records nothing. Recording happens only on first sight and rotation.
type KeyPinner struct {
	selfID  uuid.UUID
	journal journal.Journal
	views   []journalview.View
	pins    *journalview.PinnedKeys
	verify  transport.Verifier
	logger  *logs.Logger
}

// newKeyPinner builds the transport verifier for this node: CA signature
// check wrapped with first-seen pinning against the node's views. The
// returned closure captures ctx for pin journal writes; verify itself runs on
// both handshake directions.
func newKeyPinner(
	ctx context.Context,
	logger *logs.Logger,
	selfID uuid.UUID,
	j journal.Journal,
	views []journalview.View,
	pins *journalview.PinnedKeys,
) (transport.Verifier, error) {
	paths, err := certs.DefaultPaths()
	if err != nil {
		return nil, fmt.Errorf("default cert paths: %w", err)
	}

	caCert, err := transport.LoadCACert(paths.CA)
	if err != nil {
		return nil, fmt.Errorf("load ca: %w", err)
	}

	pinner := &KeyPinner{
		selfID:  selfID,
		journal: j,
		views:   views,
		pins:    pins,
		verify:  transport.CAVerifier(caCert),
		logger:  logger,
	}

	return func(id uuid.UUID, generation uint64, pub, sig []byte) error {
		return pinner.Verify(ctx, id, generation, pub, sig)
	}, nil
}

// Verify checks a presented Noise parcel: CA signature first, so an unsigned
// key is never pinned, then the pin table.
func (p *KeyPinner) Verify(ctx context.Context, id uuid.UUID, generation uint64, pub, sig []byte) error {
	if err := p.verify(id, generation, pub, sig); err != nil {
		return err
	}

	pinned, err := p.pins.Get(ctx, id)
	if err != nil {
		return fmt.Errorf("pinned keys lookup: %w", err)
	}

	if pinned == nil {
		return p.pin(ctx, id, generation, pub, "peer key pinned")
	}

	if generation > pinned.Generation {
		return p.pin(ctx, id, generation, pub, "peer key rotated")
	}

	if generation < pinned.Generation {
		return fmt.Errorf("peer %s presents stale generation %d, pinned %d", id, generation, pinned.Generation)
	}

	if !bytes.Equal(pinned.PublicKey, pub) {
		p.logger.Warn("peer key pin mismatch: pinned key sticks",
			logs.String("peer_id", id.String()),
			logs.Uint64("generation", generation),
			logs.String("pinned_key", hex.EncodeToString(pinned.PublicKey)),
			logs.String("presented_key", hex.EncodeToString(pub)),
		)
		return fmt.Errorf("peer %s presents an unpinned key at generation %d", id, generation)
	}

	return nil
}

// pin records a first sight or rotation and admits the peer.
func (p *KeyPinner) pin(ctx context.Context, id uuid.UUID, generation uint64, pub []byte, message string) error {
	pin := journalview.KeyPin{NodeID: id, PublicKey: pub, Generation: generation}
	if err := journalview.RecordKeyPin(ctx, p.journal, p.views, p.selfID, pin); err != nil {
		return fmt.Errorf("record key pin: %w", err)
	}

	p.logger.Info(message,
		logs.String("peer_id", id.String()),
		logs.Uint64("generation", generation),
		logs.String("key", hex.EncodeToString(pub)),
	)

	return nil
}
