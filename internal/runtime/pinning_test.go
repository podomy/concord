// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package runtime

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"path/filepath"
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/certs"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/kvstore"
	"github.com/podomy/concord/internal/transport"
)

// pinFixture holds a verifier wired to temp disk state plus a real CA.
type pinFixture struct {
	verify transport.Verifier
	pins   *journalview.PinnedKeys
	selfID uuid.UUID
}

func newPinFixture(t *testing.T) pinFixture {
	t.Helper()

	t.Setenv("XDG_CONFIG_HOME", t.TempDir())

	if err := certs.WriteCA(); err != nil {
		t.Fatalf("write CA: %v", err)
	}

	dir := t.TempDir()
	kv, err := kvstore.OpenDBPath(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("open kv: %v", err)
	}
	t.Cleanup(func() {
		if err := kv.Close(); err != nil {
			t.Fatalf("close kv: %v", err)
		}
	})

	j, err := journal.OpenJSONLPath(filepath.Join(dir, "journal.jsonl"))
	if err != nil {
		t.Fatalf("open journal: %v", err)
	}

	pins := journalview.NewPinnedKeys(kv)
	views := []journalview.View{pins}
	selfID := uuid.New()

	verifier, err := newKeyPinner(context.Background(), logs.NewNop(), selfID, j, views, pins)
	if err != nil {
		t.Fatalf("new pinner: %v", err)
	}

	return pinFixture{verify: verifier, pins: pins, selfID: selfID}
}

func testStaticPub(t *testing.T) []byte {
	t.Helper()

	key, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	return key.PublicKey().Bytes()
}

func TestPinnerPinsFirstSight(t *testing.T) {
	fx := newPinFixture(t)
	ctx := context.Background()
	peerID := uuid.New()
	pub := testStaticPub(t)

	sig, err := certs.SignNodeKey(peerID, 0, pub)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}

	if err := fx.verify(peerID, 0, pub, sig); err != nil {
		t.Fatalf("first sight rejected: %v", err)
	}

	pinned, err := fx.pins.Get(ctx, peerID)
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if pinned == nil || !bytes.Equal(pinned.PublicKey, pub) || pinned.Generation != 0 {
		t.Fatalf("pin = %+v, want gen 0 key", pinned)
	}

	// Same key again: admitted, no error.
	if err := fx.verify(peerID, 0, pub, sig); err != nil {
		t.Fatalf("pinned key rejected: %v", err)
	}
}

func TestPinnerRejectsSameGenerationMismatch(t *testing.T) {
	fx := newPinFixture(t)
	ctx := context.Background()
	peerID := uuid.New()
	pubA := testStaticPub(t)
	pubB := testStaticPub(t)

	sigA, err := certs.SignNodeKey(peerID, 0, pubA)
	if err != nil {
		t.Fatalf("sign A: %v", err)
	}
	sigB, err := certs.SignNodeKey(peerID, 0, pubB)
	if err != nil {
		t.Fatalf("sign B: %v", err)
	}

	if err := fx.verify(peerID, 0, pubA, sigA); err != nil {
		t.Fatalf("first sight rejected: %v", err)
	}
	if err := fx.verify(peerID, 0, pubB, sigB); err == nil {
		t.Fatal("same-generation mismatch admitted")
	}

	// The pin sticks to the first key.
	pinned, err := fx.pins.Get(ctx, peerID)
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if !bytes.Equal(pinned.PublicKey, pubA) {
		t.Fatal("pin moved to the mismatched key")
	}
}

func TestPinnerAdmitsRotation(t *testing.T) {
	fx := newPinFixture(t)
	ctx := context.Background()
	peerID := uuid.New()
	pubA := testStaticPub(t)
	pubB := testStaticPub(t)

	sigA, err := certs.SignNodeKey(peerID, 0, pubA)
	if err != nil {
		t.Fatalf("sign A: %v", err)
	}
	sigB, err := certs.SignNodeKey(peerID, 1, pubB)
	if err != nil {
		t.Fatalf("sign B: %v", err)
	}

	if err := fx.verify(peerID, 0, pubA, sigA); err != nil {
		t.Fatalf("gen 0 rejected: %v", err)
	}
	if err := fx.verify(peerID, 1, pubB, sigB); err != nil {
		t.Fatalf("rotation rejected: %v", err)
	}

	pinned, err := fx.pins.Get(ctx, peerID)
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if pinned.Generation != 1 || !bytes.Equal(pinned.PublicKey, pubB) {
		t.Fatalf("pin = %+v, want gen 1 key", pinned)
	}

	// The old generation is now stale.
	if err := fx.verify(peerID, 0, pubA, sigA); err == nil {
		t.Fatal("stale generation admitted")
	}
}

func TestPinnerRejectsUnsignedKey(t *testing.T) {
	fx := newPinFixture(t)
	ctx := context.Background()
	peerID := uuid.New()
	pub := testStaticPub(t)

	if err := fx.verify(peerID, 0, pub, []byte("forged")); err == nil {
		t.Fatal("unsigned key admitted")
	}

	// Nothing pinned from a failed verification.
	pinned, err := fx.pins.Get(ctx, peerID)
	if err != nil {
		t.Fatalf("get pin: %v", err)
	}
	if pinned != nil {
		t.Fatalf("unsigned key pinned: %+v", pinned)
	}
}
