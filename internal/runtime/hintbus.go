// Copyright (C) 2026 Forge Universal LLC.
// SPDX-License-Identifier: BUSL-1.1

package runtime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/podomy/weld/bus"

	"github.com/podomy/concord/internal/hints"
	"github.com/podomy/concord/internal/journal"
	"github.com/podomy/concord/internal/journalview"
	"github.com/podomy/concord/internal/logs"
)

// hintPublishQueue caps queued outgoing hints. A full queue drops,
// the ticker still converges.
const hintPublishQueue = 64

// hintPublishTimeout bounds one hint publish on the bus.
const hintPublishTimeout = 2 * time.Second

// openHintBackend joins the weld bus for pull hints. It reports an
// error when discovery cannot join, and the caller falls back to the
// ticker alone. Discovery is scoped to localhost: hints stay on one
// host by default so single host deployments need no network tuning.
// Fleet deployments that span hosts must pass a wider range instead.
func openHintBackend(ctx context.Context, participant string) (bus.Backend, error) {
	cfg := bus.DefaultDDSConfig()
	cfg.Range = bus.DiscoveryRangeLocalhost
	info := bus.DiscoveryInfo{
		ParticipantName: participant,
		DomainID:        cfg.DomainID,
		Range:           cfg.Range,
	}
	backend, err := bus.OpenDDS(ctx, cfg, info)
	if err != nil {
		return nil, fmt.Errorf("open hint backend: %w", err)
	}
	return backend, nil
}

// startHintBus publishes local journal events as hints and relays bus
// hints into hintCh for the pull loop. A nil backend disables hints
// and returns nil. The observer is set before background loops start and
// cleared by the caller after they drain.
func startHintBus(
	runCtx context.Context,
	logger *logs.Logger,
	backend bus.Backend,
	bg *sync.WaitGroup,
) <-chan hints.Hint {
	if backend == nil {
		return nil
	}
	publishCh := make(chan hints.Hint, hintPublishQueue)
	hintCh := hints.NewChannel()
	journalview.SetNotify(func(event journal.Event) {
		h := hints.Hint{Origin: event.NodeID, Tip: event.ID}
		select {
		case publishCh <- h:
		default:
		}
	})
	goBackground(bg, func() {
		runHintPublisher(runCtx, logger, backend, publishCh)
	})
	goBackground(bg, func() {
		hints.RunRelay(runCtx, logger, backend, hintCh)
	})
	return hintCh
}

// attachHintBus opens the hint backend and starts the hint loops.
// A backend failure degrades to ticker only with a warn, returning nil
// channel and a no-op cleanup. The caller defers cleanup: it runs after
// the background loops drain, closing the backend while nothing uses it
// and clearing the observer.
func attachHintBus(
	runCtx context.Context,
	logger *logs.Logger,
	nodeID uuid.UUID,
	bg *sync.WaitGroup,
) (<-chan hints.Hint, func()) {
	backend, err := openHintBackend(runCtx, "concord-"+nodeID.String())
	if err != nil {
		logger.Warn("hint bus unavailable, ticker only", logs.Error(err))
		return nil, func() {}
	}
	hintCh := startHintBus(runCtx, logger, backend, bg)
	return hintCh, func() {
		journalview.ClearNotify()
		err := backend.Close()
		if err != nil {
			logger.Warn("close hint backend", logs.Error(err))
		}
	}
}

// runHintPublisher forwards queued hints onto the bus. It blocks until
// ctx ends. A failed publish drops quietly, the ticker still converges.
// Each publish is bounded by hintPublishTimeout and derived from ctx so
// cancel aborts the wait promptly instead of overrunning the drain.
func runHintPublisher(
	ctx context.Context,
	logger *logs.Logger,
	backend bus.Backend,
	publishCh <-chan hints.Hint,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case h := <-publishCh:
			pubCtx, cancel := context.WithTimeout(ctx, hintPublishTimeout)
			err := hints.Publish(pubCtx, backend, h)
			cancel()
			if err != nil {
				logger.Debug("hint publish failed", logs.Error(err))
			}
		}
	}
}
