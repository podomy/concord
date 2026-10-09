// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/geo"
)

type NodeConfig struct {
	// MemberlistAddress is the local bind address for gossip.
	MemberlistAddress netip.AddrPort `json:"memberlist_address"`
	// AdvertiseAddress is an optional IP other peers should dial. The port
	// always comes from MemberlistAddress (bind). Empty means the runtime
	// picks one from the bind address or a non-loopback interface.
	AdvertiseAddress netip.Addr `json:"advertise_address"`
	ID               uuid.UUID  `json:"id"`
	// Anchor marks this node as a rendezvous anchor: stable, reachable,
	// and safe for newcomers to join. Provisioned with concord --anchor.
	Anchor bool `json:"anchor,omitempty"`
	// Position is this node's geographic position, or nil when unknown.
	// Anchors use it to order nearest-first; without it the anchor list
	// order decides. Operator-provisioned; anchors are stationary so
	// theirs never goes stale.
	Position *geo.Point `json:"position,omitempty"`
	// Anchors are rendezvous addresses tried in order (nearest first when
	// Position is known). Empty means LAN-only discovery via mDNS.
	Anchors []AnchorEntry `json:"anchors,omitempty"`
}

// AnchorEntry is one rendezvous anchor: a stable reachable address plus
// the coordinates that order it against the others.
type AnchorEntry struct {
	// Name labels the anchor for operators (depot, rim-mast). Unused by code.
	Name string         `json:"name,omitempty"`
	Addr netip.AddrPort `json:"address"`
	Lat  float64        `json:"lat"`
	Lon  float64        `json:"lon"`
}

// AnchorPoint returns the entry's coordinates, or false when they are
// outside the planet and must not order anything.
func (a AnchorEntry) AnchorPoint() (geo.Point, bool) {
	p := geo.Point{Lat: a.Lat, Lon: a.Lon}
	if !p.Valid() {
		return geo.Point{}, false
	}
	return p, true
}

// getNodeConfigPath returns the auto-determined path for the local node config.
func getNodeConfigPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return "", fmt.Errorf("create node config directory: %w", err)
	}

	return filepath.Join(appDir, "config.json"), nil
}

// LoadOrCreateNodeConfig creates or loads the config for this node.
// The path is auto-determined, you cannot specify it.
// In practice configuration directory of the user gets used.
func LoadOrCreateNodeConfig() (config *NodeConfig, err error) {
	configPath, err := getNodeConfigPath()
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filepath.Clean(configPath), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open node config: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("close node config: %w; original error: %w", closeErr, err)
				return
			}

			err = fmt.Errorf("close node config: %w", closeErr)
		}
	}()

	return decodeNodeConfig(file)
}

// decodeNodeConfig unmarshals JSON from the provided reader into a NodeConfig.
// If the reader is empty (io.EOF), it falls back to creating and persisting a new config.
func decodeNodeConfig(reader io.Reader) (*NodeConfig, error) {
	var config NodeConfig

	if err := json.NewDecoder(reader).Decode(&config); err != nil {
		if errors.Is(err, io.EOF) {
			return createNodeConfig()
		}

		return nil, fmt.Errorf("decode node config: %w", err)
	}

	return &config, nil
}

// createNodeConfig generates a new NodeConfig with a fresh UUID and persists it to disk.
func createNodeConfig() (*NodeConfig, error) {
	config := &NodeConfig{ID: uuid.New()}
	if _, err := UpdateNodeConfig(config); err != nil {
		return nil, err
	}

	return config, nil
}

// PersistPosition stores a live position fix into the node config so the
// next boot publishes it. The file is storage, not an interface: live
// updates arrive over IPC and call this; nothing polls the file back.
// Load-modify-write preserves anchors, the anchor flag, and identity.
func PersistPosition(lat, lon float64) error {
	config, err := LoadOrCreateNodeConfig()
	if err != nil {
		return err
	}
	config.Position = &geo.Point{Lat: lat, Lon: lon}
	if _, err := UpdateNodeConfig(config); err != nil {
		return err
	}
	return nil
}

// UpdateNodeConfig returns a pointer to the written result if the update
// was successful, otherwise it returns an error.
func UpdateNodeConfig(config *NodeConfig) (_ *NodeConfig, err error) {
	configPath, err := getNodeConfigPath()
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filepath.Clean(configPath), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open node config: %w", err)
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			if err != nil {
				err = fmt.Errorf("close node config: %w; original error: %w", closeErr, err)
				return
			}

			err = fmt.Errorf("close node config: %w", closeErr)
		}
	}()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", " ")
	if err := encoder.Encode(config); err != nil {
		return nil, fmt.Errorf("encode node config: %w", err)
	}

	return config, nil
}
