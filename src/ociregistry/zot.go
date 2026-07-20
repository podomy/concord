package ociregistry

import (
	"context"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"

	"zotregistry.dev/zot/v2/pkg/api"
	"zotregistry.dev/zot/v2/pkg/api/config"
)

type Registry struct {
	controller *api.Controller
	port       int
}

func rootDir() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config directory: %w", err)
	}

	appDir := filepath.Join(dir, "concord", "zot")
	if err := os.MkdirAll(appDir, 0o700); err != nil {
		return "", fmt.Errorf("create zot directory: %w", err)
	}

	return appDir, nil
}

func New(port int) (*Registry, error) {
	root, err := rootDir()
	if err != nil {
		return nil, err
	}

	cfg := config.New()
	cfg.Storage.StorageConfig.RootDirectory = root
	cfg.HTTP.Address = "0.0.0.0"
	cfg.HTTP.Port = strconv.Itoa(port)
	cfg.Log.Level = "error"
	cfg.Extensions = nil

	return &Registry{
		controller: api.NewController(cfg),
		port:       port,
	}, nil
}

func (r *Registry) Start(ctx context.Context) error {
	if err := r.controller.Init(); err != nil {
		return fmt.Errorf("zot init: %w", err)
	}

	go func() {
		if err := r.controller.Run(); err != nil {
			return
		}
	}()

	go func() {
		<-ctx.Done()
		r.Stop()
	}()

	return nil
}

func (r *Registry) Stop() {
	r.controller.Shutdown()
}

func (r *Registry) Addr() netip.AddrPort {
	return netip.AddrPortFrom(netip.IPv4Unspecified(), uint16(r.controller.GetPort()))
}
