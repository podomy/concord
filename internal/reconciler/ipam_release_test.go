// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package reconciler

import (
	"testing"

	"github.com/google/uuid"

	"github.com/podomy/concord/internal/logs"

	"github.com/podomy/concord/internal/cn"
	"github.com/podomy/concord/internal/workload"
)

// Container exit cleanup returns the IP to the pool: the next allocation
// reuses the freed address instead of marching toward exhaustion.
func TestExitCleanupReleasesIP(t *testing.T) {
	cn.SetNodeSubnet(11)
	t.Cleanup(func() { cn.SetNodeSubnet(0) })

	cidr, err := cn.AllocateIP()
	if err != nil {
		t.Fatalf("allocate ip: %v", err)
	}

	spec := workload.Spec{ID: uuid.New()}
	ipAndCIDRs := map[uuid.UUID]string{spec.ID: cidr}
	entry := &ContainerAndProcess{Spec: spec}

	cleanupContainerResources(t.Context(), logs.NewTest(t), entry, ipAndCIDRs)

	if len(ipAndCIDRs) != 0 {
		t.Fatalf("ipAndCIDRs = %v, want empty after cleanup", ipAndCIDRs)
	}
	got, err := cn.AllocateIP()
	if err != nil {
		t.Fatalf("allocate after cleanup: %v", err)
	}
	if got != cidr {
		t.Fatalf("reallocated = %s, want freed %s", got, cidr)
	}
}

// Adopted port mappings join the used set: the cursor skips the
// rediscovered IP instead of handing it out twice.
func TestAdoptPortMappingAdoptsIP(t *testing.T) {
	cn.SetNodeSubnet(12)
	t.Cleanup(func() { cn.SetNodeSubnet(0) })

	cidr, err := cn.AllocateIP()
	if err != nil {
		t.Fatalf("allocate ip: %v", err)
	}
	cn.ReleaseIP(cidr)
	cn.AdoptIP(cidr)

	got, err := cn.AllocateIP()
	if err != nil {
		t.Fatalf("allocate after adopt: %v", err)
	}
	if got == cidr {
		t.Fatalf("allocated adopted IP %s twice", cidr)
	}
}
