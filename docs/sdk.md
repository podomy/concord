# Go SDK Reference

Concord provides a type-safe Go SDK for managing workloads and nodes programmatically.

---

## Installation

```bash
go get github.com/podomy/concord/sdk
```

---

## Connecting to the Daemon

`sdk.Dial()` connects to the local Concord daemon Unix socket (`~/.config/concord/concord.sock`):

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/podomy/concord/sdk"
)

func main() {
	client, err := sdk.Dial()
	if err != nil {
		log.Fatalf("connect to concord daemon: %v", err)
	}
	defer client.Close()
}
```

---

## Deploying a Workload

Use the fluent builder `sdk.NewWorkload()` to define container specifications:

```go
ctx := context.Background()

spec, err := sdk.NewWorkload().
	Image("docker.io/library/nginx:alpine").
	Port(8080, 80).
	Env("ENV", "production").
	Restart(sdk.RestartAlways).
	CPUShares(1024).
	MemoryMB(512).
	HealthCheck("/healthz", sdk.HealthActionRestart).
	Build()
if err != nil {
	log.Fatalf("invalid spec: %v", err)
}

id, err := client.Submit(ctx, spec)
if err != nil {
	log.Fatalf("submit workload: %v", err)
}

fmt.Printf("Workload submitted: %s\n", id)
```

---

## Listing Active Workloads

```go
workloads, err := client.List(ctx)
if err != nil {
	log.Fatalf("list workloads: %v", err)
}

for _, w := range workloads {
	fmt.Printf("[%s] %s (ports: %d:%d, restart: %s)\n",
		w.ID, w.Image, w.HostPort, w.ContainerPort, w.Restart)
}
```

---

## Inspecting a Workload

```go
workload, err := client.Get(ctx, workloadID)
if err != nil {
	log.Fatalf("get workload: %v", err)
}

fmt.Printf("Image: %s, CPU Shares: %d\n", workload.Image, workload.CPUShares)
```

---

## Stopping a Workload

```go
if err := client.Stop(ctx, workloadID); err != nil {
	log.Fatalf("stop workload: %v", err)
}

fmt.Println("Workload stopped successfully")
```

---

## Inspecting Cluster Nodes

```go
nodes, err := client.Nodes(ctx)
if err != nil {
	log.Fatalf("get cluster nodes: %v", err)
}

for _, n := range nodes {
	fmt.Printf("Node %s | %s | %s\n", n.ID, n.Address, n.State)
}
```
