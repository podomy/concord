# Go SDK Reference

```bash
go get github.com/podomy/concord/sdk
```

`sdk.Dial()` connects to `~/.config/concord/concord.sock`.

```go
client, err := sdk.Dial()
if err != nil {
    log.Fatalf("connect to concord daemon: %v", err)
}
defer client.Close()
```

## Example

```go
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

id, err := client.Submit(context.Background(), spec)
if err != nil {
    log.Fatalf("submit workload: %v", err)
}
fmt.Printf("Workload submitted: %s\n", id)
```

## API

| Method | Description |
| :--- | :--- |
| `client.Submit(ctx, w)` | Submit a workload, returns its UUID |
| `client.Stop(ctx, id)` | Stop and remove a workload |
| `client.Get(ctx, id)` | Fetch a workload spec by UUID |
| `client.List(ctx)` | List active workloads |
| `client.Nodes(ctx)` | List cluster nodes |

Builder methods: `Image`, `Command`, `Env`, `Envs`, `Port`, `Resources`, `MemoryMB`, `CPUShares`, `Restart`, `HealthCheck`, `StopTimeout`, `StopTimeoutSeconds`, `Build`, `MustBuild`.
