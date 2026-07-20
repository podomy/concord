### What Concord does well

- **Fully disconnected operation** Each node is autonomous. No central
  coordination needed.
- **No split-brain** No leader, no quorum. Every partition makes progress.
- **Graceful catch-up** Pull reconciliation with watermarks syncs only what
  changed.
- **Deterministic state reconstruction** Journal replay rebuilds node state.
- **No SPOF** No coordinator, no single registry, no control plane.
- **Simple ops** Join a node and it converges.

### What Concord does not do

- **Strong consistency** No read-your-writes or linearizability across nodes.
  Eventually consistent by design.
- **Global ordering** No total order across segments. Events have causal
  ordering at best.
- **Real-time** 5 s pull interval. Sub-second requires local processing.
- **Distributed transactions** No atomic multi-node commits.
- **Resource migration** Workloads are assigned to a segment. If the segment
  dies, the workload is lost.
- **Large single meshes** A single memberlist mesh is bounded to hundreds of
  nodes. Multi-segment hierarchy scales to thousands.
