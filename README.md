<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/concord-robot-dark.png">
    <source media="(prefers-color-scheme: light)" srcset="./assets/concord-robot-light.png">
    <img src="./assets/concord-robot-light.png" alt="Concord" width="15%">
  </picture>
</p>

<h1 align="center">Concord</h1>

<p align="center">
  <a href="https://github.com/podomy/concord/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/podomy/concord/ci.yml?label=linux" alt="Linux"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/podomy/concord" alt="License"></a>
</p>

<p align="center">
  <code>binary size 103 MB</code>&nbsp;&nbsp;&nbsp;<code>startup RSS ~54 MB</code>
</p>

Concord is an AP distributed system, a runtime and coordination layer
designed for robotic fleets. It gives you highest reliability from
warehouse aisles to mines, factories, and space. It is a fleet brain
that coordinates the fleet, not a real-time controller. Motor,
collision, and sensor loops run at the edge.

Developed with [Resonance](https://github.com/podomy/resonance), the
simulator for Concord. It is the primary way we validate partition,
reunion, and scheduling.

### Documentation

- [Overview](./docs/overview.md)
- [Architecture](./docs/architecture.md)
- [CLI Reference](./docs/cli.md)
- [Go SDK Reference](./docs/sdk.md)
- [Deployment Guide](./docs/deployment.md)

### Contributing

Discuss your change with the engineering team at [contact@podomy.com](mailto:contact@podomy.com) before opening a PR in order not to waste anybody's effort or time.

Announcements and engineering updates on distributed systems, consensus algorithms, and robotic fleet coordination are shared via our newsletter at [podomy.com](https://podomy.com).

- [Commit message format](./COMMITS)
- [Contributor license agreement](./CLA)
- [Contributing](./CONTRIBUTING)

### License

Concord is distributed under the GNU Affero General Public License v3.0 or
later. See [LICENSE](./LICENSE).
