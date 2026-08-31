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
warehouse aisles to mines, factories, and space. It is a fleet brain,
not a real-time controller. Motor loops, collision avoidance, and
sensor fusion run at the edge, below its reach. It coordinates the
fleet; it does not pilot the machine.

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

Discuss your change with the community before opening a PR

[dev@podomy.com](mailto:dev@podomy.com)

[Archive of the past messages can be found here.](https://archive.podomy.com)

You must subscribe to receive responses.

- [Commit message format](./COMMITS)
- [Contributor license agreement](./CLA)
- [Contributing](./CONTRIBUTING)

### License

Concord is distributed under the GNU Affero General Public License v3.0 or
later. See [LICENSE](./LICENSE).
