<p align="center">
  <img src="assets/concord-robot-transparent.png" alt="Concord" width="15%">
</p>

<h1 align="center">Concord</h1>

<p align="center">
  <a href="https://github.com/podomy/concord/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/podomy/concord/ci.yml?label=linux" alt="Linux"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/podomy/concord" alt="License"></a>
</p>

<p align="center">
  <code>binary size 103 MB</code>&nbsp;&nbsp;&nbsp;<code>startup RSS ~54 MB</code>
</p>

Concord is a runtime for machine fleets that lose their network and keep
running. Built for space, sea, remote terrain, and underground. Places where
clusters segment, operate locally, and reunite later.

Concord is designed for mathematical consistency. Each segment can keep
operating from local knowledge, and when segments meet again their state
is reconciled by explicit rules instead of a hidden central truth.

Concord is a fleet brain, not a real-time controller. Motor loops, collision
avoidance, and sensor fusion run at the edge, below Concord's reach. Concord
coordinates the fleet; it does not pilot the machine.

### Documentation

- [Commit message format](./COMMITS)
- [Contributor license agreement](./CLA)
- [Contributing](./CONTRIBUTING)

### Contributing

Please discuss your idea with the community before opening a PR. Create an RFC and propose it if the change is complex.

https://groups.google.com/g/podomy

See [CONTRIBUTING](./CONTRIBUTING) for details.

### License

Concord is distributed under the GNU Affero General Public License v3.0 or
later. See [LICENSE](./LICENSE).
