<h1 align="center">Concord</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/github/license/podomy/concord" alt="License"></a>
</p>

Fleets that lose their network keep running.

Concord is the AP coordination runtime for robots that operate where links die. Each node runs from its own journal with zero peers reachable; when segments reunite, state merges by rule instead of by master.

Proven by a 33-scenario kill, partition, and reunion suite, green end to end.

Full docs live in [docs/](/docs).

## SDK

The Go client lives in [podomy/concord-sdk](https://github.com/podomy/concord-sdk). It is licensed separately under Apache License 2.0. Install with `go get github.com/podomy/concord-sdk@latest`; see [docs/sdk](/docs/sdk) for the reference.

## Contributing

Concord takes no pull requests or GitHub Issues. Propose first, patch second, both by email to [contact@podomy.com](mailto:contact@podomy.com): an RFC describing the problem, then a `git format-patch` series.

## License

GNU Affero General Public License v3.0 or later. See [LICENSE](/LICENSE). Questions: [contact@podomy.com](mailto:contact@podomy.com).
