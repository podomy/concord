<h1 align="center">Concord</h1>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-BUSL--1.1-blue" alt="License: BUSL-1.1"></a>
</p>

Fleets that lose their network keep running.

Concord is the AP coordination runtime for robots that operate where links die. Each node runs from its own journal with zero peers reachable; when segments reunite, state merges by rule instead of by master.

Proven by a 33-scenario kill, partition, and reunion suite, green end to end.

Full docs live in [docs/](/docs).

## Commercial Licensing

Commercial licenses are available. See [podomy.com/projects](https://podomy.com/projects/) for pricing, or contact [contact@podomy.com](mailto:contact@podomy.com).

## SDK

The SDK lives in [podomy/concord-sdk](https://github.com/podomy/concord-sdk), licensed separately under Apache License 2.0; client reference and examples live there.

## Contributing

Concord takes no pull requests or GitHub Issues. Propose first, patch second, both by email to [contact@podomy.com](mailto:contact@podomy.com): an RFC describing the problem, then a `git format-patch` series.

## License

Business Source License 1.1, with Change License GNU Affero General Public License v3.0 or later. See [LICENSE](/LICENSE). Prior releases remain AGPLv3-or-later, see [LICENSE.AGPL](/LICENSE.AGPL). Production use requires a commercial license. See [podomy.com/projects](https://podomy.com/projects/) or contact [contact@podomy.com](mailto:contact@podomy.com).
