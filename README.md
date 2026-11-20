<h1 align="center">Concord</h1>

<p align="center">
  <a href="https://github.com/podomy/concord/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/podomy/concord/ci.yml?label=linux" alt="Linux"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/podomy/concord" alt="License"></a>
</p>

Fleets that lose their network keep running.

Concord is the AP coordination runtime for robots that operate where links die. Each node runs from its own journal with zero peers reachable; when segments reunite, state merges by rule instead of by master.

Proven by a 33-scenario kill, partition, and reunion suite, green end to end.

## Scope

Consolidated, not expanding: Noise IK over :8443 is the only transport. No new surface, fixes only. Every change keeps behavior identical and is measured before and after.

Full docs live in [docs/](/docs).

## Contributing

Concord takes no pull requests. Propose first, patch second, both by email to [contact@podomy.com](mailto:contact@podomy.com): an RFC describing the problem, then a `git format-patch` series.

## License

GNU Affero General Public License v3.0 or later. See [LICENSE](/LICENSE). Questions: [contact@podomy.com](mailto:contact@podomy.com).
