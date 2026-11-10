# langalpha-chartkit

Chart layers from pandas. Agent code in a LangAlpha sandbox builds an
explained overlay for the user's chart (indicators, channels, fibs, patterns,
projections, trade setups, alerts), checks it against the layer contract, and
snapshots it as the user will see it before publishing.

## Two sides of one contract

- **In the sandbox** the package ships as `chartkit` (`_internal/src/chartkit/`,
  through the backend's internal packages). `Chart(df).publish()` appends the
  layer as one JSON line to the job's outbox file; the backend publishes it
  when the job ends.
- **On the server** only `chartkit.schema` (the layer contract, `parse_layer`)
  and `chartkit.protocol` (the env names) are read.
  Neither imports pandas or numpy.

## Modules

| Module | What it is |
|---|---|
| `chart` | `Chart`: the builder, its drawing methods, `publish()` and `snapshot()` |
| `analysis` | Helpers that turn bars into drawing arguments plus quotable numbers |
| `ta` | Indicators with TradingView Pine v5 semantics |
| `data` | `bars()`: the chart's own OHLCV from the sandbox's market-data tool |
| `schema` | ChartLayer v1, the one definition of a layer on both sides |
| `protocol` | Names shared with the server |
| `_snapshot`, `_view`, `_bars_source`, `_renderer`, `_report` | Snapshots: the app's own chart in headless Chromium |

The agent-facing guide is
`plugins/langalpha_deliverables/skills/chart-annotation/references/chartkit.md`.

## The render page

Snapshots load `chartkit/render/chart-render.html`, the web app's
`MarketChart` built alone (`cd web && pnpm build:chart-render`). It is a build
output, so it is gitignored; the backend images build it in their
`chart-render` stage. Without it, `snapshot()` says snapshots are unavailable
and `publish()` still works.

## Development

```bash
cd libs/chartkit && uv run pytest -v --tb=short   # its own environment
make test-chartkit                                 # the same, from the root
```

Sandbox-only imports (`playwright`, the generated `tools.price_data` wrapper)
load lazily, so the suite runs without them.
