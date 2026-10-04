# langalpha-market-protocol

The Common Market Data Protocol (CMDP): one vocabulary for market-data
identity, time, price and freshness that every producer and consumer of
bars, quotes and series can agree on. It is a contract package, so it has no
network code and no provider clients; it only defines what a well-formed
market-data record is and how instruments, venues and routing decisions are
named.

## What it covers

- **Instrument keys and symbology** (`market_protocol.symbology`): the canonical
  `SYMBOL.MIC` key, the routing market of each venue (`market_of`), index
  families, and per-instrument overrides seeded from `instruments.yaml`, which
  ships inside the package. `to_canonical` parses display, legacy and common
  vendor spellings (`600519.SS`, `^GSPC`, `I:SPX`, `EURUSD=X`) and raises
  `ValueError` on input that cannot be a symbol, including path and URL
  delimiters. Real spellings still carry `&`, `=` and `:`, so a symbol is
  percent-encoded like any other value on its way into a URL. Rendering
  covers the display and legacy API spellings plus the suffix spelling FMP and
  yfinance resolve (`vendor_symbol`); any other vendor's request format
  belongs to that vendor's adapter.
- **Venues and calendars** (`market_protocol.calendars`): exchange calendars
  keyed by calendar id (`InstrumentRef.calendar_id`, not the MIC: XNAS trades
  on the XNYS calendar), session bounds, lunch breaks, early closes, the
  market phase (pre, regular, lunch, post, closed) at any instant, and the
  next and previous session.
- **Bar, quote and series types** (`market_protocol.models`): `OhlcvBar`,
  `Quote`, `Series`, `SeriesHeader`, `Coverage`, `Gap` and `InstrumentRef`, with
  timestamps in Unix milliseconds UTC. A bar is stamped at the open of its
  window, so a daily bar carries 00:00 of its trade date in the instrument's
  time zone (UTC for crypto and FX). `Series.to_wire()` is the wire form; it
  writes field aliases such as `schema`. Non-finite prices are rejected.
- **Series builder** (`market_protocol.series`): `build_series` turns a
  provider's rows into a `Series` sorted by time with one bar per timestamp
  (the last row wins). Rows arrive in the venue's quote unit; pence on XLON
  are scaled to pounds, and the header names the currency the bars are in.
- **Lineage enums** (`market_protocol.enums`): `AssetClass`, `MarketPhase`,
  `PriceTreatment`, `Tier`, `FeedScope` and the canonical interval schemas.
- **Interval helpers** (`market_protocol.intervals`): canonical bar schemas
  (`ohlcv-1m`) and conversions from the bare (`1m`) and legacy (`1min`)
  spellings.
- **Lag ceilings** (`market_protocol.freshness`): where realtime and delayed
  end for a measured lag, how far ahead of the clock a lag stops being clock
  skew, and `classify_lag` to turn one into a `Tier`.
- **Provider routing ruleset** (`market_protocol.routing`): the schema of a
  generated routing table (which provider serves which
  `market / asset_class / surface / interval` cell), the deterministic ordering
  policy applied to probe measurements, and a loader that migrates older
  ruleset versions and skips a malformed cell rather than the whole file.
  Where the file lives, and how it is generated and merged, is the deploying
  service's concern.

## Usage

```python
from pathlib import Path

import market_protocol
from market_protocol.calendars import get_calendar
from market_protocol.routing import load_ruleset, RoutingTable

ref = market_protocol.to_canonical("600519.SH")      # InstrumentRef, key "600519.XSHG"
phase = get_calendar(ref.calendar_id).phase_at(some_datetime)  # MarketPhase.REGULAR, ...

ruleset = load_ruleset(Path("data_routing.yaml"))     # None when absent or unreadable
if ruleset is not None:
    # None again for a cell the file does not cover: keep the configured order.
    order = RoutingTable(ruleset).order_for(
        "intraday", market="cn", asset_class="equity", interval="ohlcv-1m"
    )
```

## Installing

The distribution is `langalpha-market-protocol` and the import name is
`market_protocol`. It is not published to a package index; it installs from a
subdirectory of the LangAlpha repository. Declare it as a direct reference in
`dependencies`, which uv and pip both read:

```toml
[project]
dependencies = [
    "langalpha-market-protocol @ git+https://github.com/ginlix-ai/LangAlpha@<commit>#subdirectory=libs/market-protocol",
]
```

A bare `langalpha-market-protocol` with the location only in
`[tool.uv.sources]` works under `uv sync`, but an installer that ignores those
sources (pip, `uv --no-sources`, a built wheel's metadata) looks the name up on
the package index instead.

Pin the full 40-character SHA of a commit on `main`. A commit that exists
only on a feature branch can disappear when that branch is rebased or deleted.

- **hatchling** refuses a direct reference unless the consumer's own
  `pyproject.toml` allows one; setuptools and uv's own build backend accept it
  as is:

  ```toml
  [tool.hatch.metadata]
  allow-direct-references = true
  ```

- **git** has to be on the path wherever the dependency is resolved or
  installed, a Docker build stage included; a slim Python image has none. An
  uncached fetch clones the repository's whole history, so an image build
  without a persistent uv cache pays for that clone every time.

## Compatibility

The package is a contract, so services that exchange its values have to agree
on what they mean.

- **Pin the same revision on both sides of a key exchange.** An instrument key
  depends on the seed table as well as the code. A seed row that moves an
  existing symbol to another venue changes that symbol's key, which makes it a
  breaking change even though no code changed.
- **An unseeded symbol keeps the venue it is spelled with.** `SBUX` resolves to
  the US default venue (`SBUX.XNYS`) while `SBUX.XNAS` stays on XNAS, so one
  listing can carry two keys. Seed a symbol that producers may spell
  differently.
- **Parse a key from another service with `from_instrument_key`.**
  `to_canonical` reads an unknown last segment as part of the symbol, so a key
  minted on a venue that a newer pin added (`430047.BJSE` on a pin without
  BJSE) would resolve to a different key on the US calendar. `from_instrument_key`
  raises `ValueError` for it instead, which a service can answer with a 400.
- **Only `instrument_key` round-trips.** `to_canonical(ref.instrument_key)`
  returns the same instrument. A display spelling of a crypto or FX pair
  (`BTC-USD`, `EUR-USD`) parses back only with the `asset_class` hint, and the
  key carries no asset class: an unseeded fund reparses from its key as an
  equity. Send the asset class alongside the key when the reader needs it.
- **Enums are closed.** A reader rejects a value it does not know, so every
  reader's pin has to move before any producer emits a new value.
- **Versions.** `models.SCHEMA_VERSION` versions the wire models and
  `routing.RULESET_VERSION` the routing file. `load_ruleset` reads the current
  version, migrates the older ones it knows, and returns `None` for anything
  else; a writer passes `strict=True` to get `ValueError` instead, for any file
  that is present but unusable. A new field in the routing file bumps
  `RULESET_VERSION`: a reader ignores fields it does not know, so an older
  writer would otherwise drop the field when it rewrites the file.

## Requirements

Python 3.11 or newer; `pydantic` 2, `pyyaml`, `pandas` and
`exchange-calendars` 4. The suite runs against the locked versions and against
the lowest versions `pyproject.toml` allows.

## Development

The package has its own environment and suite:

```bash
cd libs/market-protocol
uv run pytest
```

`make test-market-protocol` at the repository root does the same. Lint runs
with the repository's Ruff settings, through `make lint` at the root.
