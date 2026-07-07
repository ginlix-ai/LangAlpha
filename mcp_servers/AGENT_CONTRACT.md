# Agent-Facing Market-Data Contract

The agent's only knowledge of these tools comes from their docstrings (parsed into
prompts and generated sandbox wrappers), and agent-authored code consumes the return
values at runtime. Both are therefore contracts. This document is the single
authority for the market-data MCP servers (`price_data`, `options`, `fundamentals`,
`macro`, `yf_*`). The `x_mcp_server` is exempt (its own conventions predate this and
are already machine-readable).

**Out of scope — direct LangChain tools.** `src/tools/market_data/tool.py` holds
`@tool` functions the agent invokes directly and whose result is a **markdown report
it reads**, not a JSON payload its code parses. They are not codegen'd, so none of the
envelope shape, the `Returns: dict:` block, or the no-`Example`/`Note` rule below apply
to them. Their docstring is the tool's prompt description and must stay a call-time
decision aid — **what it is, what to pass (the `Args:`), and optionally when NOT to
use it** (only a real constraint, e.g. a US-only tool → "US only, not for non-US
symbols"). No "for X use tool_Y" cross-references (they duplicate across every tool),
and no output-shape or formatting detail (no `Returns:` block, no markdown/table/
currency description). Do not "conform" them to the envelope standard.

## Success envelope

```python
{
    "symbol": "0700.HK",           # canonical display spelling; key is always `symbol`
    "interval": "1min",            # echoed when the tool takes one (canonical vocab)
    "currency": "HKD",             # ISO 4217 of the price fields, when known
    "timezone": "Asia/Hong_Kong",  # IANA tz of timestamp strings, when known
    "count": 3,                    # plain int — total records in `data`
    "data": [...],                 # THE record payload — always this key
    "source": "ginlix-data",       # provider that served the request
    # tool-specific echo keys (period, data_type, ...) may follow
}
```

Rules:

- **`data` is the one payload key** — never `rows`, `results`, or per-tool names.
  A list of records, or a dict of lists for multi-section payloads (e.g. all three
  financial statements); `count` is then the total across sections.
- **Time-ordered lists are ascending (oldest → newest)**, stated in the docstring.
- **Timestamps** are exchange-local strings `"YYYY-MM-DD"` or `"YYYY-MM-DD HH:MM:SS"`,
  self-described by the envelope `timezone`.
- **Prices are major units** of `currency` (pence/GBX converted to pounds, etc.).
- **Symbols** go through `src.market_protocol.symbology` at the boundary; the echoed
  `symbol` is the canonical display spelling, regardless of what the caller passed.
- **Interval vocab**: `1min|5min|15min|30min|1hour|4hour|1day|1week|1month`.
  Provider-native spellings (`1m`, `1wk`, `daily`, ...) are accepted as input aliases
  and normalized; native-only granularities a provider supports may pass through but
  must be documented in that tool's docstring.
- Vendor-native record payloads (FMP fundamentals/macro) may stay raw inside `data`,
  but the envelope around them is standard and the docstring names the key leaf
  fields and says the field names are vendor-native.

## Error envelope

```python
{"error": "<machine_code>", "detail": "<human-readable message>", ...context keys}
```

`error` is one of: `invalid_argument | unsupported_interval | not_found |
auth_failed | rate_limited | upstream_error | client_unavailable`. Raw exception
text never lands in `error`; a sanitized summary may go in `detail`. Context keys
(`symbol`, `supported`, `retry_after_seconds`, ...) are welcome.

Use the shared helpers in `_envelope.py` (`make_response`, `make_error`,
`normalize_interval`) rather than hand-rolling either shape.

## Docstring standard

Three sections, in order, moderate length (target ≤800 characters total):

```
<1-2 lines: what it does + when to use it.>
<0-2 lines: hard constraints — symbol formats, interval vocab, limits.>

Args:
    symbol: Ticker — US "AAPL", HK "0700.HK", A-share "600519.SS".
    interval: One of 1min|5min|15min|30min|1hour|4hour|1day.

Returns:
    dict: {symbol, interval, currency, timezone, count, data, source}.
    data: list of {date, open, high, low, close, volume}, ascending (oldest
    first); date is exchange-local "YYYY-MM-DD[ HH:MM:SS]"; prices in
    `currency` major units. On error: {error: <code>, detail}.
```

Codegen constraints behind the pattern (`src/ptc_agent/core/tool_generator.py`):

- The `Returns:` label is mandatory — `_extract_return_info` anchors on it; prose
  contracts are lost from the structured slot.
- The first `Returns:` line starts with `dict:` (or `list[dict]:`) so the generated
  wrapper gets a real return-type hint instead of `-> Any`.
- Ordering, timestamp format/timezone, currency semantics, and the error shape live
  *inside* the `Returns:` block — that is the text that survives extraction.
- No `Example:`/`Note:` sections — they terminate the Returns capture and rot.
- Any docstring change requires an `MCP_CLIENT_CODEGEN_VERSION` bump to reach warm
  sandboxes.

## Content pinning

The tuned wording is itself part of the contract. Every agent-facing docstring and
signature (the market-data MCP servers above **plus** the direct tools in
`src/tools/market_data/tool.py`) is snapshot-locked in
`tests/unit/mcp_servers/agent_docstring_lock.json` and enforced by
`tests/unit/mcp_servers/test_agent_contract.py::test_docstrings_match_tuned_lock`,
which runs in the default unit suite.

Any edit — rewording, adding examples, renaming or adding parameters — fails that
test with a diff against the locked text. Rules of engagement:

- **Never** rephrase or "improve" these docstrings as a side effect of other work.
  If the gate fires and you didn't mean to change a docstring, restore the locked
  wording.
- An **intentional** contract change must follow the standards in this document,
  be reviewed, and then regenerate the lock:

  ```bash
  uv run python scripts/utils/update_agent_docstring_lock.py
  ```

  The pin test itself is read-only by design (it can never self-heal); the
  script is the lock's only writer. Commit the regenerated lock together with
  the docstring change so the diff shows both sides.
