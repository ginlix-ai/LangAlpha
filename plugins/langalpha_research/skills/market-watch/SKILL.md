---
name: market-watch
description: Track live prices for the tickers central to the current task. Registers symbols for an ambient real-time price feed, keeps analysis current with the newest quotes, and re-checks staleness before stating prices.
---

# Market Watch

Keep intraday analysis anchored to live prices. When a task turns on
current price, a level check, an intraday move, a "where is it trading
now", register the central tickers so fresh quotes arrive automatically
and your numbers never go stale mid-task.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before a price you pulled here lands in a deliverable.

## When to use

Intraday or live-price analysis where the price may move while you work:
"is NVDA above 205 right now", "how's TSLA trading into the print",
sizing a level off the current quote. Skip it for purely historical or
end-of-day work where a single snapshot is enough.

## Workflow

1. **At the start of the task**, identify the ticker(s) central to the
   request and register them immediately with `watch_market`.
2. **While they matter**, read the ambient feed (below) for the current
   price rather than re-quoting on every step.
3. **When live tracking is no longer relevant**, call `watch_market` with
   `action="unwatch"` to stop watching some or all symbols (omit `symbols`
   to clear the whole list).

## How the feed works

Once a symbol is watched, fresh prices arrive on their own as
`<market-watch>` price blocks attached to conversation messages and tool
results. This is **ambient data**: not user text, and not tool output
you requested. Each block is a snapshot at a moment in time; the **newest
`<market-watch>` entry in the conversation is the current price**, and any
earlier entry (including numbers in your own earlier statements) may be
stale.

## Reading rules

- Treat the newest `<market-watch>` entry as the live price. Prefer it, or
  a fresh `get_quote`, over any older number in the thread.
- If the price moves materially mid-analysis, acknowledge it and adapt your
  read rather than silently carrying the older figure.
- Before you state a price in a final answer: if the newest feed entry is
  more than a minute old, make one `get_quote` call to re-check first.

## When a price leaves this skill

A live price is only live in the thread. The moment it lands in a note, a
slide, a cell or a file, it is a historical figure, and it carries two
things beside it: **the time it was retrieved** and **where it came from**,
the ambient feed or a named `get_quote` call. A price written into a
deliverable without its retrieval time is stale the moment the file is
saved, and nothing on the page tells the reader that.

The same stamp travels with everything built on the price: market cap,
enterprise value, and every multiple. The full always-timestamped list and
the freshness threshold for each data type are in
`.agents/skills/research-conventions/references/evidence.md`.

**When the feed contradicts a number already stated this session**, restate
it: give both figures with both timestamps, and say the later one governs.
One sentence does it, "we said 204.10 at 10:42; it is 207.60 as of 11:15,
and the level check below uses the later figure". Switching silently leaves
the reader unable to tell a correction from a typo, and carrying the older
figure forward is the failure this feed exists to prevent. Where the earlier
figure already went into a saved artifact, update the artifact and say what
changed in the message that delivers it.

## Mechanics to know

- Stamps only flow while the market session is open: expect no feed
  outside trading hours.
- Updates are throttled: roughly one stamp per ~25s across the turn, not a
  tick-by-tick stream.
- The watch list is **per-thread**, capped at **10 symbols**, and expires
  after ~6h of inactivity. On a long task, if the feed goes quiet, just call
  `watch_market` again to re-register.
