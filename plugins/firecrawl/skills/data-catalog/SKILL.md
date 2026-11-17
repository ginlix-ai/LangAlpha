---
name: data-catalog
description: "Structured data the market data servers don't carry, from a catalog of providers called in code: earnings call transcripts with speakers, segment revenue and KPIs, as-reported statements, analyst ratings and consensus, unusual options activity, 'why is it moving' notes, 10-K/10-Q item text, 13F/13D/Form 4 ownership, FedWatch rate probabilities, Commitments of Traders positioning, senators' trades, federal contracts, private funding rounds, executive podcast appearances, search interest, web traffic, prediction markets. Triggers on any of these, or when a request needs a dataset the built-in tools lack."
---

# Data catalog

A catalog of data providers reached through the `firecrawl` MCP server, in code. Each provider exposes typed tools ("capabilities"), each with a published contract and a price. Finding and reading contracts is free; running a capability is charged to the user.

Check the built-in market data servers first (prices, fundamentals, ownership, macro, options chains, Yahoo analyst data). Use the catalog for what they don't cover.

## Find, read, run

```python
from tools.firecrawl import alexandria_library_search, alexandria_library_read

# 1. Find a capability (free). The catalog is a read-only filesystem with one
#    contract per capability at /library/<category>/<provider>/<capability>.md.
found = alexandria_library_search(command='grep -ril "transcript" /library/companies | head -20')
print(found["stdout"])

# 2. Read its contract before calling (free): when to use it, inputs, output.
spec = alexandria_library_search(command="cat /library/companies/fiscal-ai/events/transcript.md")
print(spec["stdout"])

# 3. Run it (paid). Up to 10 calls in one request.
res = alexandria_library_read(objective="NVDA Q3 FY27 call: guidance and gross margin",
                              calls=[{"provider": "fiscal-ai", "capability": "events/transcript",
                                      "options": {...}}])
for item in res["data"]["alexandria"]:
    if item.get("error"):
        print(item["capability"], item["error"]["code"], item["error"]["message"])
    else:
        rows = item.get("data") or item.get("records")
```

- The path is the address: `/library/companies/fiscal-ai/events/transcript.md` is provider `fiscal-ai`, capability `events/transcript`. `ls /library` lists the categories; each category and provider folder has a `README.md`.
- Call only capabilities whose contract you read, with the inputs it marks required. Don't guess option names.
- One item in a batch can fail while the others succeed, so check each item's `error`.
- Load results into pandas and print only what the answer needs.
- `firecrawl_crawl` needs `limit` set to 100 pages or fewer.
- Once you know a tool's address, call it directly in later runs rather than finding it again.

## Providers that matter

Prices are Firecrawl credits per call, checked 2026-11-16. Contracts don't show a price, so for a provider not listed here, run one call and read its cost from the result before running more.

| Need | Provider | Credits |
|---|---|---|
| Transcripts with speakers, segment revenue and KPIs, as-reported and adjusted statements, fund letters, institutional and insider holders | `fiscal-ai` | 0-30 |
| Analyst ratings and consensus, options activity, why a stock is moving, M&A calendar, press releases | `benzinga` | 15-45 |
| 10-K/10-Q item text, 13F holdings, 13D/13G, Form 4, XBRL facts | `sec-gov` | 5 |
| FedWatch FOMC probabilities, SOFR | `cmegroup-com` | 5 |
| Commitments of Traders positioning | `cftc-gov`, `cftc` | 5, 1 |
| Senators' financial disclosures and trades | `efdsearch-senate-gov` | 5 |
| Federal contract awards by recipient | `usaspending-gov` | 5 |
| Private company funding rounds and investors | `fireround` | 5 |
| Executives on podcasts, speaker-labelled | `particle` | 5-50 |
| Search interest over time and by region | `firecrawl-trends` | 0-5 |
| Prediction market prices | `polymarket` | 0 |
| US economic series | `fred` | 0-1 |
| Website traffic and audience | `similarweb` | 110-550 |

Skip `finance-yahoo-com`: the Yahoo servers already cover it for free.

## Cost

- Each run's result ends with what its paid calls cost. Report it to the user when you used paid data.
- Before a call over 100 credits (Similarweb, for one), or a loop that would spend more than about 300 in total, tell the user the cost and ask first.
- If a call is refused with `credits_exhausted`, the user is out of credits: say so, don't retry.

## Errors

- `THIRD_PARTY_DATA_TERMS_REQUIRED`: the provider needs its terms accepted before use. Tell the user the provider is unavailable to them for now; don't retry it, and use another provider if one covers the need.
- A `tool_blocked` refusal means the tool is not offered here; use the tools listed in the import above.
