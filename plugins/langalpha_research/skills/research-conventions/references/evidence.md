# Evidence and Source Discipline

Deferred reference for `research-conventions`. Load it before the first figure lands in a deliverable, when a number has no source, when two sources disagree, when a figure may be stale, or when deciding how densely to cite.

## Evidence labels

Seven labels, a closed set. Every load-bearing figure and claim carries exactly one, in a column, a footnote, or the source note for its section.

| Label | Marks | Carries with it |
|---|---|---|
| `fact` | reported in a primary document, or returned by a data tool for a period that has closed | the document or the tool call |
| `company claim` | the company's own statement about itself that nothing independent confirms: guidance, a market-share figure, an addressable-market number | the venue it was said in and the date |
| `street estimate` | a consensus or single-analyst figure | the vintage, and the estimator or the analyst count |
| `model-derived` | computed by us from labelled inputs, including a value solved outside the artifact because the artifact cannot hold the procedure | its inputs, and the formula if it is not obvious; a solved value also carries its target and what re-solves it |
| `judgement` | our view, not a measurement | the reason, in one clause |
| `assumption` | an input chosen without evidence because the work needs a number | its basis and where the answer is sensitive to it |
| `needs-source` | a figure the artifact wants and does not have | what was searched and what would supply it |

`needs-source` is a drafting label. It is the one label that is gone before delivery, resolved either by finding the source or by the unsupported-claim rule below.

## Source tiers by claim family

One global hierarchy is not enough: the best source for a short-interest figure is not the best source for a segment margin. Use the list for the family the claim belongs to, resolve conflicts down that list, and record which tier the figure actually came from.

### Company financials, segments and valuation

1. The filing itself via `get_sec_filing` (10-K, 10-Q, 8-K, proxy), including the transcript it attaches.
2. The fundamentals MCP tools (`get_financial_statements`, `get_financial_ratios`, `get_growth_metrics`, `get_historical_valuation`) for normalized series and the ratios built on them.
3. `get_company_overview` for consensus, earnings history, price targets and rating distribution.
4. The company's own investor materials, fetched with `WebFetch`, labelled `company claim`.
5. Third-party summaries via `WebSearch`, labelled with the outlet.

The filing beats the deck, the deck beats a news summary. When a tool figure and the filing disagree, the filing is the number and the tool figure is a normalization difference worth naming.

### Market, ownership, short interest and options

1. Live quotes from the ambient feed, per `.agents/skills/market-watch/SKILL.md`.
2. `get_daily_prices` and the price-data MCP tools for history, returns and volume.
3. `get_shares_float`, `get_insider_trades` and `get_short_data` for ownership and short interest, each read at its own publication lag.
4. `get_options_chain` and `get_options_snapshot` for implied moves, same session only.

A price quoted inside an article dates from the article. Use it as evidence of what was reported, and pull the level itself from a price tool.

### Macro and rates

1. `get_economic_indicator`, `get_treasury_rates` and `get_market_risk_premium` from the macro MCP tools.
2. `get_economic_calendar` for the release schedule and for what is still pending.
3. The issuing agency's own release via `WebFetch` when a series needs its definition, its revision status or its footnotes.
4. Commentary about a series, labelled as commentary and never as the series.

### Events and dated facts

1. The primary document: the filing, the agreement, the docket entry, the regulator's decision, the transcript.
2. The company or issuer announcement.
3. `get_earnings_calendar` and `get_economic_calendar` for scheduled dates.
4. Reported news, with the outlet, the time, and the date confidence stated as confirmed, scheduled, estimated or reported.

## Freshness

Every figure carries an **as-of**: the moment the figure is true as of, which is not the moment it was pulled. Record both when they differ, as a quarterly holding pulled today is as of the quarter end.

### Thresholds by data type

| Data | Fresh while | Refresh on |
|---|---|---|
| Intraday price, quote, index level, FX | under a minute during the session | any statement of a current level |
| Close, market cap, and every multiple built on price | the trading day it was pulled | the next open |
| Consensus estimates | seven days | the print, a pre-announcement, a guidance change |
| Reported financials | until the company's next scheduled report | a later filing or a restatement |
| Company guidance | until the next guidance event | any filing or call that revises it |
| Ownership and institutional holdings | the quarter it reports for | the next filing window |
| Short interest and borrow | until the next publication | the next settlement date |
| Options snapshot and implied move | the session | the next session |
| Macro series | until the next scheduled release | the release, and any revision to prior prints |
| Sector size and share research | twelve months, always stated with its vintage | a newer study, or a structural change in the market |
| News and event claims | the day | any development a later headline can carry |

### The six staleness states

Every figure sits in one of these, because a boolean would let a figure a minute past its threshold read the same as one two quarters past it.

- `live`: inside the threshold and verified this turn.
- `current`: inside the threshold, pulled earlier this session.
- `aging`: past the threshold, still usable for direction; say so beside the figure.
- `stale`: past the threshold and load-bearing; refresh it, or drop the claim it supports.
- `superseded`: a newer print, filing or revision exists; use the newer figure, and say the earlier one was superseded when a reader may have seen it.
- `unknown`: the as-of cannot be established. Treat it as `needs-source`.

## The conflict register

When two sources disagree on a figure that matters, write the register rather than settling it silently. Four steps: confirm both figures measure the same thing over the same period, apply the tier list for that family, record the row, and name what would settle it.

| Claim | Source A and value | Source B and value | Selected | Why | What settles it |
|---|---|---|---|---|---|

Two guardrails, because both failures are invisible in the finished artifact: a figure that is the average of two disagreeing sources is a third number nobody reports, and a silent pick leaves the reader unable to tell a resolved conflict from an unnoticed one. Show the selection instead.

When the sources sit at the same tier and nothing settles it, carry both values, label the claim `needs-source` while drafting, and when the claim is load-bearing re-select the posture from the ladder in `.agents/skills/research-conventions/SKILL.md` against the new input state (an unresolved load-bearing conflict reads `not-ready`).

## Evidence to language

Confidence in the prose tracks confidence in the evidence.

| Behind the claim | Write it as | Not yet available to you |
|---|---|---|
| Two or more top-tier sources agree | a flat statement, with the section source note | nothing withheld |
| One top-tier source | a flat statement naming the source in place | "confirmed", "multiple sources" |
| A company figure nothing independent confirms | "the company reports", "management guides to" | the figure stated as an outcome |
| A street estimate | "consensus of N analysts looks for", with the vintage | "will", "is expected to" with no one named |
| Our own model | "our model implies", inputs shown | "the fair value is", a point target with no range |
| Judgement | "we think", "our read is", with the reason | "clearly", "obviously", "the market is wrong" |
| An assumption | "assuming X, then Y", with the sensitivity | the figure carried into a headline unlabelled |
| Nothing | the claim does not appear | a hedged version of the claim |

Precision tracks evidence too: a figure that descends from an assumption is written to two significant figures at most, and a range beats a point wherever the range is what you actually know.

**Reader-facing labels.** The label taxonomy stays in the working table; a delivered document or slide prints the reader's word for it, one per label: `reported` for a `fact` (a filed figure, or a data-tool figure for a closed period); `management statement` for a `company claim`, or `company-defined` when the claim is a metric the company defines; `consensus of N analysts` with its vintage for a `street estimate`, or the named analyst where it is one; `our estimate` for `model-derived`, `derived` where the space is a slide; `our judgement` for `judgement`; `assumed` for an `assumption`. `needs-source` never prints: by delivery it is a source or the claim is gone.

## The unsupported-claim rule

A claim is load-bearing when removing it changes the conclusion. When a load-bearing claim has no support after a genuine search, take one of two exits:

- Remove the claim, and with it every conclusion resting on it. This is the default.
- Keep it labelled `assumption`, state what would confirm it, and re-select the posture from the ladder against the new input state: a conclusion resting on an assumption reads `not-ready`, or `screen-grade` when the artifact declares itself a thin pass.

Softening the wording is the exit that ships a wrong answer, because a hedge reads as knowledge to everyone downstream of it. Remove or downgrade instead.

## Writing around a gap

Three failure shapes, each with a sentence that is honest rather than careful.

- **Sparse sources.** Name the coverage you have against the coverage you wanted: "Two of the five peers disclose the metric; the table covers those two and marks the rest not disclosed."
- **Conflicting sources.** Give the register's selection in one sentence: "We use the 10-Q figure of X; the investor deck shows Y on a basis that also includes Z."
- **Unsupported precision.** Give the bound instead of the point: "Segment margin is not disclosed; the range consistent with the reported total is A to B."

## Citation density

- **Prose**: a source note per section, naming the tools and documents that section rests on, plus an inline citation on any claim a reader would challenge. A citation chip in every cell buries the ones that matter.
- **Workbooks**: a source note per input, which is the provenance comment on every blue input that `.agents/skills/xlsx/SKILL.md` already requires. One note for the sheet does not locate anything.
- **Slides**: one source line per data slide, placed so it survives the export.

Web claims keep the inline citation format your system prompt defines. Density is what this section governs, not format.

## Always timestamped

These carry their as-of every time they appear in a deliverable: price, market cap, enterprise value, every multiple built on price, index level, FX rate, yield or spread, short interest, borrow, an options snapshot or implied move, a consensus figure, an ownership stake, and any sentence containing "currently", "today" or "now".
