---
name: comps-analysis
description: "Comparable company analysis: peer set, operating metrics, valuation multiples, statistics and an implied value. Triggers on comps, trading comparables, how does it trade against peers, peer benchmarking, what multiple should it get."
---

# Comparable Company Analysis

A comps table is arithmetic anyone can do and data discipline almost nobody does. The multiples are the easy half: the work is in which companies belong in the set, whether each number measures the same thing over the same period, and how old it is. The steps below build the table in the order those questions have to be answered.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first figure enters the table.

Period labels, per-company LTM windows, NTM as four quarterly estimates, and one common base date for every comparative return are binding here: `.agents/skills/research-conventions/references/market-data-rules.md`, read before the first pull. Step 3 adds no rule of its own and says only which of those period cases a comps table selects.

## Step 1: Frame the table

Four questions decide what gets built. How many to ask, in what shape, and what to do when no answer comes back: `.agents/skills/research-conventions/references/intake.md`.

1. **Format**: their template, or ours
2. **Audience**: investment committee, board, quick reference, detailed memo
3. **The key question**: valuation, growth, competitive position, efficiency
4. **The context**: M&A, an investment decision, sector benchmarking, performance review

The answers change the table, not just its wrapper. A relative-value question needs multiples and quartiles; an efficiency question needs margins and turns and can drop the multiples entirely. Big-cap incumbents and emerging names in the same sector do not take the same metrics. User-provided examples and stated preferences outrank every default in this file.

**Done when** the question the table answers is written down in one sentence and the metric list follows from it.

## Step 2: Build the peer set, with roles

Every peer carries a role, and the roles are tiered so the headline statistic is not diluted by a name that was only ever context.

| Role | Tier | Belongs when |
|---|---|---|
| `core` | 1 | same business model, comparable economics and scale, and the same demand driver |
| `read-through` | 2 | partial overlap, kept because it prints one metric that informs the subject |
| `analogue` | 2 | different business, kept as a growth, margin or size analogue, and labelled as such |
| `excluded` | listed, not shown | a near-peer a reader would expect, with the reason it is out |

**Headline statistics come from tier 1 alone.** Tier 2 appears in the table for context, visibly separated, and never inside the median that drives the selected range. The excluded list is part of the deliverable: a reader who cannot see why the obvious comparable is missing assumes it was missed.

Five to ten names in tier 1 is the working range. Below three, the median is one company's opinion: the selected-range hierarchy in Step 8 falls through to a single named analogue and the output is screen-grade under Step 10. Above ten, the set has stopped being a peer group.

Exclude rather than stretch. The comparability tells: materially different business models labelled as comps, a pure-play sitting beside a conglomerate, a peer whose fiscal year end does not line up and has not been calendarised, negative EBITDA valued on an EBITDA multiple, and a peer whose revenue is recognised on a different basis (gross billings against net revenue).

**Done when** every name carries a role and a tier, every exclusion carries a reason, and tier 1 holds three or more names or the screen-grade posture in Step 10 applies.

## Step 3: Fix the basis before the first pull

Every rule here is cheap now and expensive to retrofit once the table is written.

**Reported or adjusted, decided once.** One basis for the whole table, applied to every peer, with the adjustments made to each name listed. Reported and adjusted are different metrics, and a table that mixes them compares two things that were never the same. Where a peer only discloses one of the two, either bridge it or mark the row not comparable and say why.

**Comparability, per peer and per line.** Three states: directly comparable, comparable after a stated adjustment, or not comparable with the reason. The state travels with the cell, not with the company: a peer can be directly comparable on revenue and not comparable on EBITDA.

**Denominator pairing.** The numerator and the denominator have to belong to the same claim on the business, and to the same period:

| Numerator | Pairs only with | Never with |
|---|---|---|
| Enterprise value | pre-financing metrics: revenue, EBITDA, EBIT, unlevered FCF | net income, EPS, book equity |
| Equity value or price | post-financing metrics: net income, EPS, equity FCF, book value | EBITDA, EBIT, revenue |

The period matches on both sides: an LTM numerator over an NTM denominator is a number with no meaning, however carefully it was computed.

**Sign, scale and currency at ingestion.** Normalise when the data enters the sheet, not when it is used: one reporting currency for the table, one scale (millions or billions, stated in the header), and one sign convention. Record what was applied per peer, because the alternative is a silent factor of 1,000 in a single row.

**FX.** Balance-sheet items convert at spot, flow items at the period average, and the table states which currency and which convention. A peer reporting in another currency and translated at spot for revenue overstates or understates growth by the currency move alone.

**Calendarisation.** The period rule has three cases, and a comps table takes the first for every LTM column:

- an LTM multiple takes each company's own latest four reported quarters, per peer, with the four quarters recorded beside the name. A peer that has reported one quarter further than the subject carries that quarter,
- a calendarised comparison is a separate column of its own, built only for the forward or fiscal-year estimates that need one calendar year: sum the quarters falling inside it and head the column `calendarised to CY20XX`. An LTM column relabelled as a calendar year is a different metric wearing the label,
- a historical analysis takes the window each figure was true for, carrying its own as-of.

State the method and the window on the table. A peer whose quarterly detail does not support a calendarised column is marked as reporting on its own year end and kept out of the tier 1 statistic for that column.

**Done when** the basis, the currency, the scale, the FX convention and the calendarisation window are written in the table's header block or its notes, and every peer has been normalised to them.

## Step 4: Pull the data, with an as-of on every row

- fundamentals MCP: `get_financial_statements(symbol, 'all', 'annual', 5)` for the statements, `get_financial_ratios(symbol)` for ratios, `get_growth_metrics(symbol)` for growth, `get_historical_valuation(symbol)` for where each name has traded
- `get_company_overview` for market cap, consensus, price targets and rating distribution
- `get_sec_filing` where the tool figure and the filing disagree, and the filing settles it

**Every row carries an as-of column**: the date of the reporting period behind its fundamentals, not the date you pulled them. Prices, market caps and every multiple built on them carry the retrieval date as well, in the header block, since they move daily.

The freshness threshold for each data type, the six staleness states and the conflict register are in `.agents/skills/research-conventions/references/evidence.md`. Reported financials there stay fresh until the company's next scheduled report; a peer that is past that date and has not filed is past its threshold rather than exempt, and the table below says what a comps row does about it rather than lifting the threshold:

| Age of the trailing period | Treatment |
|---|---|
| Within two quarters | usable |
| Two to four quarters | `aging`: usable for direction, with the reason beside the row saying why the peer has not reported |
| Beyond four quarters | not usable in a tier 1 statistic without a stated bridge: an interim update, a pre-announcement, or a calendarised partial year. Without the bridge the peer moves to tier 2 or out |

When two sources disagree on the same figure, name both, select by the source tiers in `evidence.md`, and disclose the discrepancy. Two figures averaged into a third that nobody reports is the failure this rule exists to prevent.

**Done when** every fundamentals cell has an as-of, every price-derived figure has a retrieval date, and no tier 1 row is beyond four quarters old without a bridge.

## Step 5: Share count and the enterprise value bridge

The dilution the market prices is not the basic share count, and the EV a table ships is often nobody's EV.

**Dilution protocol**, applied per peer and named on the table:

- **Options and warrants in the money**: treasury stock method. Shares issued on exercise, less the shares repurchasable with the proceeds at the current price. Out-of-the-money grants are excluded, and the strike distribution comes from the equity footnote
- **Convertibles in the money**: if-converted. Add the conversion shares, and for an earnings-based multiple add back the after-tax interest to the numerator. Out of the money, the instrument stays in debt
- **Unvested RSUs**: included at their gross count; there are no proceeds to net against
- Show the share count build rather than a single number, and name the method beside it

**The EV bridge, reconciled.** Compute both sides independently and compare:

```
Market cap  = price x diluted shares
Enterprise value = market cap + total debt + preferred + minority interest - cash and equivalents
```

Compare the computed market cap and enterprise value against the reference figures from `get_company_overview`. **A gap beyond 2 percent is a finding, not a rounding difference**: it usually means a different share count, a stale price, or a claim (leases, pensions, non-controlling interests) that one side counts and the other does not. Resolve it, or disclose the difference and its cause on the table. Both comparisons are rows on the `Checks` sheet.

**Done when** every peer's share count names its method, and the market cap and EV reconciliations are within 2 percent or carry a stated cause.

## Step 6: Choose the metrics

Start from the question, not from the list of everything computable.

| The question | Focus on | Drop |
|---|---|---|
| Which company is undervalued | EV/Revenue, EV/EBITDA, P/E, market cap | operating detail, growth breakdowns |
| Which is most efficient | gross margin, EBITDA margin, FCF margin, asset turnover | size metrics, absolute dollars |
| Which is growing fastest | revenue growth, EBITDA CAGR, customer or unit growth | margin and leverage metrics |
| Which generates the most cash | FCF, FCF margin, FCF conversion, capex intensity | EBITDA multiples, P/E |

**Core operating columns**: company, revenue, revenue growth, gross profit, gross margin, EBITDA, EBITDA margin.
**Core valuation columns**: company, market cap, enterprise value, EV/Revenue, EV/EBITDA, P/E.

Add by sector, and only what changes a conclusion:

| Sector | Must have | Skip |
|---|---|---|
| Software and SaaS | revenue growth, gross margin, Rule of 40 on FCF margin; optionally ARR, net dollar retention, CAC payback | asset turnover, inventory metrics |
| Manufacturing and industrials | EBITDA margin, asset turnover, capex/revenue; optionally ROA, inventory turns, backlog | Rule of 40 on FCF margin |
| Financial services | ROE, ROA, efficiency ratio, P/E; optionally net interest margin, reserves | gross margin, EBITDA, which are not meaningful for a bank |
| Retail and e-commerce | revenue growth, gross margin, inventory turnover; optionally same-store sales, GMV, take rate | heavy R&D or capex metrics |
| Healthcare | R&D/revenue, EBITDA margin, growth; optionally pipeline value, patent timeline | inventory-driven metrics |

**The 5-10 rule**: five operating metrics, five valuation metrics, ten columns. Past fifteen you are including noise. Include three to five multiples that matter for the sector rather than every multiple that computes.

**Done when** every column either appears in the metric list written down in Step 1 or is identification or provenance metadata (name, ticker, role and tier, as-of, source), and the count is at or under ten.

## Step 7: Build the workbook

**Formulas, not hardcoded values.** Every derived value (margin, multiple, statistic, implied value) is a live Excel formula referencing the input cells. A number computed in Python and pasted in is a defect even when the value is right today. With openpyxl, `cell.value = "=E7/C7"` is correct and `cell.value = 0.687` is not. The only typed numbers are the raw inputs (revenue, EBITDA, share price, share count, net debt), and every one carries a cell comment naming its source, its as-of, and any adjustment applied. For an assumption rather than a source, the comment carries the reasoning and what would replace it.

**Build, present, recalc, audit.** Build from a saved Python script using openpyxl, following `.agents/skills/xlsx/SKILL.md`. Then run `python .agents/skills/xlsx/scripts/recalc.py <file> 30` until the status is "success", and `python .agents/skills/xlsx/scripts/audit.py <file> --strict` and fix every `fail`.

**Three sheets**: `Cover` first, then the comps table, then `Checks`. The cover's tiles are formulas linking to the selected statistic, the implied enterprise value, the implied equity value and the per-share value, and it is written once those cells are locked.

Present each stage as you finish it rather than delivering the sheet complete. A PTC turn is not chat-interactive at every step, so this is not a blocking question: present the block, say what you are building next, and carry on unless the user objects. A bad peer or a mismatched period surfaces before the statistics and the implied value are built on top of it.

1. **Peer set and structure** - the names, their roles and tiers, why each is comparable, and the column layout
2. **Basis** - currency, scale, reported or adjusted, FX and calendarisation conventions
3. **Raw inputs** - the input block, with the period, as-of and source behind each number
4. **Share count and EV bridge** - the dilution build and both reconciliations
5. **Operating metrics** - the calculated margins and growth rates
6. **Valuation multiples** - the multiples, against the sanity ranges in Step 9
7. **Statistics and selected range** - the quartile block, the outlier decisions, the selected statistic
8. **Checks sheet** - the roll-up cell reading OK

**Header block (rows 1 to 3):**

```
Row 1: [ANALYSIS TITLE] - COMPARABLE COMPANY ANALYSIS
Row 2: [Company 1 (TICK1)] | [Company 2 (TICK2)] | [Company 3 (TICK3)]
Row 3: Prices as of [date] | Financials as of [period] | [USD millions] | [reported or adjusted] basis
```

**Ratios**: every one is `[something] / [revenue]` or `[something] / [something on this sheet]`. `Gross Margin (F7): =E7/C7`, `EBITDA Margin (H7): =G7/C7`, `Rule of 40 on FCF margin: =[growth %]+[FCF margin %]`, named that way on the sheet so it is never confused with the EBITDA-margin variant.

**Cross-reference rule**: valuation multiples reference the operating cells. Never input the same raw number twice. If revenue is in C7, EV/Revenue divides by C7.

**Done when** the workbook rebuilds from the saved script, `audit.py --strict` reports no `fail`, and no derived cell holds a typed number.

## Step 8: Statistics, outliers and the selected range

**The statistics block**, below one blank row, with no "SECTOR STATISTICS" or "VALUATION STATISTICS" header row:

```
Maximum:         =MAX(B7:B9)
75th Percentile: =QUARTILE(B7:B9,3)
Median:          =MEDIAN(B7:B9)
25th Percentile: =QUARTILE(B7:B9,1)
Minimum:         =MIN(B7:B9)
```

Statistics belong on comparable metrics: growth rates, margins, EPS, EV/Revenue, EV/EBITDA, P/E, dividend yield, beta. They do not belong on size metrics (revenue, EBITDA, net income, market cap, enterprise value), where the spread is the scale of the companies rather than a valuation signal.

Quartiles carry information a mean does not: the 75th percentile is what the market pays for the premium names in this set, the 25th is discount territory, and the gap between them is how much the set actually agrees.

**Outliers, by a stated rule.** A value is an outlier when it sits more than 1.5 interquartile ranges below the 25th percentile or above the 75th. Then decide, per outlier, and record the decision on the sheet:

- **trim** it from the statistic, with the row still visible in the table,
- **keep** it with a note explaining why it is real (a genuine premium, a distressed name, a different growth rate),
- **exclude** the peer entirely, which moves it to the excluded list in Step 2 with its reason.

Never silently drop a name: a table whose statistic quietly omits a peer cannot be reproduced by the reader.

**The selected range, by hierarchy.** Test the exceptional conditions first and take the first that holds, since the median is the default only where none of them do. Say which one applied:

1. The single closest analogue, named as such, when fewer than three tier 1 names survive. This is a screen-grade output under Step 10
2. The trimmed mean, when one recorded outlier is doing the distorting and the rest of the set is tight
3. The tier 1 interquartile band, when the set disagrees with itself by more than roughly half a turn
4. The tier 1 median, when three or more tier 1 names survive and none of the above holds

Apply the selected statistic to the subject's own metric, on the same basis and the same period, to get the implied value. The implied enterprise value bridges to equity value through the same claims as Step 5, in reverse.

**Done when** every outlier has a recorded decision, the selected statistic names which hierarchy rung it came from, and the implied value is a formula over the statistic and the subject's metric.

## Step 9: Sanity checks and the failure list

- **Margin ordering**: gross margin above EBITDA margin above net margin, always, by definition. A violation is a data error
- **Multiple ranges**: EV/Revenue usually 0.5x to 20x and wildly sector-dependent, EV/EBITDA usually 8x to 25x, P/E usually 10x to 50x against the growth rate. Outside those, find the reason before shipping the cell
- **Growth against multiple**: higher growth generally carries a higher multiple. A high-growth name at the bottom of the range is either a finding or an error, and it is worth knowing which
- **A P/E above 100x** with no hypergrowth story, and a negative-EBITDA company carrying an EBITDA multiple, are both cells that should not have been computed

The mistakes that ship most often: market cap and enterprise value mixed in one formula; different periods across a numerator and a denominator; a hardcoded number where a reference belongs; an input with no source comment; a peer whose fiscal year end was never calendarised; a mean where a median belongs; and data used past its threshold with no disclosure.

**Done when** every sanity check has been run against the built sheet and each violation has either a fix or a stated reason.

## Step 10: Output

**Posture, once, near the top.** The ladder is in `.agents/skills/research-conventions/SKILL.md`. Comps reaches `screen-grade` more often than any other deliverable in this plugin, and reaching it honestly beats the two failure modes on either side of it.

**The screen-grade fallback.** When the data will not support a decision-grade table, still emit the table. Label every unavailable cell as unavailable rather than leaving it blank or filling it with an estimate, name what is missing and what would supply it, and set the posture to `screen-grade`. A blank cell reads as zero to half of readers and as an oversight to the other half; a labelled one reads as what it is. Refusing to produce anything is the other failure: the peer set and the metrics that do exist are useful even when three cells are not.

**The handoff block**, a fixed set of fields for whoever consumes the table next, whether that is a model, a memo or a person:

```
As-of:                 prices [date], financials [period]
Peer set:              tier 1 names and roles; tier 2 names and roles; exclusions with reasons
Statistic used:        which rung of the selected-range hierarchy, and its value
Basis:                 reported or adjusted, currency, scale, calendarisation window
Denominator:           the subject metric the statistic was applied to, and its period
Implied EV:            value
Claims bridged:        debt, preferred, minority interest, cash, leases, pensions
Implied equity value:  value
Per-share value:       value, and the share count method behind it
Limitations:           what is stale, what is missing, what is not comparable
```

**Notes and methodology**, on the sheet: where each number came from and how it was verified; the definitions in use (which EBITDA build, which FCF formula, how LTM was computed per peer); how enterprise value was constructed and which claims are in it; and what a reader should take from the quartiles.

**Confidence, in one sentence**, with its reasons: how tight the tier 1 set is, how fresh the data is, and how much adjustment the table needed to make the peers comparable.

**Done when** the posture, the handoff block and the confidence sentence are all present, and no unavailable cell is blank.

## Checks Sheet (Required)

Every comps workbook carries a `Checks` sheet. The four-column layout, the verdict formula, the roll-up and the read-back after recalculation are the sheet contract under *Financial Model Conventions* in `.agents/skills/xlsx/SKILL.md`. The rows a comps table has to carry:

| Check | Column B holds | Verdict |
|---|---|---|
| Every peer has a ticker | `=COUNTA(<peer name block>)-COUNTA(<ticker block>)`; column C tests `=0` | FAIL |
| No duplicate peers | `=COUNTA(<ticker block>)-SUMPRODUCT(1/COUNTIF(<ticker block>,<ticker block>))`; column C tests `=0` | FAIL |
| Every peer has a role and a tier | `=COUNTA(<ticker block>)-COUNTA(<role block>)`; column C tests `=0` | FAIL |
| Every peer has an as-of | `=COUNTA(<ticker block>)-COUNT(<as-of block>)`; column C tests `=0` | FAIL |
| No tier 1 row is stale | `=MAX(<today> - <tier 1 as-of block>)`; column C tests against the four-quarter bound | WARN |
| Market cap reconciles | For each peer, the largest absolute percentage gap between the computed market cap and the reference market cap; column C tests `<0.02` | WARN |
| Enterprise value reconciles | Same, for enterprise value against its reference; column C tests `<0.02` | WARN |
| Diluted share count exceeds basic | `=MIN(<diluted block> - <basic block>)`; column C tests `>=0` | FAIL |
| Multiples equal EV or price over the denominator | For each multiple column, the largest absolute difference across the peer block between the multiple cell and enterprise value (or price) divided by its metric | FAIL |
| Median and mean rows are formulas over the tier 1 block | Median cell minus `=MEDIAN(<tier 1 block>)`, and mean cell minus `=AVERAGE(<tier 1 block>)`, one row each | FAIL |
| Target implied value ties to the chosen multiple | Implied value minus (the selected statistic times the target's metric) | FAIL |
| Implied equity value ties to the bridge | Implied equity value minus (implied EV - net debt - other claims) | FAIL |

The build script writes this sheet **last**, once the peer block and the statistics rows exist and their row positions are locked, so the check formulas point at final addresses. Links into another workbook are `audit.py`'s job, under `references_external`, so no row here counts them. A FAIL blocks delivery: fix the sheet, not the check. A WARN either gets a fix or gets its reason in column D and one sentence in the delivery saying why the table is right and the band is not.

## Output Checklist

- [ ] Every peer carries a role and a tier; exclusions listed with reasons; headline statistics from tier 1 only
- [ ] Every row carries an as-of; prices and multiples carry a retrieval date; no tier 1 row past four quarters without a bridge
- [ ] One basis, currency, scale, FX convention and calendarisation window, stated on the sheet
- [ ] Every numerator paired with a matching denominator on a matching period
- [ ] Share count method named per peer; market cap and EV both reconcile within 2 percent or carry a stated cause
- [ ] Outlier decisions recorded; the selected statistic names its hierarchy rung
- [ ] Formulas reference cells; every input has a source or assumption comment; hyperlinks where a filing supports one
- [ ] `Cover` is the first sheet, its tiles linking to the selected statistic, the implied EV, the implied equity value and the per-share value
- [ ] `recalc.py` reports "success"; `audit.py --strict` reports no `fail`; `Overall` reads OK and the `Diagnostics open` count is read back, both with `data_only=True`
- [ ] Sanity checks pass, or each violation carries a stated reason
- [ ] Posture, handoff block and confidence sentence present; unavailable cells labelled rather than blank
