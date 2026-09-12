---
name: earnings-preview
description: "Pre-print setup for a company about to report: the expectation bar, EPS-quality watch, call questions, scenarios and the reaction framework. Triggers on earnings preview, what to watch for [company] earnings, pre-earnings setup, preview Q[N]."
---

# Earnings Preview

Everything in this note locates the stock against one object: the **bar**, the level of results the market is already positioned for. Consensus is one input to the bar, not the bar itself. The preview says where the bar sits, what could clear or miss it, what to listen for on the call, and what the reaction is likely to be if the print lands each way.

After the print, route to `.agents/skills/earnings-analysis/SKILL.md`.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## Route

The full preview is the default. A short version is produced only when the user asks for one, and it is rebuilt at the lower depth rather than truncated (`.agents/skills/research-conventions/references/depth.md`): the freeze time, the source posture, the bar numbers, the top two debates, the must-watch call questions and the missing-evidence block all survive the rebuild.

**Missing inputs never shorten the note.** A section whose input is unavailable keeps its heading and carries a labelled gap saying what is missing and what would supply it. A short note reads as a thin setup; a full note with three labelled gaps reads as the truth.

## Correctness rules

Every number follows the shared market-data rules: `.agents/skills/research-conventions/references/market-data-rules.md` covers fiscal periods, per-company LTM, NTM as four quarterly estimates, one base date for comparative returns, shown arithmetic, rate against level changes, margin selection and date-stamping. Read it before the first pull.

Preview-specific, on top of those:

- **Freeze the clock.** Record one as-of moment for the whole set of time-sensitive inputs: consensus, whisper, options, price, positioning. State it at the top of the note. An input with no as-of is a gap, not an estimate.
- Suppress a percent change when the denominator is zero, negative or definitionally unstable, and show the absolute delta instead.
- Mark a year-over-year comparison non-comparable when the company changed the metric's definition and published no restated bridge.

## Step 1: Freeze the clock and map the period

Pull the setup: `get_company_overview` for consensus, earnings history and price targets; `get_daily_prices` for the price history and the event window; `get_sec_filing` for the prior quarter, with its transcript attached, to recover the guidance in force; `get_shares_float` and `get_short_data` for the positioning picture; `WebSearch` and `WebFetch` for news since that filing.

Pin the **period map** once and reuse it everywhere, taking each fiscal label from the earnings event name `get_company_overview` returns: the preview quarter, the prior quarter, the year-ago quarter and the two-years-ago quarter. Every table, chart, scenario and options reference uses those same four labels. Ask the user only when the preview quarter is genuinely ambiguous, for example when the company reports two quarters inside one month.

**Complete when** the four period labels are written down, the freeze-time as-of is recorded, and the earnings date is confirmed from the company or `get_earnings_calendar` rather than inferred.

## Step 2: Build the expectation bar

Five expectation sources, kept separate because they say different things. Collapsing them is how a preview ends up measuring the print against the wrong number.

| Source | What it is | Where it comes from |
|---|---|---|
| Company guide | the range management put in force, with its date and venue | prior release, prior transcript, any 8-K since |
| Published consensus | the mean estimate and the analyst count, with its vintage | `get_company_overview` |
| Whisper | the buy-side number, when it is genuinely sourced | see the whisper rules below |
| Our base | what we expect, built from the drivers | our own model or the driver work in this note |
| Last-reported baseline | the operating run-rate the company actually printed last quarter | prior release |

Close the table with one sentence stating where the bar sits and why: which of the five the stock is trading against, and how far the others sit from it.

**Whisper rules.** A whisper is never blended into consensus without showing the bridge. With strong support, carry it as a value with its provenance and a confidence label. With weak support, convert it to qualitative setup language rather than a number. With no external whisper at all, either derive an implied whisper from the guide midpoint plus this management's beat history and label it analyst-derived, or write "not provided" and leave the row empty.

**Complete when** all five rows are filled or carry an absence reason, and the bar sentence states the selected bar's value, source, vintage and basis, labelled `judgement` wherever the five rows leave competing bars in play.

## Step 3: Watch the EPS quality before the print

Headline EPS is a bad proxy for recurring earnings more often than a preview assumes, and the items that break it are knowable in advance.

| Item | Why it distorts | Pre-print risk | What resolves it after the print | Model line it hits |
|---|---|---|---|---|

Screen at least these: the effective tax rate against the guided or trailing rate, the diluted share count (buyback timing, issuance, convertible dilution), below-the-line and mark-to-market items, FX translation and remeasurement, disposals, impairments, restructuring and litigation.

Then check the basis: does published consensus measure the same thing the company reports? Where it does not, say so, and say which figure the bar actually rests on. The real bar is often revenue, operating income, segment profit or free cash flow rather than EPS, and a preview that assumes EPS measures the wrong quarter.

**Complete when** each screened item carries a pre-print risk, and the note states whether consensus and company bases match.

## Step 4: Bridge the guide to an implied bar

A guide is a starting point, not an expectation. Convert it using this management's own record.

- Beat frequency over the last eight quarters, against its own guide, per metric.
- Typical beat size, in currency for level metrics and basis points for rate metrics.
- Whether the conservatism is steady, widening or fading, and what changed if it moved.
- The limits behind those statistics: how many quarters the sample has, whether the management team or the guidance policy changed inside it.

**Complete when** the implied bar is stated as a number or range with the beat history that produced it, and the sample limits are named.

## Step 5: Choose the metrics that decide the quarter

Financial: revenue in total and by segment, EPS, gross and operating margin, free cash flow, and forward guidance against consensus. Rank them by which one moves the stock, not by which one leads the P&L.

Operational, by sector, as the default pack: software and internet (ARR, net revenue retention, remaining performance obligation, customer count), retail (comparable-store sales, traffic, basket, inventory), industrials (backlog, book-to-bill, price against volume), financials (net interest margin, credit quality, loan growth, fee income), healthcare (scripts, patient volumes, pipeline events), energy and materials (volumes, realised price, unit cost).

**Selection rule.** The issuer's own disclosure model beats the generic pack. Use the metrics this company guides on and gets asked about, and where an important sector metric is not disclosed, list it as a data request rather than dropping it silently.

Flag as material: a gap between guide, consensus and whisper wider than the sector norm; a metric breaking its trailing four-quarter slope; growth decelerating or margin compressing past a stated threshold in basis points. For a seasonally distorted metric or one with a distorted base year, show the two-year stacked growth rate beside the year-over-year rate.

**Complete when** the metric list is ranked, each entry carries the expectation it will be measured against, and undisclosed metrics appear as data requests.

## Step 6: Read through from everyone who already reported

| Name | Relationship | What they reported | Why it matters here | Read-through | Confidence |
|---|---|---|---|---|---|

Cover competitors, suppliers, customers and sector bellwethers that have printed since the subject's last report, plus the macro releases that bear on the quarter (`get_economic_calendar`, `get_economic_indicator`). A read-through with no named transmission channel is a coincidence, not a signal, so state the channel or drop the row.

**Complete when** every row names the channel and carries a confidence label, and the table says which read-throughs are already in consensus.

## Step 7: Scenarios and the reaction framework

Three scenarios against the bar, not against last year:

| Scenario | Revenue | EPS | Key driver | Management tell | Falsifier | Likely reaction |
|---|---|---|---|---|---|---|
| Bull | | | | | | |
| Base | | | | | | |
| Bear | | | | | | |

Calibrate the reaction column on history rather than intuition:

| Quarter | Result against the bar | Next-day move | What drove the move | Continued or reversed |
|---|---|---|---|---|

Then state what is already priced, and the two asymmetries that matter: what would make a weak print buyable, and what would make a strong print fadeable.

**Options tenor caveat.** Pull the chain with `get_options_snapshot` or `get_options_chain`, in the same session the note is written, and record the expiry beside the implied move. An options-implied move is an earnings hurdle only when the expiry brackets the event tightly. When the nearest expiry sits well past the print, relabel the figure as expiry-tenor volatility context, keep it out of the headline tiles, and say what it does and does not measure.

**Complete when** each scenario names its driver and its falsifier, the reaction column is grounded in the historical table, and any implied move carries its expiry and tenor.

## Step 8: Write the call questions

Three or four **must-watch** items, ranked, and nothing else at the top. A list of eight equally weighted questions is not a plan. Everything else goes into an overflow bank below them.

Each must-watch question carries four things:

1. Why it matters, in one clause tied to the bar or a thesis pillar.
2. The answer that validates the view.
3. The answer that breaks it.
4. The specific phrases to listen for, including the hedges that signal an answer is being avoided.

Any material news since the last report that creates a contradiction or an open diligence item becomes a question here rather than a standalone news bullet. The overflow bank holds the sector add-ons and the second-tier items, and can run to eight or so.

**Complete when** the must-watch list is three or four items, each ranked with all four parts, and every open news item is either a question or explicitly closed.

## Step 9: Assemble and deliver

Save deliverables to `$WORK_DIR/work/{task}/`. A formatted document, when the user wants one, is built through `.agents/skills/docx/SKILL.md`. Sections, in order:

1. Company, quarter, earnings date, freeze-time as-of, readiness posture.
2. The expectation bar table and the bar sentence.
3. Metrics to watch, ranked, with the materiality flags.
4. EPS-quality watch and the guidance-credibility bridge.
5. Peer and macro read-throughs.
6. Scenarios, historical reactions, what is priced.
7. Must-watch call questions, then the overflow bank.
8. Trading setup: recent performance, positioning, and the implied move with its tenor caveat.
9. Missing evidence.
10. Position action.

**Salience first.** When a growth rate, an acceleration, a surprise percentage or a guide delta is what moves the stock, that is the headline number and the absolute figure is the supporting detail. Trend charts pick the margin the business is actually run on (operating, adjusted operating, EBITDA, contribution or free cash flow) rather than defaulting to net margin, and the note says which one and why.

**Missing evidence block.** Close with the exact refreshes required before taking event risk: each missing input, the tool or document that supplies it, and the conclusion it currently blocks.

**Position action.** One verb from the closed vocabulary in `.agents/skills/research-conventions/references/judgment.md`, gated by the inputs actually in hand. Without a sourced implied move, positioning context and adequate consensus or whisper evidence, the note delivers a setup and a reaction framework rather than a trade-ready instruction, and says which input is missing.

## Pre-delivery checks

- [ ] Period map explicit, and the same four labels used in every table and chart.
- [ ] Freeze-time as-of stated, and every time-sensitive figure inside its freshness threshold or marked aging.
- [ ] Every bar number carries source, as-of, unit and definition.
- [ ] Guidance, consensus and whisper appear as separate rows, never blended.
- [ ] EPS-quality section present whenever EPS is the bar, with the consensus-basis check stated.
- [ ] Rate figures in basis points, level figures in percent or currency, no percent on an unstable denominator.
- [ ] Chart axes agree with the units in the adjacent table.
- [ ] Each scenario states its driver and its falsifier.
- [ ] Implied move either brackets the event or is relabelled as tenor context.
- [ ] Must-watch questions capped at four and ranked.
- [ ] Missing-evidence block present, even when empty, and every gap labelled rather than dropped.
- [ ] Readiness posture stated, and it names the input holding it back when it is below decision-grade.
