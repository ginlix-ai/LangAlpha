---
name: idea-generation
description: "Find long and short candidates across a universe when no name is on the table yet: mandate, universe validation, archetype screens, thematic sweep, triage, idea cards, idea log. Triggers on idea generation, stock screen, find ideas, what looks interesting, screen for, new ideas, pitch me something."
---

# Idea Generation

A funnel, not a screen dump. A name that clears a filter is a **candidate**: it has earned the next hour of work, nothing more. The output is a short ranked list where each name says why it surfaced, what would have to be true, and what would kill it, plus the names that screened well and were rejected, which is where most of the learning sits.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## Step 1: Mandate and universe

Read the investor-mandate memory first per `.agents/skills/research-conventions/references/intake.md`, then ask only about the forks it leaves open. Record every slot, with a disclosed default where nothing fixes it:

| Slot | Default when unstated |
|---|---|
| Mandate and vehicle | long-only fundamental |
| Objective | absolute return over a multi-quarter horizon |
| Direction | long, plus shorts only where the mandate allows them |
| Horizon | two to eight quarters, which is what the screens below are calibrated for |
| Geography and listing | the user's home market, primary listings |
| Market-cap range | above the liquidity floor, otherwise unrestricted |
| Liquidity floor | a minimum daily traded value the position size could clear in a few days |
| Benchmark | the mandate's index, or none stated |
| Instruments and constraints | common equity; restricted lists, borrow availability and position limits taken from the user when they exist |
| Theme | none, unless the request names one |

Done when every slot carries a value or a default that the delivery message discloses.

## Step 2: Validate the universe before anything is ranked

Screen metrics lie on a universe nobody cleaned:

- Resolve identifiers to one traded line per company, choosing between an ADR, the local line and multiple share classes deliberately, and note which line the figures use.
- Drop delisted and stale listings; confirm the benchmark membership the mandate cares about; mark overlap with existing holdings so a "new idea" is not a position the user already owns.
- Flag names whose recent history breaks a screen: an IPO inside a year (no comparable base), a spin-off (pro-forma financials, no like-for-like history), a restatement, a fiscal-year change, and a transformative acquisition that resets every growth rate.

Done when every candidate resolves to one traded line with its liquidity measured, and each flagged name carries the flag that will distort its screen output.

## Step 3: Pick archetypes, then run the screens

Choose two to four archetypes that fit the mandate rather than ranking one composite score. An archetype names a specific way a mispricing happens, which is what makes the false-positive test possible: a composite rank has no characteristic failure mode, so nothing can be tested against it.

Long archetypes: quality compounder, growth at a reasonable price, revision inflection, derated quality, self-help margin expansion, capital-allocation catalyst, sum-of-the-parts, post-earnings overreaction.
Short archetypes: over-earning, deteriorating revisions, quality trap, balance sheet and refinancing, narrative excess, accounting and cash conversion.

The signal set for each archetype and, in the same entry, the ways that signal is characteristically wrong: `.agents/skills/idea-generation/references/screen-archetypes.md`, read before the first screen runs and again when a survivor is triaged.

### Data sourcing

- `screen_stocks` with filters (market cap, sector, price, volume, beta, dividend) for the initial candidate list.
- `get_company_overview` for the snapshot: ratios, earnings history, consensus, price targets.
- Fundamentals MCP: `get_financial_statements(symbol)` for income statement, balance sheet and cash flow; `get_insider_trades(symbol)` for insider transactions and buy/sell statistics; `get_shares_float(symbol)` for float and short interest; `get_technical_indicator(symbol, 'rsi')` and `get_technical_indicator(symbol, 'macd')` for the technical overlay.
- `WebSearch` and `WebFetch` for recent news and catalysts.

### Screen expressions

- **Quality compounder**: return on invested capital above the sector median; incremental returns at or above its own three-year average; earnings volatility below the sector median.
- **Growth at a reasonable price**: organic revenue growth above the sector median; forward price-to-earnings below its own five-year median; estimate revisions above its own prior-quarter rate.
- **Revision inflection**: ninety-day EPS revisions turning positive after its own prior ninety-day cuts; upward revision breadth above the sector median; price performance since the revisions below the sector median.
- **Derated quality**: forward multiple below its own five-year median; return on invested capital at or above its own three-year median; cash conversion at or above its own three-year median.
- **Self-help margin expansion**: operating margin below the sector median; a named cost or mix programme with a dated margin target above its own three-year median; realised savings as a share of target above its own prior programme.
- **Capital-allocation catalyst**: announced buyback or dividend yield above its own three-year median; net share retirement above its own prior four quarters; projected net debt below its own trailing three-year median.
- **Sum-of-the-parts**: enterprise value below after-tax, after-cost segment value at each segment's sector median multiple; discount to segment value above its own three-year median.
- **Post-earnings overreaction**: post-print price decline larger than its own median decline over eight prints; two-year-forward EPS cut smaller than its own median cut over eight prints; forward operating margin at or above its own three-year median.
- **Over-earning**: operating margin above its own cycle median; return on invested capital above the sector median; selling prices above its own cycle median.
- **Deteriorating revisions**: newest EPS estimates below its own prior-quarter estimates for two consecutive quarters; downward revision breadth above the sector median; price decline smaller than its own historical response to comparable estimate cuts.
- **Quality trap**: forward multiple above the sector median; retention below its own prior-year level for three consecutive periods; return on invested capital below its own three-year median.
- **Balance sheet and refinancing**: net debt to EBITDA above the sector median; debt due within the horizon divided by available liquidity above its own prior-year ratio; covenant headroom below its own prior-quarter level.
- **Narrative excess**: EV/revenue above the sector median; valuation-implied revenue growth above its own historical peak; reported revenue growth below its own three-year median.
- **Accounting and cash conversion**: like-for-like four-quarter cash conversion below its own three-year median; receivable days above its own same-quarter history; capitalised costs as a share of revenue above its own three-year median; non-GAAP adjustments as a share of earnings above its own three-year median.

The valuation base comes from the sector, never from one house multiple: banks price on book and returns on it, property on cash flow per share and asset value, semiconductors and other cyclicals on cycle-adjusted earnings or replacement value. The base per sector, and the screen traps that come with it: `.agents/skills/idea-generation/references/sector-overlays.md`, read whenever the candidate list crosses a sector boundary, before names are ranked against each other, and when a screen metric looks unusually good.

### Stale-data checks, run before ranking

Consensus that predates the latest print, guidance changed since the screen's inputs were built, a capital structure changed by an issuance, a buyback or an acquisition, prices from a prior session, and estimates mixed between calendarised and fiscal bases. Any of these invalidates a rank rather than a single cell. Freshness thresholds per data type: `.agents/skills/research-conventions/references/evidence.md`.

Done when each survivor names the archetype it came from, the screen values it cleared, the valuation base used for its sector, and the stale-data checks that were run.

## Step 4: Thematic sweep with an exposure gate

For a theme-driven request:

1. State the driver as a testable claim with a horizon ("data-centre power capacity constrains deployment through 2027"), not as a slogan.
2. Map the beneficiary pathway and group candidates by their position in the chain: supplier, enabler, direct operator, downstream adopter. That grouping is the organising axis of the output, since names in different links fail differently.
3. Separate pure-play exposure from diversified exposure, and quantify it: what share of revenue or profit is actually touched.
4. Ask what the current price already assumes about the theme, per `.agents/skills/research-conventions/references/judgment.md`.

**Exposure attribution gate.** A name advances on a sourced link from the driver to a financial line: orders, backlog, revenue, margin or estimate revisions. A theme-day rally, a mention in a management call, or a plausible narrative connection is not attribution. Without the link the name is marked `needs exposure attribution` and stays out of the ranked list until someone sources it.

Done when every thematic candidate carries either its sourced exposure link with the line item named, or the `needs exposure attribution` mark.

## Step 5: Triage each survivor

Answer these before a name is presented. The answers are the raw material for the idea card, so write them down as you go:

1. Why did it screen well, and is that reason economic or an artifact of the metric?
2. Is the denominator in the valuation credible (earnings that recur, an asset base that is real)?
3. What is the market debating about this name right now?
4. Do we have a variant view, or a well-known fact restated?
5. Is the risk compensated at this price?
6. Is it investable given liquidity, borrow and the mandate constraints?
7. What is the first smart reason a good investor rejects it?
8. Which false positive from the archetype entry applies here?
9. What is the next highest-value piece of work, and how long does it take?

**Crowding.** Reserve `crowded` for direct evidence: positioning data, ownership concentration, fund flows, short interest and days to cover. With price appreciation or narrative visibility alone, the accurate words are `expectations-heavy` or `valuation-gated`, and those are what the card says.

**Buckets.** Every survivor lands in exactly one:

| Bucket | Means |
|---|---|
| `immediate research candidate` | the work starts now, with a named next step |
| `watchlist pending trigger` | good setup, missing one thing; name the trigger and where it will show |
| `screen flag only` | the metric is interesting, the story is unproven; revisit next screen |
| `reject` | killed, with the reason, in the rejected-names section |

Done when every survivor answers all nine questions, carries a bucket, and uses `crowded` only where positioning evidence supports it.

## Step 6: Write the idea cards

**[Company] ([TICKER]) | [long/short candidate] | [archetype] | [bucket]**

| Metric | Value | vs. peers | As-of |
|---|---|---|---|
| Market cap | | | |
| Valuation base | | | |
| Second valuation cut | | | |
| Growth | | | |
| Profitability or returns | | | |
| Cash generation | | | |

Every row below market cap takes the sector's own base from `.agents/skills/idea-generation/references/sector-overlays.md`: EV/EBITDA and P/E on NTM with revenue growth and EBITDA margin for a generic industrial, price to tangible book with return on tangible equity for a bank, mid-cycle earnings power for a cyclical. A metric the sector is not priced on is left out rather than filled in.

Then, each in one or two sentences: why it surfaced (the screen and its values), the exposure proof where a theme is involved, the variant wedge (what we would believe that the price does not), why now (the dated reason this is a today problem rather than a someday problem), what must be true, what would invalidate it, the main false-positive risk from the archetype entry, the next highest-value work, and the routing.

**Register.** These are research candidates, so the language is `candidate`, `screen flag`, `watchlist`, `requires diligence`. Where a card ends in what to do, the verb comes from the gated vocabulary in `.agents/skills/research-conventions/references/judgment.md`, which for a screen output is normally `watchlist` or `wait for proof` with the missing input named.

Weak against strong, the difference being whether anyone could disagree with the sentence:

- Weak: "Attractive valuation with strong growth and a solid competitive position." Strong: "At 11x NTM earnings the market is pricing the 2024 margin trough as permanent; two of three cost programmes are already in the reported gross margin, and the third annualises next quarter."
- Weak: "Well positioned to benefit from AI adoption." Strong: "Two thirds of orders in the last two quarters came from a product line the market still models as flat, and backlog disclosure in the 10-Q shows the mix shift."
- Weak: "Momentum is fading and the multiple looks stretched." Strong: "Growth decelerated in each of the last three quarters while consensus holds next year flat; the 2026 estimate needs a reacceleration that no disclosed metric supports."

Done when every card carries all nine narrative fields, every metric row is one the sector is actually priced on, every figure carries its as-of, and no card contains an execution or position-sizing instruction.

## Step 7: Output, log and routing

Save to `$WORK_DIR/work/{task}/`.

- **A sharp list.** Five well-argued names beat twenty ranked ones, and the cut is part of the product. Say how many were screened, how many survived and why the list ends where it does.
- **Rejected names.** Each one: the name, why it screened well, and the specific reason it was killed. Anything killed for a fixable reason gets a trigger that would revive it.
- **Screen methodology.** The filters, thresholds and universe, so the run can be reproduced or challenged.
- **A comparison table** across the surviving names on the sector-appropriate base.

**Idea log**, appended per run so hit rate by archetype becomes reviewable: date, name, archetype, direction, rank, why surfaced, variant view, catalyst path, bucket, next work, owner, status, and, once known, the outcome and what the screen missed. The log is the only part of this skill that improves the next run.

**Portfolio-aware variant.** When the user supplies holdings: which candidates are additive, which duplicate exposure already held, which work as hedges or pairs against existing positions, what the list does to factor and sector exposure, and where position size runs into liquidity.

**Routing.** Each `immediate research candidate` names its next skill and the fields that travel with it: `.agents/skills/earnings-preview/SKILL.md` when a print is the next event, `.agents/skills/earnings-analysis/SKILL.md` when the trigger was a print already out, `.agents/skills/initiating-coverage/SKILL.md` for a full underwrite, `.agents/skills/comps-analysis/SKILL.md` or `.agents/skills/dcf-model/SKILL.md` when the debate is valuation, `.agents/skills/thesis-tracker/SKILL.md` once a view exists, and `.agents/skills/catalyst-calendar/SKILL.md` for the dated trigger a watchlist name waits on.

**User files.** A watchlist or portfolio file the user supplied is extended, never overwritten: write to a new file or a new tab, and add a data-quality flag column rather than editing source values.

Done when the list, the rejected names and the log are all present, one posture from the ladder in `.agents/skills/research-conventions/SKILL.md` is stated near the top, every routed name carries its handoff fields, and any user file supplied is unmodified.
