---
name: catalyst-calendar
description: "Dated events ranked by what they can change: earnings, regulatory decisions, flow events, macro releases, with prep owners and a weekly preview. Triggers on catalyst calendar, upcoming events, what is coming up, earnings calendar, event calendar, catalyst tracker, what should I prepare for."
---

# Catalyst Calendar

A calendar of dates is a list; a catalyst calendar is a **decision-pressure ranking**. An event earns its place because it can move the estimate path, the narrative, the multiple, position size, the downside case, liquidity, or the odds of another event. Proximity alone ranks nothing: a high-materiality readout in eleven weeks outranks a routine print on Thursday, and the routine print belongs in a hygiene block.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## Step 1: Scope the universe

Settle four things, taking the mandate and horizon from memory per `.agents/skills/research-conventions/references/intake.md` and asking only about what it leaves open:

- The universe: tickers, a portfolio or watchlist file, a sector, or an index.
- The horizon: the next two weeks, a month, a quarter, or through the next reporting season.
- Whether macro releases and policy dates are in scope, or only issuer events.
- Whether the output goes to the desk or outside it, which decides the externally clean rule in step 7.

With nothing supplied, ask for one of those four concrete inputs and say what the default calendar will contain once it arrives (a rolling ninety days, issuer events plus the macro releases that bind the sector).

Done when the universe resolves to a named list of issuers, the horizon has dates, and the macro decision is recorded and the destination is recorded as desk or external.

## Step 2: Gather events and classify by investment path

Pull live rather than recalling:

- Macro MCP: `get_earnings_calendar(from_date, to_date)` for every reporter in the window, and `get_economic_calendar(from_date, to_date)` for releases and policy dates.
- `get_company_overview` for the issuer's reporting history and consensus context.
- `WebSearch` and `WebFetch` for the events no calendar feed carries: regulatory dockets, trial readouts, conference agendas, lockup schedules, contract expiries.

Company, regulator and exchange sources override an aggregator. When two sources disagree on a date, keep both, mark the row's confidence at the lower level, and write the disagreement as a conflict-register row per `.agents/skills/research-conventions/references/evidence.md`.

### Event groups

**Earnings and financial**: quarterly results with session (pre-market, post-close), annual meeting, investor or capital-markets day, guidance updates, debt maturities and refinancings, dividend and buyback authorisations.

**Corporate**: product launches, regulatory decisions and their preceding milestones, contract renewals and expirations, M&A milestones (shareholder vote, regulatory clearance, close), management transitions, litigation dates.

**Industry**: conferences and which issuers present, trade shows, comment periods and rulings, recurring industry data (monthly sales, traffic, channel checks).

**Macro**: policy meetings, labour, inflation and growth releases, central bank decisions elsewhere, scheduled geopolitical dates.

**Index and passive flow**: index additions, deletions, rebalances and reconstitutions; lockup expiries; secondaries and block trades; convertible issuance and conversion; buyback blackout windows; float changes. Each carries the fields that make it assessable: expected flow in shares and in days of average daily volume, the holder base behind the supply, and the float change in percentage points. This group is mechanical supply and demand, so it is kept apart from anything fundamental in the write-up.

### Classify by the path, not the headline

The classification decides what the row must contain to be useful, so it happens as the event is captured. A merger date needs the spread, the implied probability and the break-price arithmetic; a spin needs a parts-based value and a forced-selling estimate; an activist date needs the vote arithmetic; a litigation date needs merits, timing, damages and the settlement path; a flow event needs flow against average daily volume. Per-class core questions, required outputs and the must-extract list from the primary document: `.agents/skills/catalyst-calendar/references/event-lenses.md`, read when a captured event falls outside the earnings and macro classes you already handle, and again when a `P1` row needs the action block of step 5 filled.

### Timing fields

Every row carries a date type and a date confidence, kept separate because a rumoured hard date and a company-guided window fail differently.

| Date type | Renders as |
|---|---|
| `hard` | a single date, with the session where it matters |
| `window` | its span, in the table, in the workbook and in any export |

| Confidence | Basis |
|---|---|
| `confirmed` | a primary source states it: the issuer, the regulator, the exchange, the docket |
| `guided` | the company said roughly when, without committing to a date |
| `expected` | a reliable aggregator carries it and the issuer has not confirmed |
| `inferred` | derived from the reporting cadence, a statutory clock or a contractual term |
| `rumoured` | reported by media or a market participant with no primary support |
| `undated` | thesis-critical, and no date can be established yet |

A `window` renders as its span everywhere it appears, including a calendar export, and the export carries only rows whose date type is `hard`. Presenting "second half" as one Tuesday manufactures a date the issuer never gave.

An `undated` row keeps the event: undated thesis-critical events go to a review table with what would date them (a statutory clock, a filing, a conference agenda), not to the cutting-room floor.

Source metadata per row: source name, retrieval date, the publication date of the date itself when it differs, and a last-checked timestamp.

Done when every captured event carries a category, a class, a date type, a date confidence and its source metadata, and every undated thesis-critical event sits in the review table.

## Step 3: Score materiality and actionability

Two independent one-to-five scores, combined by judgement rather than multiplied:

- **Materiality**: how much the event can move the estimate path, the narrative, the multiple, the downside case, position size, liquidity, or the probability of a later event. A 5 changes the thesis; a 1 changes nothing we track.
- **Actionability**: how much we can do about it. A 5 has prep time, a tradable expression and the inputs a decision needs; a 1 is a fact we learn afterwards.

Priority is `P1`, `P2` or `P3`, written with the sentence that justifies it. A high-materiality, low-actionability event is still `P1` when its outcome resets the model.

**Escalators.** Any one of these promotes an event above its base score, and the row names which fired:

1. It lands within a week and the prep is not done.
2. Date confidence is low while materiality is high.
3. It collides with another portfolio-critical event.
4. Expected volatility is high or the name trades thinly.
5. The market setup is one-sided (crowded positioning, a stretched multiple, consensus clustered).
6. The asymmetry is not reflected in consensus.
7. It can trigger follow-on catalysts.
8. Missing data is blocking a clean risk decision.

**Clustering.** Flag every date carrying events for two or more correlated positions, and flag the week where portfolio-level event risk concentrates. A cluster is its own catalyst.

Done when every row carries both scores, a priority with its one-line justification, the escalators that fired, and a clustering flag where a date is shared.

## Step 4: Build the calendar

Canonical fields per event, all of them in the workbook, the readable subset in the markdown view:

`event_id` (stable across refreshes), issuer and ticker, category and subcategory, event name, one-line description, date, date type, date confidence, session and time zone, source, retrieval date, date publication date, last checked, materiality, actionability, priority, escalators fired, expected outcome, what to watch for variance, prep required, prep owner, prep due date, follow-up date, decision implication, post-event action.

Markdown view:

| Date | Type / confidence | Issuer | Event | Class | Materiality | Actionability | Priority | Prep owner | Due | Decision it supports |
|---|---|---|---|---|---|---|---|---|---|---|

Routine dates that nothing hangs on group into a hygiene block underneath, listed but unranked.

Done when every row shows its date type and confidence beside the date, every `window` row shows a span, and every `P1` and `P2` row names the decision it supports.

## Step 5: Prep plan, per-event block and the weekly preview

Each `P1` and `P2` event gets a prep item: the work product, the owner, the due date, and the decision it supports. A prep item with no due date is a wish.

Per-event action block, written for every `P1`:

- **Setup and current debate**: what the market is arguing about going in.
- **Metrics that will matter**: the two or three numbers the reaction hangs on.
- **Expectation frame**: guidance, consensus with its vintage, the buy-side bogey where it is observable, the options-implied move with its session, and prior commentary.
- **Our variant view**: where we differ, and the evidence behind the difference.
- **Confirm and disconfirm thresholds**: the level on each side that changes something.
- **Prep plan**: the items above, with owners.
- **Post-event playbook**: what each outcome triggers, written before the event so the reaction is not composed under time pressure.

Weekly preview, the cadence artifact:

**This week**: each day, the event, why it matters for which names, consensus and our estimate where it is an earnings date, and the metric in focus.
**Next week**: the heads-up on what needs prep started now.
**Position implications**: what could move, what prep is outstanding, and the risk decision each binary event forces.

Horizon framing beyond the two-week view: the next thirty days carry high-materiality events, overdue prep and unresolved source conflicts; sixty days carry the events whose pre-work starts now; ninety days carry long-lead workstreams (a model rebuild, an expert call, a channel check).

Done when every `P1` and `P2` event has a named prep owner and a due date, every `P1` carries its action block including the post-event playbook, and the preview names next week's prep starts.

## Step 6: Refresh without destroying history

A refresh appends. Match incoming events on `event_id`, keep every prior row, and record what changed:

- A date that moved: keep the row, update the date, add the prior date and the source that moved it to the row's history.
- A row the sources no longer carry: mark it `stale` with the date it was last confirmed, rather than deleting it.
- A confidence that improved (`expected` to `confirmed`): update it and note the primary source that confirmed.
- A row that resolved: mark it complete, record the actual outcome against the expected outcome, and keep it. The archive of what happened against what was expected is what makes the next preview sharper.

Emit a dated change log with three counts and their rows: added, changed, flagged.

Done when the change log lists every row added, changed and flagged, and no prior row was deleted or overwritten in place.

## Step 7: Output

Save to `$WORK_DIR/work/{task}/`.

- Excel workbook with the canonical fields, sortable and filterable by priority, class and date. Formatting and conventions per `.agents/skills/xlsx/SKILL.md`, then `python .agents/skills/xlsx/scripts/recalc.py calendar.xlsx 60`.
- Weekly preview note in markdown.
- A single-event brief when the user asks about one date: timing with its type, confidence and source; the setup; the market expectation; what could surprise; confirming and disconfirming evidence; the prep; the post-event action; and the source caveats.

**Externally clean output.** A calendar leaving the desk carries dates, sources, confidence and public expectations. Our positions, cost basis, target prices, variant view and trade intent stay in the desk version, and the two are separate files rather than one file with hidden columns.

### Delivery checklist

Run this before handing the calendar over. Two checks are hard: they pass, or the calendar does not go out.

| Hard check | Fails when |
|---|---|
| File safety | a user file was overwritten rather than extended, or the desk version left the desk |
| Date honesty | a readout or approval window was rendered as one day, its endpoint is unstated, or a date is presented harder than its type and confidence support |

The rest are research checks, the errors this artifact actually makes. One still open is disclosed on its own line in the delivery note, with what it costs the calendar's coverage:

| Research check | Fails when |
|---|---|
| Date provenance | an earnings date came from a secondary source and no primary source confirms it |
| Flow completeness | a lockup or secondary appears with no share count, holder base or days of volume |
| Regulatory precision | filing, acceptance, advisory committee, decision and launch are used interchangeably |
| Macro relevance | a macro release is listed with no stated link to a name in the universe |
| Conference reality | a conference is listed without confirming which issuers present |
| Timestamped expectations | consensus or guidance appears without its vintage |
| Scheduled work | an event requires a model update and no prep item carries it |
| Priority sanity | an event with a direct thesis linkage sits at `P3` |
| Calendar hygiene | duplicates survive, time zones are mixed, or a cluster is unflagged |
| PM usefulness | a row says what happens without saying what could change |

Done when both hard checks pass, one posture from the ladder in `.agents/skills/research-conventions/SKILL.md` is stated near the top of the calendar, and every research check either passes or is named individually in the delivery note with its effect on coverage.
