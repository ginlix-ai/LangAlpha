# Earnings Update Workflow

Deferred reference for `earnings-analysis`. Load the phase you are about to run. The evidence contract (findable citations, the source ladder, call-only guidance, non-GAAP pairing, the four absence words) is in `SKILL.md` and binds every step here.

Completion criteria for each phase are in SKILL.md.

## Phase 1: Freshness gate and source assembly

### Step 1: Fix the period

Training data is old, and reporting on last year's quarter is the failure this phase exists to stop. Establish the period from documents, never from memory.

1. Write today's date down explicitly.
2. Find the most recent release: `get_company_overview` for the earnings history and the event name, then `WebSearch` for the company's latest results, then the investor relations newsroom sorted by date.
3. Read the release date off the document itself and compare it to today. Inside 90 days, proceed. Older, search again with different terms; the latest quarter has not been found yet.
4. Take the fiscal label verbatim from the event name, per `.agents/skills/research-conventions/references/market-data-rules.md`. A February print from a January year-end is Q4 FY2026.

Recognise the naming variants while searching: "Q1 2024", "Q1 FY24", "1Q24", "First Quarter Fiscal 2024" are the same event, and the legal entity name in EDGAR often differs from the common name, so search filings by ticker.

**Red flags, each of which sends you back to step 2**: the release is more than 90 days old; the exact release date cannot be stated; the transcript period differs from the release period; two artifacts name different quarters; the quarter was inferred from today's date rather than read off a document.

### Step 2: Assemble the artifacts

Collect in ladder order, and record for each one its type, its date and the location pointers you will cite.

| Artifact | Where | Notes |
|---|---|---|
| 10-Q or 10-K | `get_sec_filing`, which auto-attaches the earnings call transcript | Q4 reports land in the 10-K; the filing often trails the release by one to five days |
| 8-K exhibit and press release | `get_sec_filing`, or the investor relations newsroom via `WebFetch` | carries the reported figures before the 10-Q exists |
| Transcript | attached to `get_sec_filing`, otherwise `WebSearch` for the call transcript | verify its date matches the release date before using a single line of it |
| Investor deck and supplemental data | investor relations site via `WebFetch` | slide numbers are valid location pointers |
| Consensus, price targets, earnings history | `get_company_overview` | consensus must be the pre-print vintage; record its as-of |
| Prior guidance | the previous quarter's release and transcript | needed for the guidance bridge in step 8 |
| Historical financials | `get_financial_statements`, `get_growth_metrics` | for the eight-to-twelve-quarter trend charts |

## Phase 2: Extraction and beat/miss

### Step 3: Build the results table

One table, reported against expected, with our estimate and consensus as separate columns because they answer different questions.

```
                    Reported    Our Est    Consensus    Variance
Revenue             $X,XXX      $X,XXX     $X,XXX       +$XX (+X%)
Gross margin        XX.X%       XX.X%      XX.X%        +XXbp
EBITDA              $XXX        $XXX       $XXX         +$XX (+X%)
Operating income    $XXX        $XXX       $XXX         +$XX (+X%)
EPS (adjusted)      $X.XX       $X.XX      $X.XX        +$X.XX
EPS (GAAP)          $X.XX       $X.XX      $X.XX        +$X.XX
[Business metric]   XXX         XXX        XXX          +X% y/y
```

- Reported and constant-currency figures occupy separate columns and never mix inside one bridge. A constant-currency growth rate is labelled as such everywhere it appears.
- A metric the company stopped disclosing, or defines differently this quarter, is marked with the absence word or a definition-change note rather than compared silently.
- Adjusted lines carry their GAAP comparable, per the evidence contract.

### Step 4: Quantify the surprise

- Level metrics (revenue, income, cash flow) take a percent or a currency delta. Rate metrics (margins, tax rate, growth rates, retention) take basis points or percentage points. "Margin beat by 5%" is ambiguous in both directions; "margin beat by 40bp to 24.0%" is not.
- **Suppress the surprise percentage when the expectation is zero, near zero, or negative.** Show the absolute delta and say why the percentage is unavailable. A 900% EPS beat against a penny of expected earnings tells a reader nothing true.
- Decompose the variance where the disclosure supports it: by segment, geography, product and channel. Each line of the decomposition ties back to the total, and the arithmetic is shown per `.agents/skills/research-conventions/references/market-data-rules.md`.

## Phase 3: Quality and drivers

### Step 5: EPS-quality screen

Run this on every post-print, before any conclusion about the beat. Headline EPS is only as good as the items behind it.

Screen the trigger list. Any one of these fires the full bridge:

- The EPS surprise is materially larger than the revenue, operating-income or free-cash-flow surprise.
- Below-the-line movement: interest income or expense, other income, mark-to-market on investments, FX remeasurement.
- The effective tax rate sits away from the guided or trailing rate.
- The diluted share count moved on buyback timing, issuance, or convertible and option dilution.
- A disposal, an impairment, a restructuring charge, or a litigation settlement lands in the period.
- The consensus basis is unclear, or consensus and the company report on different bases.
- Management steers attention to an adjusted figure that is new, newly defined, or newly excludes a recurring cost.
- The stock reaction looks like it is capitalising a beat that cannot repeat.

When a trigger fires, build the recurring-EPS bridge (shape in `.agents/skills/earnings-analysis/references/report-structure.md`) and carry its conclusion into the estimate revision. When none fires, write the one-line conclusion "no material trigger identified" into the profitability section, so a reader knows the screen ran.

### Step 6: Driver isolation

Name two to three load-bearing drivers, not every line item. A driver is load-bearing when removing it changes the quarter's conclusion. For each: what moved, why it moved, and what it does to forward expectations.

Decompose into volume, price and mix only when the source inputs for all three exist. When they do not, keep the causal account qualitative and name the missing input, rather than implying a decomposition the disclosure cannot support.

### Step 7: Cash-quality module

Earnings quality shows up in cash before it shows up in the P&L. Pull operating cash flow, capex and free cash flow from `get_financial_statements`, then check:

- Operating cash flow against net income for the same period, and the size of the gap against prior quarters.
- The working-capital swing, by component: receivables, inventory, payables, deferred revenue.
- Any pull-forward or deferral language in the release or on the call (channel loading, prepayments, extended terms, a shifted fiscal week).
- Capex against depreciation, and free cash flow against the guided figure.

Say plainly whether cash confirms or contradicts the earnings result, and carry a contradiction into the debate map as a falsifier of the bull's earnings-quality claim.

### Step 8: Guidance read

When guidance was given, bridge it: new against prior, new against consensus, and the implied quarterly path if only an annual figure was given (show the arithmetic). Assess credibility from this management's own history of beating or missing its guide, and say whether the conservatism is steady or fading. Label anything that exists only in call commentary as call-only guidance.

When guidance was withheld, state that explicitly, note whether the company has guided before, and supply our own outlook built from the results and the commentary.

### Step 9: Transcript Q&A map

The call is where the debate is visible. Map every material exchange:

| Topic | Questioner and firm | Answering executive | Section | Quote or paraphrase | Why it matters | Bull implication | Bear implication | Falsifier or next check |
|---|---|---|---|---|---|---|---|---|

- Section is prepared remarks or Q&A; the two carry different weight, since prepared remarks are scripted and Q&A is not.
- Quotes keep enough context to preserve the meaning, and carry a transcript line range with the speaker.
- Repeated pressure on one topic is itself the signal: when three analysts ask the same question, that is the quarter's debate, and it belongs at the top of the map.

**When no transcript exists**, keep the section, name the missing artifact and its expected availability, and say which questions it would answer. An empty table, or a shorter report, hides the gap instead of showing it.

### Step 10: Debate map

Close the phase by writing the two cases against each other:

| Side | The case | What changed this quarter | Falsifier | Catalyst that settles it |
|---|---|---|---|---|
| Bull | | | | |
| Bear | | | | |

Each falsifier is a named observable with a threshold, and each catalyst is a real scheduled event or reporting cadence, never an invented date. The monitored-item standard in `.agents/skills/research-conventions/references/judgment.md` applies to both rows.

## Phase 4: Estimates, valuation and model update

### Step 11: Revise estimates

Update the current year, the next year, and the year after where the model carries it. Show old, new, change and a one-clause reason per line:

```
                        Old Est     New Est     Change      Reason
FY2024E Revenue         $XX.XB      $XX.XB      +X.X%       [one clause]
FY2024E EBITDA          $X.XB       $X.XB       +X.X%       [one clause]
FY2024E EPS             $X.XX       $X.XX       +X.X%       [one clause]
```

An EPS revision that rests on a non-recurring item identified in step 5 says so in its reason column, so the revision is not read as an operating improvement.

### Step 12: Valuation and rating

Recompute the DCF on the revised cash flows and refresh the comparable multiples where peers have reported. Then decide the target explicitly: a material estimate change (roughly 5% or more) usually moves the price target, and a smaller change usually does not, but either way the report states the decision and its reason rather than leaving the reader to infer it.

Rating changes come from the same evidence: results and guidance materially better with the thesis intact argues for an upgrade, materially worse argues for a downgrade, and mixed usually maintains. Weigh the stock reaction, the valuation on the new estimates, and whether the risk and reward have become asymmetric.

### Step 13: Model update, packet or apply

Two modes, and the default is the packet.

**Packet (default).** Produce a driver-update packet rather than touching any workbook. One row per change:

| Driver | Period | Old value | New value | Source | Rationale | Confidence |
|---|---|---|---|---|---|---|

**Apply.** Only when the user supplied a model and asked for it to be updated. Write into a copy, never the original, and hand back the change log alongside it. The mechanics of writing into a user workbook belong to `.agents/skills/model-update/SKILL.md`.

Rank the diff by decision impact, not by how many cells moved:

1. Next-quarter revenue, EPS and free cash flow.
2. Full-year revenue, EPS and free cash flow.
3. Margin and operating-expense run-rates.
4. Working capital and capex.

Formatting, rounding and label changes are suppressed from the diff entirely.

## Phase 5: Charts, report and gates

### Step 14: Charts

Eight to twelve, built with matplotlib, all on quarterly trends and what changed:

1. Quarterly revenue progression, last eight to twelve quarters, actual against estimate, current quarter highlighted.
2. Quarterly EPS progression, adjusted and GAAP.
3. Quarterly margin trend, gross, operating and the margin the business is actually run on.
4. Revenue by segment or geography, with growth rates.
5. Key operating metrics over the same window.
6. Beat/miss decomposition, waterfall.
7. Estimate revisions, old against new.
8. Valuation, the multiple against its historical range.

Optional beyond that: peer comparison where peers have reported, guidance against consensus, cash-flow bridge, balance-sheet items where they are the story. Chart axes carry the same units as the table beside them.

### Step 15: Assemble the report

Page-by-page structure, the decision box, the recurring-EPS bridge, the Q&A map section and the debate map section are all in `.agents/skills/earnings-analysis/references/report-structure.md`. Build the DOCX through `.agents/skills/docx/SKILL.md`.

### Step 16: Gate and deliver

Run the three tiers in `.agents/skills/earnings-analysis/references/best-practices.md`: hard fails, the delivery checklist, then the judgement gate. Then deliver with a short message carrying the result, the takeaways, the estimate changes, the rating and target, and the update mode used in step 13.
