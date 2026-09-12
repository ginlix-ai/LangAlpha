---
name: check-model
description: "Audit a model somebody already built, and report on it without editing it: structure, formulas, integrity identities, source tie-out and reasonableness, ending in a routed issue log. Triggers on check my formulas, QA this spreadsheet, audit model, model review, something is off in my model, why does my balance sheet not balance."
---

# Model Checker

Two different things can be wrong with a model: the arithmetic, and the underwriting. A model whose formulas are flawless can still be built on a figure that contradicts the filing it cites, or on an estimate nobody refreshed after the last print. This skill tests both, and ends in a report a reader can act on rather than a list of cells.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first finding.

## Step 1: Ingest and scope the audit

- Accept the user's model (`.xlsx` or `.xlsm`) from `$WORK_DIR/work/{task}/`
- Identify the model type: DCF, LBO, merger, 3-statement, comps, returns, or custom
- Map the structure: which tabs exist, how they link, where inputs live against outputs

**Scope discipline.** Judge the model against the job it was built for. A standalone operating model with no valuation layer is not missing a DCF, and a quick screen is not a failed initiation. Where the model's purpose is unclear, state the scope you audited against at the top of the report and let the user correct it. A finding that amounts to "this is not the model I would have built" belongs in a scope note, not in the issue log, where it dilutes the findings that matter.

**Missing files are surfaced first.** An absent workbook, a source document the model cites and does not include, or a tab referenced by formulas and not present goes at the top of the report, not into a source appendix at the bottom. The reader's first question is whether you audited what they think you audited.

**Done when** the model type, the tab map, and the scope you are auditing against are written down, and any missing file is named.

## Step 2: Recalculate, then read the Checks sheet

**Recalculation honesty governs the whole audit.** Cached values pass checks on broken models. Before reading a single number, recalculate on a copy:

```bash
python .agents/skills/xlsx/scripts/recalc.py <copy of file> 60
```

Then reopen with `data_only=True`. Every statement in the report about what the model computes is a statement about recalculated values, and the report says so. Where recalculation was not possible, an external link that cannot be resolved, a macro-driven calculation, a timeout, the report says the values were read from cache and that a passing check proves nothing.

**If the workbook has a `Checks` sheet, read it first.** A model built to our conventions carries one: a row per tie-out, a live formula per row, and a roll-up cell. The roll-up tells you in one cell whether the model ties, and column D on each failing row names the two cells that disagree. Do not re-derive by hand what the sheet already tests. Then audit the checks themselves for coverage: is every linkage tested, or does the sheet only test the ones that were easy to write?

**Done when** recalculation succeeded and the `Checks` roll-up and every failing row were read from recalculated values, or the limit that stopped it is recorded, the conclusions that depend on it are marked unverified, and the posture is read from `.agents/skills/research-conventions/SKILL.md` against that state.

## Step 3: Structural and formula checks

**Tab and layout review**
- Are inputs clearly separated from calculations?
- Is there a consistent colour convention (blue input, black formula, green link)?
- Are there hidden tabs or rows that could hold overrides?
- Is the flow logical: assumptions, IS, BS, CF, valuation?

**Formula consistency**
- Hardcoded numbers inside formulas: `=A1*1.05` where the `1.05` belongs in a cell of its own, referenced
- A formula that breaks the pattern of its row: one cell in a period row computing something different from its neighbours is either a deliberate exception nobody documented or a typo
- Inconsistent formulas across a row or column that should be one formula dragged across
- Off-by-one ranges: a `SUM` or `AVERAGE` that starts one row late or stops one row early, so a line item is silently excluded or a header included
- `#REF!`, `#VALUE!`, `#N/A`, `#DIV/0!`
- Cells formatted as formulas that hold hardcoded values: a pasted-over formula looks right, evaluates fine, and stops moving
- Hidden rows, columns and tabs, which are where overrides and stale calculations survive a review

**What is mechanised:** `audit.py` covers the first two of these plus hidden sheets and the iterative-calculation flag. Run `python .agents/skills/xlsx/scripts/audit.py <file> --strict` first and read the findings named `formula_hardcode`, `formula_family`, `hidden_sheet` and `iterative_calc`, then spend your own reading on what the script cannot see: off-by-one ranges, pasted-over formulas, and whether the logic is right at all.

**Done when** `audit.py --strict` has been run and read, and each manual check listed in this step is recorded for every calculation sheet as clear, a finding, or not applicable with a reason.

## Step 4: Integrity checks

**Balance sheet**
- Total assets = total liabilities + equity, every period
- If imbalanced, quantify the gap and trace where it breaks
- Retained earnings roll forward: prior RE + net income - dividends = current RE
- Goodwill and intangibles flow from the acquisition assumptions, in an M&A model

**Cash flow**
- Ending cash from CF = cash on BS, every period
- CFO + CFI + CFF = change in cash
- D&A on CF matches D&A on IS
- Capex on CF matches the PP&E rollforward
- Working-capital changes on CF match the BS movements in AR, AP and inventory

**Income statement**
- Revenue builds tie to the segment or product detail
- COGS and gross margin consistent with the assumptions
- Tax expense = pre-tax income x tax rate, allowing for deferred tax
- Share count ties to the dilution schedule (options, converts, buybacks)

**Circular references**
- Interest expense to debt balance to cash to interest expense
- Intentional (common in LBO and 3-statement models): verify the iteration toggle works and a circuit breaker exists
- Unintentional: trace the loop and say how to break it

**Done when** every identity above has been evaluated for every period, and each failure carries the size of the gap and the cell where it starts.

## Step 5: Source tie-out ledger

Formula correctness and source correctness are different failures, and a model can pass every identity while contradicting the document it cites. This pass is a separate register from the issue log: one row per material input, traced back to where it claims to come from.

| Input | Model cell | Value in model | Claimed source | Value in source | As-of | Tie status |
|---|---|---|---|---|---|---|

`Tie status` is one of a closed set:

| Status | Means |
|---|---|
| `ties` | the model value equals the source value |
| `ties within tolerance` | the difference is a rounding or units artefact, with the tolerance stated |
| `does not tie` | the values differ materially; this is a `source-contradiction` finding |
| `source not provided` | the model asserts a source that is not in the file or reachable |
| `not verifiable` | the source exists and does not disclose the figure at this granularity; this is a segment split or an allocation, and it is an `unsupported-assumption` finding |

Material inputs are the ones the output moves with: revenue and margin drivers, share count, net debt, the discount rate and its components, the exit or terminal assumption, and every figure the model's own summary quotes.

**Staleness, judged per source type.** A filing figure and a consensus figure age at completely different rates, so one freshness rule for the whole model is wrong for most of it. Take the threshold for each data type from `.agents/skills/research-conventions/references/evidence.md`, record the as-of in the ledger row, and raise a `stale-forecast` finding where a figure is past its threshold and load-bearing. The common one: a model refreshed for price and not for the estimates the price is being compared against.

**Done when** every material input has a ledger row with a tie status and an as-of, and every status other than `ties` or `ties within tolerance` has a corresponding finding in the issue log.

## Step 6: Logic and reasonableness checks

**Reasonableness**
- Do growth rates make sense? Revenue growth above 100 percent with no explanation is a flag
- Are margins within sector norms? Flag the outliers
- Does terminal value dominate the DCF? Above 80 percent of EV is a yellow flag
- Are projections hockey-sticking? Does EBITDA compound to an absurd number by year 10?

**Edge cases**
- What happens at zero growth, or negative growth?
- Does the model break with negative EBITDA?
- Do leverage ratios go negative or exceed realistic bounds?
- Any divide-by-zero risk in an early period before revenue ramps?

**Cross-tab consistency**
- Do linked cells match their source? Copy-paste errors are common
- Are date headers consistent across tabs?
- Do units match: thousands, millions, actuals?

**Auditor stress is an illustration, never corrected output.** Any sensitivity you run to probe the model is your own calculation, presented and labelled as such: "at a 12 percent discount rate rather than the model's 9 percent, the implied value is X". It never appears as what the model says, and it never replaces a model number in the report. The distinction matters because the reader's next move may be to quote you.

**Static inspection is never sufficient.** No model is called decision-grade from reading it. The bar is all three: the recalculation in Step 2 succeeded, the tie-out ledger in Step 5 has no unresolved `does not tie` or `source not provided` row, and the identities in Step 4 hold for every period. Any one of those failing caps the report's posture, whatever the formulas look like.

**Done when** each reasonableness question has an answer against recalculated values, and every stress calculation in the report is labelled as the auditor's own.

## Step 7: Common Bugs by Model Type

**DCF**
- Discount rate applied to the wrong period (mid-year against end-of-year convention)
- Terminal value not discounted back correctly
- WACC using book values instead of market values
- FCF including interest expense when the stream is meant to be unlevered
- Tax shield double-counted

**LBO**
- Debt paydown not matching the cash sweep mechanics
- PIK interest not accruing to the debt balance
- Management rollover not reflected in returns
- Exit multiple applied to the wrong EBITDA (LTM against NTM)
- Fees and expenses not deducted from day-one equity

**Merger**
- Accretion and dilution using the wrong share count (pre- against post-deal)
- Synergies not phased in
- Purchase price allocation not balancing
- Foregone interest on cash not included
- Transaction fees missing from sources and uses

**3-statement**
- Working-capital changes with the wrong sign convention
- Depreciation not matching the PP&E schedule
- Debt maturity schedule not matching principal payments
- Dividends paid exceeding net income with no explanation

**Comps**
- An enterprise-value numerator over an equity-level metric
- Periods mismatched across the numerator and the denominator
- A peer's LTM window taken from a fixed calendar rather than its own reported quarters
- Statistics computed over a peer set that includes names marked as context only

**Done when** the catalogue for this model's type has been walked and each item is either clear or a finding.

## Step 8: Write the report

Save deliverables to `$WORK_DIR/work/{task}/`.

### Readiness posture, at the top

The report opens with one posture for the model, from the ladder in `.agents/skills/research-conventions/SKILL.md`, not with a count of issues by severity. A count tells the reader how much you found; the posture tells them whether they can use the model this afternoon. Any unresolved blocker forces it down regardless of how clean the rest is: an identity that does not hold, a `does not tie` row in the ledger, or a recalculation that could not be run. Name the specific finding responsible.

### Three registers, kept apart

- **The issue log**: findings about the model, below.
- **The tie-out ledger**: Step 5, about the sources.
- **The `Checks` sheet**: the model's own arithmetic register, which stays in the workbook and is not copied into the report.

Mixing them produces a list where a stale consensus figure and a broken `SUM` sit side by side at the same weight.

### The issue log

Every finding carries five fields, plus its type and severity:

| # | Location | Type | Severity | Evidence | Decision impact | Suggested fix | Owner |
|---|---|---|---|---|---|---|---|

- **Location**: sheet, cell or range. A finding without a cell is a comment
- **Evidence**: what establishes it. The cell's formula, the two values that disagree, the source page. Not "looks wrong"
- **Decision impact**: what it changes for someone acting on the model. "The equity value is overstated by 8 percent" is impact; "this is bad practice" is not
- **Suggested fix**: the specific change, not the direction of one
- **Owner**: who or which skill fixes it, per the routing table below

**Type**, which is what the problem is, from a closed set:

| Type | Marks |
|---|---|
| `mechanical` | a formula, reference, range or control defect |
| `source-contradiction` | the model disagrees with the source it cites |
| `unsupported-assumption` | an input with no evidence behind it, where the output moves with it |
| `stale-forecast` | a figure past the freshness threshold for its data type |
| `missing-output` | the model does not produce a decision output it was built to produce |
| `invalid-comparison` | a comparison that needs a bridge and does not have one: bases, periods, currencies, adjusted against reported |

**Severity**, which is how badly it breaks:

- **Critical**: the model produces a wrong output. The balance sheet does not balance, a formula is broken, an input contradicts its source materially
- **Warning**: the model works and carries risk. Hardcodes, inconsistent formulas, edge-case failures, an assumption with no support
- **Info**: style and convention. Colour coding, layout, naming
- **Question**: something only the author can settle before it can be classified at all. An unusual assumption that may be deliberate, a schedule that may be intentionally simplified. Keeping these separate stops an open question being reported as a defect, which is the fastest way to lose a reader's trust in the other findings

The two axes are independent: a `source-contradiction` can be Critical or Info depending on how far the number travels.

### Remediation routing

The audit ends in a work plan, so every finding's owner is a specific place the fix happens:

| The fix is | Owner |
|---|---|
| Rebuild or repair the operating model | `.agents/skills/3-statements/SKILL.md` |
| Redo or repair the valuation layer | `.agents/skills/dcf-model/SKILL.md` |
| Fix the peer set, the multiples or the statistics | `.agents/skills/comps-analysis/SKILL.md` |
| Refresh estimates, actuals or market data into the model | `.agents/skills/model-update/SKILL.md` |
| A judgement call about the underwriting | the model's author, phrased as a question |

Group the log by owner at the end of the report, so each owner's list is a task rather than a search.

**Done when** the report opens with a posture naming the finding that set it, every finding carries all five fields plus a type and a severity, and every finding has an owner.

## Important Notes

- **Balance first.** If the balance sheet does not balance, nothing downstream of it means anything; report it and resolve it before spending time elsewhere
- **Hardcoded overrides are the most common single defect.** Search for them harder than feels necessary
- **Sign conventions** on cash outflows are the second most common
- **A model that "works" can still be wrong.** Sanity-check outputs against sector benchmarks and against where the stock trades
- **VBA**: note any macro-driven calculation, which cannot be audited from formulas alone and blocks a decision-grade posture
- **Report, do not repair.** Findings and suggested fixes go to the user; the model changes when they ask for it. When they do ask, the edit runs under `.agents/skills/model-update/SKILL.md`, which keeps the original intact and logs every write

> For Excel formatting standards, the verification scripts and their flags, see `.agents/skills/xlsx/SKILL.md`.
