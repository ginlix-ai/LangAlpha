---
name: 3-statements
description: "Build or repair an integrated three-statement model: linked IS, BS and CF, supporting schedules, scenarios and a Checks sheet. Triggers on three-statement model, build me a model, link the statements, populate this template, make my balance sheet balance."
---

# Three-Statement Model

An integrated model is two artifacts sharing one workbook: a faithful transcription of what the company reported, and a forecast that is a set of arguments. The seam between them is the most important line in the file, and most of the rules below exist to keep it visible.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first figure enters the workbook.

- Fiscal period labels, per-company LTM windows, and rate against level changes: `.agents/skills/research-conventions/references/market-data-rules.md`.
- A filing label does not match a model row, or a subtotal will not tie to its components: `.agents/skills/3-statements/references/line-items.md`.
- The template needs data pulled out of a 10-K or 10-Q: `.agents/skills/3-statements/references/sec-filings.md`.
- Writing a linkage, a roll-forward or a schedule formula: `.agents/skills/3-statements/references/formulas.md`.
- Formatting a sheet the user will read: `.agents/skills/3-statements/references/formatting.md`.
- Designing a scenario set or a sensitivity grid: the weak-design catalogue under *Step 10: Scenarios and Sensitivities* in `.agents/skills/dcf-model/SKILL.md` applies here unchanged.

## Critical Principles

**Formulas, not hardcoded values.** Every projection cell, roll-forward, linkage and subtotal is a live Excel formula, never a value computed in Python and written in. With openpyxl, `ws["D15"] = "=D14*(1+Assumptions!$B$5)"` is correct and `ws["D15"] = 12500.0` is not. The only cells holding typed numbers are historical actuals and the driver assumptions on the control panel. A hardcode does not just freeze one number: it breaks every downstream integrity check silently, because the check still evaluates and still reads OK.

**Build with openpyxl, recalc, audit.** Build as a saved Python script (for example `work/<task>/build_model.py`), following `.agents/skills/xlsx/SKILL.md`: blue font for inputs, black for formulas, green for cross-sheet links, a provenance comment on every input, an explicit number format on every computed cell. Then `python .agents/skills/xlsx/scripts/recalc.py <file> 30` until the status is "success", and `python .agents/skills/xlsx/scripts/audit.py <file> --strict` with every `fail` fixed. A saved script is what lets you edit one section and rerun cheaply.

**Present each stage as you finish it.** A PTC turn is not chat-interactive at every step, so this is not a blocking question: present the block, say what you are building next, and carry on unless the user objects. A mis-mapped tab or a wrong historical surfaces while it is still cheap to fix.

1. **The architecture or the template map** - which sheets exist, what each holds, where the inputs live
2. **Historicals** - the transcribed block, with periods and sources
3. **Drivers** - the control panel, each driver with its basis
4. **Income statement projections** - with the subtotals checked
5. **Balance sheet** - the balance check for every period
6. **Cash flow statement** - the cash tie-out for every period
7. **Checks sheet** - the whole sheet, with the roll-up cell reading OK
8. **Cover** - the workbook's first sheet, written last: tiles linking to the terminal-year revenue, EBITDA, net income, free cash flow and leverage, plus the scenario selector and the `Checks` roll-up

Do not populate the model end to end and present it complete.

## Step 1: Architecture, or the template's map

Building fresh, the sheet architecture is fixed by three rules:

- **Historicals sit on their own sheet.** Reported history is a transcription with its own source notes, and no forecast formula ever writes into it. The boundary period is visibly marked, in the column header and with a border, so a reader can see at a glance where the company stops and we start. This is the seam, and a model that blurs it cannot be reviewed.
- **One control panel.** Every scenario switch, toggle and global assumption lives on one sheet. Nothing is switched from inside a calculation sheet, because a toggle buried in the middle of the cash flow statement is a toggle nobody finds when the model gives a strange answer.
- **Calculation sheets hold formulas only.** IS, BS, CF and the supporting schedules read from the historicals sheet and the control panel and hold no typed numbers of their own.

Completing somebody else's template, map it first rather than imposing this shape. Templates vary:

| Common tab names | Holds |
|---|---|
| IS, P&L, Income Statement | income statement |
| BS, Balance Sheet | balance sheet |
| CF, CFS, Cash Flow | cash flow statement |
| WC, Working Capital | working capital schedule |
| DA, D&A, PP&E | depreciation schedule and fixed-asset roll-forward |
| Debt, Debt Schedule | debt schedule |
| NOL, Tax, DTA | net operating loss and deferred tax schedule |
| Assumptions, Inputs, Drivers | the control panel |
| Checks, Audit, Validation | the checks dashboard |

Map, in this order: which tabs exist and which of the above are missing; which tabs feed which; where the input cells are, by font colour and by reading the formulas; the row structure (title, units row, section headers, actual against estimate columns, period labels); the column order and whether it is consistent across tabs; and any defined names, which are usually the drivers and the key outputs and are the fastest way to find both.

Then edit the way `.agents/skills/xlsx/SKILL.md` says to edit somebody's workbook: load it twice, once for formulas and once with `data_only=True`, write only into input cells, match the existing units and sign conventions, preserve their formatting, and read their cell comments first because they are often instructions. Where a structural change is genuinely needed, a new line item or period, use `python .agents/skills/xlsx/scripts/insert.py` rather than openpyxl's own insert, which leaves every formula pointing at the cells that used to be there.

**Done when** every sheet the model needs is either present or planned, the input cells are identified rather than assumed, and the historical-to-forecast boundary is marked.

## Step 2: Stage the historicals, then lay them out

Extracted facts land first as rows, one fact per row, each carrying its own source, period, line-item label, unit and sign:

```
company | period | canonical line item | value | unit | currency | source | as-of | reported label
```

Only then pivot into the wide statement layout. Doing it in this order is what keeps provenance attached to the figure through the reshape: a number that arrives straight into a wide grid loses which filing it came from the moment it lands, and the provenance comment then gets written from memory. Keep the staged rows in the build script, or on a `Source Data` sheet, so the pivot can be rerun.

**Normalise at ingestion, not at use:**

- **Sign**: one convention for the whole model, applied as the fact is staged. Whether expenses are positive or negative matters less than that every row agrees.
- **Scale**: one scale for the model, recorded per source, because a single filing prints thousands in one table and millions in another.
- **Currency**: one reporting currency, with balance-sheet items at spot and flow items at the period average where translation is needed, and the convention stated on the sheet.

Map every reported label to a canonical line item as it is staged, per `.agents/skills/3-statements/references/line-items.md`, and keep the reported label in its own column so a reviewer can see what was renamed.

**When the history is not there.** A driver that cannot be anchored because the reported history does not exist, a segment the company stopped disclosing, a metric it never disclosed, a company with three quarters of public life, is stated as such. Produce the model with that driver labelled `assumption` and its basis named, re-read the readiness posture from the table in `.agents/skills/research-conventions/SKILL.md` against that input state, and say in the delivery which driver is unanchored and what would anchor it. A placeholder that looks like a transcribed actual is the one failure this step exists to prevent.

**Done when** every historical cell traces to a staged row with a source and an as-of, the canonical mapping is recorded, and any unanchored driver is labelled.

## Step 3: Set the drivers, each with a basis

A forecast is a set of arguments, and a driver with no stated basis is not an argument. Every driver on the control panel carries a basis, and the basis text is also what goes in the cell's provenance comment.

| Driver | Basis | As-of | Note |
|---|---|---|---|

`Basis` is one of a closed set: `management guidance`, `consensus`, `historical trend`, `bottom-up build`, `analyst judgement`. The mix is the tell: a model where every driver reads `historical trend` has no view in it, and one where every driver reads `analyst judgement` has no anchor under it.

**Reconcile against guidance and consensus.** For the near periods, show the model's forecast beside the company's guidance and beside consensus, with the delta and its reason:

```
FY26 revenue | our model | guidance range | consensus (N analysts, vintage) | delta to guidance | delta to consensus | why
```

A forecast that sits outside guidance without saying so is a view the model is hiding from its own reader. A forecast that matches consensus exactly is a view worth stating deliberately rather than arriving at by default.

**Working capital is anchored to a stated window.** Days-based assumptions (DSO, DIO, DPO) come from a named historical window, and the choice of window is justified: the last four quarters for a stable business, a full cycle for a seasonal one, the post-transition periods for a company that changed its terms. Setting them to the last reported period by default carries whatever was unusual about that quarter through the entire forecast.

**Capex and depreciation cohere.** The capex path and the depreciation schedule are one decision, not two. A growth capex ramp with flat depreciation is a defect, not a rounding issue: new assets depreciate. Model depreciation off the asset base the capex path builds, split maintenance from growth capex, and check that the implied asset life stays stable across the forecast. In the terminal year, capex and depreciation should be converging unless something specific says otherwise.

**Done when** every driver has a basis and an as-of, the guidance and consensus reconciliation exists for the near periods, and the working-capital window is named.

## Step 4: Build the statements and the schedules

Project the income statement from the drivers, then the balance sheet, then the cash flow statement, then close the loops. `.agents/skills/3-statements/references/formulas.md` carries the linkage and roll-forward formulas.

**The debt schedule is complete or it is not a schedule.** Model the maturity ladder tranche by tranche, mandatory amortisation, the revolver with its draw and repay mechanics, and the cash sweep if there is one. Interest expense is computed on the average balance across the period, and it reconciles to that balance. A single "total debt" line with an interest rate applied to it cannot answer the question the schedule exists for, which is whether the company can meet its maturities.

**Circularity.** Interest expense feeds net income, which feeds cash, which feeds the debt balance, which feeds interest expense. Enable iterative calculation (100 iterations, maximum change 0.001) and put a circuit breaker toggle on the control panel so the loop can be cut when it fails to converge. An unintentional circularity is a different thing: trace it and break it rather than switching iteration on to hide it.

**Quality checks per sheet**, run as the sheet is built rather than at the end:

- **Income statement**: historicals match the source; expense lines sum to the reported totals; every subtotal computes; the tax logic handles losses; forecast rows reference the control panel with no hardcodes; period-over-period changes are directionally sensible
- **Balance sheet**: assets equal liabilities plus equity in every period; cash matches the cash flow statement's ending cash; working-capital accounts tie to their schedule; retained earnings rolls forward; debt ties to the debt schedule; signs are right
- **Cash flow**: net income at the top of CFO matches the income statement; non-cash add-backs tie to their source schedules; working-capital changes carry the right sign (an increase in an asset is a use of cash); capex ties to the PP&E roll-forward; financing ties to the debt and equity movements on the balance sheet; ending cash matches the balance sheet and beginning cash matches the prior period
- **Supporting schedules**: opening balances equal prior closing balances; every roll-forward is complete (beginning + additions - deductions = ending); schedule totals tie to the statement line they feed; the assumptions used match the control panel

**NOL and deferred tax**, where the model carries one: the opening balance is zero for a new entity; the NOL grows only when pre-tax income is negative; utilisation is capped at 80 percent of pre-tax income before the NOL deduction under the post-2017 federal limitation; the balance never goes negative; the deferred tax asset ties to the balance sheet; and tax expense is zero when taxable income is at or below zero.

**Done when** every check above has been run for every period and each failure has been traced to the cell where it starts.

## Step 5: Scenarios

Scenarios are defined by a driver delta table on the control panel, not by three columns of typed numbers. Each case names the world it describes and states which drivers move and by how much against the base:

| Driver | Base | Upside delta | Downside delta | The world each describes |
|---|---|---|---|---|

Switch with `CHOOSE` or `INDEX`/`MATCH` off a single selector cell on the control panel. Drivers worth sensitising: revenue growth, gross margin, SG&A percent, DSO/DIO/DPO, capex percent, interest rate, tax rate.

The downside case is mechanical: a stated driver change with the arithmetic shown, never a percentage haircut to the base. Under it, test that the debt path still works, that covenants hold, and that the revolver capacity covers the trough, because a company that cannot fund its downside has a different downside from one that can.

Scenario audit: the toggle switches every statement, the balance sheet balances in all three cases, cash ties out in all three, and the ordering holds (upside above base above downside for net income, EBITDA, FCF and margins; the reverse for leverage).

**Done when** each case names its world, the toggle moves every dependent output, and the ordering holds in the Checks sheet.

## Step 6: The Checks sheet

Every model workbook carries a `Checks` sheet. The four-column layout, the verdict formula, the roll-up and the read-back after recalculation are the sheet contract under *Financial Model Conventions* in `.agents/skills/xlsx/SKILL.md`. What a three-statement model adds is the split below and the rows in it.

**Group 1: hard checks. Each holds exactly, and a failure means the model is wrong.**

| Check | Column B holds |
|---|---|
| Balance sheet balances | Total assets minus (total liabilities + total equity), for every period |
| Cash flow reconciles beginning to ending cash | Ending cash minus (beginning cash + CFO + CFI + CFF) |
| Ending cash ties to the balance sheet | CF ending cash minus BS cash |
| Net income ties | CF opening net income minus IS net income |
| Retained-earnings roll-forward | Prior RE + net income - dividends minus BS closing RE; SBC reaches equity through net income and APIC, never through this roll |
| Debt ties to the schedule | BS total debt minus the debt schedule closing balance |
| Interest ties to the schedule | IS interest expense minus the debt schedule interest on the average balance |
| D&A ties to PP&E | IS and CF D&A minus the PP&E schedule depreciation |
| Capex ties to the cash flow statement | PP&E schedule additions minus CF capex, sign adjusted |
| Working-capital change ties | CF change in NWC minus the movement in the BS working-capital accounts |
| Equity financing ties | Change in common stock and APIC on the BS minus equity issuance in CFF |
| Deferred tax asset ties | NOL schedule DTA minus the BS deferred tax asset |
| Units and currency consistent | Count of sheets whose units row differs from the control panel's; column C tests `=0` |
| Cash ties across the monthly and annual views | Monthly closing cash minus annual closing cash, where the model carries both |
| Year 0 equity ties | Equity raised in year 0 minus beginning equity capital in year 1 |
| Ending cash non-negative | `=MIN(<ending cash row>)`; column C tests `>=0`, since negative cash means the model has no financing plug |
| NOL balance non-negative | `=MIN(<NOL balance row>)`; column C tests `>=0` |
| NOL utilisation capped | Utilisation divided by pre-tax income before utilisation, which is the base the 80% limit caps against; column C tests `<=0.80` |
| Tax expense zero on a loss | Tax expense in every period where taxable income is at or below zero; column C tests `=0` |
| Scenario selector valid | The selector cell; column C tests membership, `=IF(OR(B14="Base",B14="Upside",B14="Downside"),"OK","FAIL")` |
| Scenario directionality | The three cases' EBITDA at the last projection year; column C tests the whole ordering, `=IF(AND(Downside<Base,Base<Upside),"OK","FAIL")` |

**Group 2: diagnostics. Each carries a band, and a breach is a question about the forecast rather than a defect.**

| Check | Column B holds | Band |
|---|---|---|
| Capex and depreciation cohere | Terminal-year capex divided by terminal-year depreciation | inside roughly 0.8x to 1.5x unless the build says otherwise |
| Implied asset life stable | Max minus min of (net PP&E divided by depreciation) across the forecast | a drift of more than a couple of years is a schedule defect |
| Days ratios in range | The largest absolute change in DSO, DIO or DPO against the anchoring window | a move of more than a few days needs a stated reason |
| Margin drift | Terminal-year EBITDA margin minus the historical maximum | above it, the forecast is claiming a record and should say why |
| Leverage in bounds | Peak net debt to EBITDA across the forecast | against the covenant, where one is known |
| Interest coverage | Minimum EBITDA divided by interest expense | against the covenant, where one is known |

The two roll-up rows, the read-back after recalculation and what an open `WARN` owes the delivery are in the xlsx contract above.

Where a template already ships its own checks dashboard, add these rows to it and put the roll-up at the bottom of that sheet rather than creating a second one.

**When the roll-up reads FAIL:** go to the source tab that owns the failing row and fix it in the build script rather than in the check, then rerun the script.

**Done when** both groups are present and labelled and the roll-up reads OK from recalculated values.

## Step 7: Deliver

- Toggle through every scenario and confirm no hard check fails in any of them
- Resolve every `#REF!`, `#DIV/0!`, `#VALUE!` and `#NAME?`, or document the one that is expected and why
- Confirm no input cell still holds a placeholder
- Confirm the `Cover` is the first sheet and every tile on it is a formula pointing at the output it reports
- State the readiness posture from `.agents/skills/research-conventions/SKILL.md`, forced down by any unanchored driver, any unexplained `WARN`, or any figure past its freshness threshold, and name the driver responsible
- Save everything under `$WORK_DIR/work/{task}/`, and keep the build script beside the workbook so the model can be rebuilt

**Done when** `recalc.py` reports success, `audit.py --strict` reports no `fail`, the roll-up reads OK in every scenario, and the posture is stated with its reason.

## Optional Analysis Blocks

Build these when the user asks for them or the template has rows waiting for them, and skip them otherwise.

**Margins**, displayed directly below each profit line on the income statement: gross margin below gross profit, EBITDA margin below EBITDA, EBIT margin below EBIT, net margin below net income. Each is the profit line over revenue.

**Credit metrics**, on the balance sheet: total debt to EBITDA and net debt to EBITDA for leverage, EBITDA to interest expense for coverage, debt to total capital and debt to equity for structure, current and quick ratios for liquidity. Where covenants are known, add an explicit compliance row against each threshold and flag the result.

## Sign Conventions

| Statement | Item | Sign |
|---|---|---|
| CFO | D&A, SBC | positive (add-back) |
| CFO | increase in AR | negative (use of cash) |
| CFO | increase in AP | positive (source of cash) |
| CFI | capex | negative |
| CFF | debt issuance | positive |
| CFF | debt repayment | negative |
| CFF | dividends | negative |
