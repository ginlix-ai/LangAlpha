---
name: dcf-model
description: "Build a DCF valuation in Excel: FCF projections, WACC, terminal value, scenarios, sensitivity grids, reverse DCF. Triggers on build a DCF, what is it worth, intrinsic value, fair value, price target from cash flows."
---

# DCF Model Builder

Builds an institutional-quality DCF as a live Excel workbook: four sheets, three scenarios, three sensitivity grids, and a valuation a portfolio manager can argue with. A model that computes correctly and says nothing about the stock is half the job, so the construction rules and the judgement rules below are one document.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first figure enters the workbook.

- `recalc.py` reports errors, the case selector does not move the model, or the valuation looks wrong: read `.agents/skills/dcf-model/TROUBLESHOOTING.md`.
- The revenue build needs the drivers of a particular sector, or the company is a bank, an insurer, a miner or a REIT: read `.agents/skills/dcf-model/references/sector-drivers.md`.

## Tools

- **fundamentals MCP**: `get_financial_statements`, `get_financial_ratios`, `get_growth_metrics`, `get_historical_valuation`
- **macro MCP**: `get_treasury_rates`, `get_market_risk_premium`
- **`get_company_overview`**: share price, beta, diluted shares, analyst consensus, growth estimates
- Web search and fetch, and user-provided data, as supplements

## Step 0: Before you build

The question budget, the shape of each question and what to do when no answer comes back are in `.agents/skills/research-conventions/references/intake.md`.

The forks that usually matter here: whether the user has an existing model to extend rather than a new build, the horizon, and whether the base case should follow guidance, consensus, or your own view.

**Three anchors are required, not optional.** A DCF without them produces a number nobody can act on:

| Anchor | Why the model cannot proceed without it |
|---|---|
| Current share price, with its as-of | The output is a fair value; without spot there is no implied return and no stance |
| Diluted share count | Equity value divided by the wrong share count is wrong by exactly the dilution |
| Net debt, with the balance-sheet date | It is the whole bridge from enterprise value to equity value |

A required input that cannot be sourced is written into the cell as required-and-absent with a comment saying what was searched, never silently defaulted, and the readiness posture is re-read from the table in `.agents/skills/research-conventions/SKILL.md` against that input state. Say it in the delivery message too, not only in the workbook.

**Done when** the three anchors are in hand with as-of dates, or the missing one is labelled and the posture is set, and any question asked has either an answer or a disclosed default.

## Build Workflow

**Execution pattern**: build the DCF as a saved Python script (for example `work/<task_name>/build_dcf.py`) rather than inline `ExecuteCode`. Model building is iterative: you will debug formulas, tweak assumptions and rerun, and a saved script lets you `Edit` one section and rerun cheaply. Read `references/workbook-patterns.md` before writing the build script; it carries the row layouts and formula patterns. For all formatting, number formats and colour standards, follow `.agents/skills/xlsx/SKILL.md`.

**Formulas, not hardcoded values.** Every projection, margin, discount factor, present value and sensitivity cell is a live Excel formula. A number computed in Python and written into the cell is a defect even when the value is right today. With openpyxl, `ws["D20"] = "=D19*(1+$E$10)"` is correct and `ws["D20"] = 12500.0` is not. The only typed numbers a DCF should hold are historical actuals, the assumption drivers in the scenario blocks, current market data (share price, diluted shares, debt, cash), and a solved value under the one exception `.agents/skills/xlsx/SKILL.md` allows (the reverse DCF driver, Step 10). If you catch yourself computing a value in Python and writing the result, stop and write the formula instead. The model has to move when the user changes an assumption, and a hardcode breaks every downstream tie-out silently, because the check still evaluates and still reads OK.

**Comment as you build.** Every blue input carries its provenance comment as the value is created, with the real source in it, before you move to the next section. The format and the examples are in `references/workbook-patterns.md`.

**Present each stage as you finish it.**

A PTC turn is not chat-interactive at every step, so this is not a blocking question: present the block, say what you are about to build next, and carry on unless the user objects. The point is that a wrong margin assumption surfaces while it is cheap to fix, not after 75 sensitivity formulas have been wired to it.

1. **Inputs** - revenue, margins, diluted shares, net debt, beta, risk-free rate, each with its source
2. **Projections** - the projected top line and margin build, with the implied growth rates
3. **FCF schedule** - NOPAT through unlevered free cash flow for every projection year
4. **WACC** - the CAPM inputs, the capital structure weights, and the resulting rate
5. **Equity bridge** - enterprise value, net debt, equity value, implied price, implied return against spot
6. **Sensitivities** - the three grids

Do not build the model end to end and present it complete.

### Step 1: Data Retrieval and Validation

- **Financial statements**: fundamentals MCP `get_financial_statements(symbol, 'all', 'annual', 5)`
- **Ratios and metrics**: `get_financial_ratios(symbol)`; **growth rates**: `get_growth_metrics(symbol)`
- **Historical valuation**: `get_historical_valuation(symbol)`, which is also where the exit-multiple back-check gets its comparison
- **Risk-free rate**: macro MCP `get_treasury_rates()`, the 10Y
- **Equity risk premium**: macro MCP `get_market_risk_premium()`
- **Consensus, price, beta, shares**: `get_company_overview`
- **Web search and fetch**: current price, beta, debt and cash when a tool does not carry them

Validate before building: net debt against net cash, diluted shares against recent buybacks or issuance, historical margins against the business model, growth against sector norms, tax rate in a defensible range.

**Done when** every input the model needs exists with a source and an as-of, and the validation list above has been walked.

### Step 2: Historical Analysis (3-5 years)

Document revenue growth and its CAGR, margin progression (gross, EBIT, FCF), capital intensity (D&A and capex as a percent of revenue), working-capital efficiency (NWC change against revenue growth), and return metrics (ROIC, ROE).

```
Historical Metrics (LTM):
Revenue: $X million | Revenue growth: X% CAGR | Gross margin: X%
EBIT margin: X% | D&A % of revenue: X% | CapEx % of revenue: X% | FCF margin: X%
```

**Done when** each forecast driver you are about to set has a historical range beside it.

### Step 3: Build Revenue Projections

Start from the latest actual, apply a growth rate per year, and show both the dollar amount and the calculated growth percent. `Revenue(N) = Revenue(N-1) * (1 + growth)`, `Growth(N) = Revenue(N)/Revenue(N-1) - 1`.

Shape the path rather than typing a flat number: near-term growth reflects visibility, the middle years moderate toward the industry rate, and the final year approaches terminal growth. Where the sector has real drivers, build revenue over them instead of over a percentage: `.agents/skills/dcf-model/references/sector-drivers.md`.

Three scenarios, each a described world rather than three numbers:

```
Bear: conservative growth, margin compression or none, higher WACC, lower terminal growth, higher capex
Base: guidance or consensus growth, moderate operating leverage, market-implied WACC, GDP-aligned terminal growth
Bull: high-end growth, meaningful margin expansion, lower WACC, higher terminal growth, lighter capex
```

**Done when** every projection cell is a formula over the consolidation column, and the implied growth row prints beside the revenue row.

### Step 4: Operating Expense Modeling

Model operating leverage rather than a fixed margin: percentages decline as revenue scales, and each of S&M, R&D and G&A keeps its own line. Where `.agents/skills/dcf-model/references/sector-drivers.md` names a driver for the sector, that driver governs the line instead: software S&M runs against new bookings, biopharma R&D is a commitment rather than a percentage. The percentage-of-revenue rule below is the default for every line the sector reference does not claim.

- **A percentage-driven opex line is based on revenue, not gross profit.** Operating expenses scale with the top line
- `EBIT = Gross Profit - Total OpEx`
- State the margin path as an argument: current gross margin to target gross margin, current EBIT margin to target, each with the reason (scale, mix, pricing, a named efficiency programme)

**Done when** the EBIT margin path has a stated reason per period, every line the sector reference claims runs off its named driver, and no opex row references gross profit.

### Step 5: Free Cash Flow Calculation

```
EBIT
(-) Taxes (EBIT x tax rate)
= NOPAT
(+) D&A (non-cash, % of revenue)
(-) CapEx (% of revenue)
(-) Change in NWC
= Unlevered Free Cash Flow
```

State which free cash flow definition the model runs on and hold it: an unlevered stream discounts at WACC to enterprise value, a levered stream discounts at cost of equity to equity value. Mixing them is the most expensive error in this file. Audit for the three contaminations: interest expense inside an unlevered stream, the tax shield counted both in the cash flow and in the WACC, and non-operating income left in EBIT.

- **Working capital**: computed on the change in revenue, not the level. Negative is a source of cash, positive a use
- **Maintenance against growth capex**: name the split, because the terminal year should carry maintenance rather than the growth ramp

**Done when** the FCF row is a formula over NOPAT, D&A, capex and the NWC change, and the cash flow definition is written on the sheet.

### Step 6: Cost of Capital (WACC)

```
Cost of Equity = Risk-Free Rate + Beta x Equity Risk Premium
After-Tax Cost of Debt = Pre-Tax Cost of Debt x (1 - Tax Rate)
Market Value Equity = Price x Diluted Shares
Net Debt = Total Debt - Cash
Enterprise Value = Market Cap + Net Debt
WACC = Cost of Equity x Equity Weight + After-Tax Cost of Debt x Debt Weight
```

Pre-tax cost of debt comes from the credit rating, the yield on the company's own bonds, or interest expense over average total debt, in that order of preference.

**Basis discipline.** Every WACC component is a defensible choice, and the provenance comment on its blue input is where the defence lives. Each comment records:

| Component | The comment states |
|---|---|
| Risk-free rate | the tenor, the source, and the date |
| Beta | the source, the observation window and frequency, and whether it is levered or relevered |
| Equity risk premium | the source and its vintage |
| Cost of debt | which of the three bases above it came from |
| Capital structure | target or current weights, and market or book values |

Market values, not book, for the weights. A comment reading "9.2%" is not provenance; a comment reading which tenor, which window and which date is what lets a reviewer disagree with a number rather than merely distrust it.

**Special cases**: with cash above debt, net debt is negative and the debt weight goes with it. With no debt, WACC is the cost of equity.

**Done when** every WACC input carries a basis comment in the form above and the WACC cell is a formula over them.

### Step 7: Discount Rate Application

Mid-year convention: periods run 0.5, 1.5, 2.5 and so on, and `Discount Factor = 1 / (1 + WACC)^Period`. `PV of FCF = Unlevered FCF x Discount Factor`.

Horizon: five years is standard, seven to ten for a company still converging on a defensible margin, three for a mature business. The explicit period should run until the drivers are stable, because everything after it is the terminal value.

**Done when** the discount-factor row is a formula over the period row and the WACC cell, and every period is present.

### Step 8: Terminal Value

**Perpetuity growth (preferred):**

```
Terminal FCF = Final Year FCF x (1 + g)
Terminal Value = Terminal FCF / (WACC - g)
PV of Terminal Value = Terminal Value / (1 + WACC)^Final Period
```

`g` below the risk-free rate and below long-term nominal GDP, and always below WACC or the value is infinite. Conservative is 2.0 to 2.5 percent, moderate 2.5 to 3.5, and anything above that is a claim that the company outgrows the economy forever, which needs a sentence defending it.

**Exit multiple (alternative):** `Terminal Value = Final Year EBITDA x Exit Multiple`, with the multiple taken from where the subject and its peers actually trade.

**The implied-exit-multiple back-check is mandatory, whichever method built the terminal value.** Divide the terminal value by the terminal-year EBITDA and compare the result against the subject's own trading history from `get_historical_valuation` and against the peer set. A perpetuity growth rate that implies an exit multiple far from where the stock has ever traded is the tell that the terminal assumptions are wrong, and it is a finding about the model rather than a footnote. Run it the other way too when the exit-multiple method was used: solve for the perpetuity growth rate that multiple implies, and check it is a rate a company could actually sustain.

**Terminal value share of enterprise value** is a structural finding about horizon adequacy, not a formatting note. Around half to two thirds is normal. Above 80 percent, the model is a claim about the terminal year wearing a forecast, and the answer is a longer explicit period, not a different growth rate. Below about 40 percent, check the terminal assumptions are not too conservative to be credible.

**Done when** both the implied exit multiple and the terminal share of EV are live cells on the sheet with Checks rows against them.

### Step 9: Enterprise to Equity Value Bridge

```csv
Valuation Component,Amount ($M)
PV Explicit FCFs,X.X
PV Terminal Value,Y.Y
Enterprise Value,Z.Z
(-) Net Debt,A.A
Equity Value,B.B
Diluted Shares (M),C.C
Implied Price per Share,$XX.XX
Current Share Price (as of DATE),$YY.YY
Implied Return,+XX%
```

**Anchor to spot.** The current price and the implied return sit next to the fair value in the output block, both as-of stamped. Without them a fair value below spot reads as an unexplained number rather than as the sell case it is, and a reader cannot tell a 4 percent gap from a 40 percent one without arithmetic you should have done.

Net debt is total debt less cash: positive subtracts from EV, negative (net cash) adds. Use diluted shares. Where they exist and matter, bridge the other claims too: minority interests, unfunded pension, and operating leases when they are not already in debt.

**Done when** the bridge is a formula chain from enterprise value to implied price, and the implied-return cell references the spot cell rather than a typed number.

### Step 10: Scenarios and Sensitivities

**Pick the grid from the decision, not from habit.** WACC against terminal growth answers "how much of this is the discount rate", which is often not the question:

| The question | The grid |
|---|---|
| How much of the value is the discount rate and the terminal assumption | WACC against terminal growth |
| Is the value in growth or in operating leverage | Revenue growth against EBIT margin |
| How much rides on the cost-of-equity inputs | Beta against risk-free rate |
| What does the market already require | Reverse DCF, below, rather than a grid |
| Where does the case break | The mechanical bear case, below |

Three grids stacked at the bottom of the DCF sheet is the default; swap one for the question actually being asked when it differs.

**Grid construction.** Use odd dimensions, 5x5 as standard and 7x7 where the range matters, so the grid has a true centre cell. Build each axis as `[base - 2*step, base - step, base, base + step, base + 2*step]`, which puts the model's own assumption in the middle row header and the middle column header, where a reader can see which cell is the actual forecast. The centre cell therefore has to equal the model's headline output. That is the check that the grid is wired correctly, and the build script asserts it: after `recalc.py`, reopen the workbook with `data_only=True`, read the centre of every grid against the output that grid varies, and fail the build if they differ. `.agents/skills/xlsx/SKILL.md` requires this assertion; do not skip it. Highlight the centre cell (bold, `BDD7EE` fill) so the base case is visually anchored. Every data cell in all three grids, 75 in total, holds a full DCF recalculation formula written by an openpyxl loop, so the grids work the moment the user opens the file.

**Weak sensitivity designs.** Each of these produces a grid that looks like analysis and carries none:

- **Dependent axes.** Beta against WACC, or revenue growth against revenue CAGR: one axis moves the other, so the corners are worlds that cannot exist
- **Off-centre base.** The grid is built around something other than the model's own assumption, so the centre rule above cannot hold and the reader cannot see which cell is the actual forecast
- **A range too narrow to inform.** Every cell rounds to the same story; widen the step until the corners are genuinely different cases
- **Sensitising a non-driver.** The grid moves the output by a rounding error while the real driver sits fixed. If the corner cells differ by less than a few percent, sensitise something else
- **A haircut bear case.** A percentage cut to the base target is not a scenario, because nothing in the business had to happen for it

**The bear case is mechanical.** It is driven by a stated change to a driver, with the arithmetic shown: demand falls to this level, or the gross margin resets to that one, or the exit multiple derates to where the stock traded in the last downturn. The case then names what breaks, through which line item, to what number. Under the bear case, check that liquidity, covenants and maturities still work rather than only reporting a lower target: a company that cannot fund the bear case has a different downside from one that can.

**Each case records where it came from**: the model, a source, the user, your own judgement, or purely illustrative, with an as-of. A bull case taken from management's own targets is a different object from one you built.

**Probabilities complete or nothing is weighted.** A probability-weighted fair value is published only when the three cases satisfy the completeness rule in `.agents/skills/research-conventions/references/judgment.md`; otherwise the cases ship unweighted and labelled an illustrative skew, and no weighted fair value appears in the model or the message.

#### Reverse DCF

Solve for what the current price already embeds, and present it beside your forecast. It converts "my model says X" into "the market is underwriting Y, and here is why I disagree", which is the only form of a valuation that is arguable.

Build it as a small block on the DCF sheet:

1. Copy the driver most of the value hangs on, usually the revenue CAGR over the explicit period or the terminal EBIT margin, into a blue input cell of its own, labelled `Solved market-implied <driver>`. A driver with a direct algebraic inverse (a single-stage growth or margin) stays a formula; only a driver that needs root-finding earns the solved cell
2. Wire a parallel valuation chain off that cell through to an implied price of its own, and leave it live. Compact is fine: re-use the forecast rows and recompute only the cells the solved driver changes. The chain stays separate from the forward valuation, so the residual row in step 5 keeps testing the solved value rather than reading back the forward price
3. In the build script, bisect on that cell until the implied price equals the spot price to within a cent, and write the solved value in as the blue input under the xlsx skill's solved-value exception: a `Solved:` comment naming the target cell and the re-solve command. Everything downstream stays a live formula, so a reader can nudge the solved driver and watch the price move; a nudged value is a trial value until the residual row reads OK again
4. Link the solved driver and your own forecast for the same driver into two cells side by side with the gap as a formula between them, and state beside them that the implied driver holds every other assumption fixed
5. Add the residual row on `Checks`: the reverse block's implied price minus the spot price cell, column C testing within one cent, so the row fails as soon as spot or any other input moves and the solved value no longer fits

The output sentence is the point: the price today requires this growth rate or this margin, our forecast is that one, and the difference is the position.

**Done when** the three grids are populated with full recalculation formulas and their centre cells assert, the bear case names a driver and a broken line item, and the reverse block ties to spot.

## Valuation Judgement

A model that computes is an arithmetic exercise. It becomes a valuation when it answers these seven, which are the DCF rendering of the seven questions in `.agents/skills/research-conventions/references/judgment.md`, in the delivery message and in the model's own summary block:

1. **What is the market implying today?** From the reverse DCF, in the driver's own units
2. **What do we disagree with, and on what evidence?** One driver, one reason
3. **What has to happen for the fair value to be reached, and where does that path sit against consensus?** The specific path, not the growth rate, reconciled in the consensus bridge below
4. **What breaks it?** The mechanical bear case, through a named line item, to a number
5. **Which dated event converts the gap into price?** The catalyst, with its date and whether it is scheduled, likely or speculative
6. **Which observation would make us wrong?** The reading, in the driver's own units, that retires the case, and when it prints
7. **What does the number imply doing?** With the conditions that would change it

**Consensus bridge.** Reconcile the model's next one or two years against the published consensus for the same periods, line by line, and state where and why they differ. Consensus is not a target to match, it is the estimate path the price is set against, so a model that quietly sits 20 percent below it without saying so is hiding its own thesis. Include the consensus vintage and the analyst count.

**A range, not a point.** Deliver a valuation band with the drivers that move you across it and say which end you sit at and why. Precision is bounded by the evidence: a fair value quoted to the cent off an assumed terminal growth rate claims a confidence the inputs do not carry.

**The stance.** Close on what the number implies, in the closed action vocabulary and under the input gates in `.agents/skills/research-conventions/references/judgment.md`, with the conditions that would change it. A fair-value range with no stance leaves the reader to do the work the model was built for.

**Done when** all seven questions are answered in the delivery, the consensus bridge names its vintage, and the stance carries the conditions that would change it.

## Checks Sheet

Every model workbook carries a `Checks` sheet. The four-column layout, the verdict formula, the roll-up and the read-back after recalculation are the sheet contract under *Financial Model Conventions* in `.agents/skills/xlsx/SKILL.md`. The rows a DCF has to carry:

| Check | Column B holds | Verdict |
|---|---|---|
| Revenue build ties to the margin build | Projected revenue minus the revenue the margin rows are applied to | FAIL |
| EBITDA equals revenue times margin | EBITDA minus revenue times the selected EBITDA margin | FAIL |
| D&A ties | D&A minus revenue times the selected D&A percentage | FAIL |
| Change in NWC ties | Change in NWC minus the revenue change times the selected NWC percentage | FAIL |
| FCF formula integrity | Unlevered FCF minus (NOPAT + D&A - CapEx - change in NWC) | FAIL |
| Discount factors positive | `=MIN(<discount factor row>)`; column C tests `>0` | FAIL |
| Terminal value positive | The terminal value cell; column C tests `>0` | FAIL |
| EV equals PV of FCF plus PV of TV | Enterprise value minus (sum of PV FCFs + PV of terminal value) | FAIL |
| Equity bridge ties | Equity value minus (enterprise value - net debt) | FAIL |
| Implied price equals equity value over shares | Implied price minus equity value divided by diluted shares | FAIL |
| Implied return ties to spot | Implied return minus (implied price divided by the spot cell, less one) | FAIL |
| WACC greater than terminal growth | WACC minus terminal growth; column C tests `>0` | FAIL |
| Terminal value share of EV within band | PV of terminal value divided by enterprise value; column C tests `<0.80` | WARN |
| Implied exit multiple computed | The implied exit multiple cell minus terminal value divided by terminal-year EBITDA | FAIL |
| Reverse DCF ties to spot | The reverse block's implied price minus the spot price cell; column C tests within `0.01`. This is the residual row the solved driver's `Solved:` comment points at | FAIL |
| Scenario blocks are distinct | `=SUMPRODUCT((<bear assumption block><><bull assumption block>)*1)`; column C tests `>0`. The `*1` form is the one every engine evaluates; `--(...)` returns 0 in IronCalc | FAIL |
| Share price falls as WACC rises | Bottom-centre cell of the WACC grid minus its top-centre cell; column C tests `<0` | FAIL |
| Share price rises as g rises | Right-centre cell of the WACC grid minus its left-centre cell; column C tests `>0` | FAIL |
| Grid centre reproduces the headline | The grid's centre cell minus the output that grid varies, under the centre rule in Step 10: the implied share price for the WACC and exit-multiple grids, and whichever output the third grid varies. One row per grid, and the row `audit.py` `sensitivity_centre` reads to learn which output a grid designates | FAIL |

The build script writes this sheet **last**, once every other sheet exists and its row positions are locked, so the check formulas point at final addresses. Comments on blue inputs are `audit.py`'s job, under `provenance`, so no row here counts them. A FAIL blocks delivery: fix the model, not the check. A WARN either gets a fix or gets its reason in column D and one sentence in the delivery saying why the forecast is right and the band is not.

## Verify Before Delivering

File: `[Ticker]_DCF_Model_[Date].xlsx` under `$WORK_DIR/work/{task}/`.

1. **Structure**: the four sheets of `references/workbook-patterns.md`; scenario blocks with the year header row; the case selector driving a consolidation column; grids at the bottom of the DCF sheet with odd dimensions; the `Checks` sheet; blue inputs, black formulas, green links; a comment on every hardcoded input; borders around major sections
2. **`python .agents/skills/xlsx/scripts/recalc.py model.xlsx 30`** until status is "success"; on errors read `.agents/skills/dcf-model/TROUBLESHOOTING.md`
3. **`python .agents/skills/xlsx/scripts/audit.py model.xlsx --strict`** and fix every `fail`
4. **Assert the grid centres**: reopen with `data_only=True` and confirm every centre cell still equals the output its grid varies, under the centre rule in Step 10
5. **Assert the case selector is live**: write 1, recalc, read the implied price; write 3, recalc, read it again. The two differ and the bear price is below the bull price. A selector that does not move the output means the consolidation column is wired to a dead cell, and every grid built on it is decoration
6. **Read the `Checks` sheet back** with `data_only=True`: any FAIL blocks delivery, and the `Overall` roll-up and the `Diagnostics open` count are read back the same way
7. **Spot-check formulas**: one FCF formula against its assumption rows, and one revenue formula against the consolidation column rather than a nested IF
8. **Restore the base case** (selector 2), then recalculate once more and read the implied price, the fair-value range and the `Checks` roll-up back with `data_only=True` before saving. Steps 5 and 6 left bear and bull values cached; without this pass the delivered file shows a base selector over another case's numbers
9. **The valuation, in the delivery message**:
   - Fair value stated as a range, with spot and the implied return beside it, both as-of stamped
   - Terminal value share of EV and the implied exit multiple, each with its comparison
   - Reverse DCF: the market-implied driver, our forecast, and the gap
   - Consensus bridge with its vintage
   - A mechanical bear case naming a driver and a broken line item
   - The seven questions answered, and a stance with the conditions that would change it
   - The readiness posture, with the input responsible if it is below decision-grade

**The gate**: `recalc.py` reports "success", `audit.py --strict` reports no `fail`, the `Checks` roll-up reads OK with the base case restored, and every bullet of step 9 is in the message. A model short of any of the four is not delivered.
