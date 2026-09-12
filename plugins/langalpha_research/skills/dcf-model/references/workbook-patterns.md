# DCF Workbook Patterns

Load this before writing the build script: it carries the sheet architecture, the row layouts, the formula patterns and the assumption requirements the workbook is built against.

## Sheet architecture

Four sheets:

1. **Cover** - tiles, each a formula linking to the cell it reports: implied price, implied return, enterprise value, WACC, terminal growth, posture
2. **DCF** - the model, with the scenario blocks, the valuation output block, and the sensitivity grids at the bottom
3. **WACC** - cost of capital
4. **Checks** - tie-outs, one live formula per row

Sensitivity grids go at the BOTTOM of the DCF sheet, not on their own sheet, which keeps every valuation output together.

This layout is the default, not a rule that outranks the person you are building for. Precedence is the same as in the xlsx skill: the user's stated layout or template first, then the xlsx skill's conventions, then this default. A user who asks for an Assumptions sheet, a separate Sensitivity sheet, or three scenarios on their own tabs gets exactly that, and the `Checks` rows and the verify sequence in `SKILL.md` are read accordingly.

## Row planning

Lock the row layout before the first formula:

1. Write ALL headers and labels first
2. Write ALL section dividers and blank rows
3. THEN write formulas against the locked row positions
4. Test formulas immediately after creation

```csv
Row,Content
1,[Company Name] DCF Model
2,Ticker | Date | Year End
4,Case Selector
7,KEY ASSUMPTIONS
26,Assumption headers
27-31,Growth assumptions
```

Formulas written before the headers exist point at rows that then shift, which is where `#REF!` comes from.

## DCF sheet layout

**Header and market data**

```csv
Row,Content
1,[Company Name] DCF Model
2,Ticker: [XXX] | Date: [Date] | Year End: [FYE]
4,Case Selector Cell (1=Bear 2=Base 3=Bull)
5,Case Name Display =IF([Selector]=1,"Bear",IF([Selector]=2,"Base","Bull"))
```

```csv
Item,Value
Current Stock Price (as of DATE),$XX.XX
Diluted Shares (M),XX.X
Market Cap ($M),[Formula]
Net Debt ($M),XXX [negative if net cash]
```

Market data is not case dependent.

**Scenario assumptions**: the three blocks under *Scenario blocks and the consolidation column* below, carrying revenue growth, the opex rates the income statement uses (S&M, R&D, G&A as a percent of revenue, or the sector's driver), tax rate, D&A percent, capex percent, NWC change percent, terminal growth and WACC across the projection years, plus the consolidation column. The `$E$10` to `$E$24` references in the layout below read that consolidation column, one row per driver and year; the row numbers are illustrative.

**Financials**

```csv
Income Statement ($M),2020A,2021A,2022A,2023A,2024E,2025E,2026E
Revenue,XXX,XXX,XXX,XXX,[=E29*(1+$E$10)],[=F29*(1+$E$11)],[=G29*(1+$E$12)]
  % growth,XX%,XX%,XX%,XX%,[=F29/E29-1],[=G29/F29-1],[=H29/G29-1]
Gross Profit,XXX,XXX,XXX,XXX,[=F29*F33],[=G29*G33],[=H29*H33]
  % margin,XX%,XX%,XX%,XX%,[=F31/F29],[=G31/G29],[=H31/H29]
Operating Expenses:,,,,,,,
  S&M,XXX,XXX,XXX,XXX,[=F29*$E$14],[=G29*$E$15],[=H29*$E$16]
  R&D,XXX,XXX,XXX,XXX,[=F29*$E$17],[=G29*$E$18],[=H29*$E$19]
  G&A,XXX,XXX,XXX,XXX,[=F29*$E$20],[=G29*$E$21],[=H29*$E$22]
  Total OpEx,XXX,XXX,XXX,XXX,[=F36+F37+F38],[=G36+G37+G38],[=H36+H37+H38]
EBIT,XXX,XXX,XXX,XXX,[=F31-F39],[=G31-G39],[=H31-H39]
  % margin,XX%,XX%,XX%,XX%,[=F41/F29],[=G41/G29],[=H41/H29]
Taxes,(XX),(XX),(XX),(XX),[=F41*$E$24],[=G41*$E$24],[=H41*$E$24]
NOPAT,XXX,XXX,XXX,XXX,[=F41-F43],[=G41-G43],[=H41-H43]
```

```csv
Cash Flow ($M),2024E,2025E,2026E
NOPAT,[=E45],[=F45],[=G45]
(+) D&A,[=E29*$E$21],[=F29*$E$21],[=G29*$E$21]
(-) CapEx,[=E29*$E$22],[=F29*$E$22],[=G29*$E$22]
(-) Change in NWC,[=(E29-D29)*$E$23],[=(F29-E29)*$E$23],[=(G29-F29)*$E$23]
Unlevered FCF,[=E57+E58-E60-E62],[=F57+F58-F60-F62],[=G57+G58-G60-G62]
```

Confirm the row numbers against the actual layout before writing, then test one column and copy across.

**Discounting and valuation output**

```csv
DCF Valuation,2024E,2025E,2026E,2027E,2028E,Terminal
Unlevered FCF ($M),XXX,XXX,XXX,XXX,XXX,
Period,0.5,1.5,2.5,3.5,4.5,
Discount Factor,0.XX,0.XX,0.XX,0.XX,0.XX,
PV of FCF ($M),XXX,XXX,XXX,XXX,XXX,
Terminal FCF ($M),,,,,,XXX
Terminal Value ($M),,,,,,XXX
PV Terminal Value ($M),,,,,,XXX

Valuation Summary ($M),
Sum of PV FCFs,XXX
PV Terminal Value,XXX
Enterprise Value,XXX
(-) Net Debt,(XX)
Equity Value,XXX
Diluted Shares (M),XX.X
IMPLIED PRICE PER SHARE,$XX.XX
Current Stock Price (as of DATE),$XX.XX
Implied Return,XX%

Terminal value % of EV,XX%
Implied exit multiple (TV / terminal EBITDA),XX.Xx
Peer / historical multiple for comparison,XX.Xx
Market-implied driver (reverse DCF),XX%
Our forecast for the same driver,XX%
```

The last five rows are what turn the output block into a valuation rather than a total.

## Scenario blocks and the consolidation column

**Assumptions live in separate blocks per scenario, three structural elements each:**

1. **Section header row** (merged cells): "BEAR CASE ASSUMPTIONS"
2. **Column header row showing the projection years** (FY2025E, FY2026E). REQUIRED: without it nobody can tell which assumption belongs to which year
3. **Data rows** with assumption values

```csv
BEAR CASE ASSUMPTIONS (section header - merge across columns A:G)
Assumption,FY1,FY2,FY3,FY4,FY5
Revenue Growth (%),12%,10%,9%,8%,7%
EBIT Margin (%),45%,44%,43%,42%,41%
Terminal Growth,X%,,,,
WACC,X%,,,,

BASE CASE ASSUMPTIONS (section header - merge across columns A:G)
Assumption,FY1,FY2,FY3,FY4,FY5
Revenue Growth (%),16%,14%,12%,10%,9%
EBIT Margin (%),48%,49%,50%,51%,52%
Terminal Growth,X%,,,,
WACC,X%,,,,

BULL CASE ASSUMPTIONS (section header - merge across columns A:G)
Assumption,FY1,FY2,FY3,FY4,FY5
Revenue Growth (%),20%,18%,15%,13%,11%
EBIT Margin (%),50%,51%,52%,53%,54%
Terminal Growth,X%,,,,
WACC,X%,,,,
```

Blocks per scenario with the years running horizontally show the progression across years within a scenario, which is the thing a reviewer needs to see.

**Reference them through a consolidation column:**

1. Case selector cell (for example B6) holds 1 = Bear, 2 = Base, 3 = Bull
2. A consolidation column pulls from the selected block: `=INDEX(B10:D10, 1, $B$6)`
3. Projection formulas reference the consolidation column: `Revenue Year 1: =D29*(1+$E$10)`, where D29 is prior-year revenue and `$E$10` is the consolidation cell for FY1 growth
4. Each block carries the full set of DCF assumptions across the projection years

The consolidation column centralises the logic and makes the model auditable. Check that it pulls from the intended block before building the projections on it.

## FCF and opex formula patterns

```csv
Item,Formula,Reference
D&A,=E29*$E$21,$E$21 = consolidation column for D&A %
CapEx,=E29*$E$22,$E$22 = consolidation column for CapEx %
Change in NWC,=(E29-D29)*$E$23,$E$23 = consolidation column for NWC %
Unlevered FCF,=E57+E58-E60-E62,E57=NOPAT E58=D&A E60=CapEx E62=change in NWC
```

Confirm the scenario block row locations and set up the consolidation column before writing any of these.

For a percentage-of-revenue expense driver, multiply same-period revenue by the selected scenario's same-period rate held in a labelled input cell: `S&M: =F29*$E$14`, where F29 is same-period revenue and `$E$14` is the selected case's S&M rate for that period. The rate lives in that input cell so a reader can find it and move it. Where `.agents/skills/dcf-model/references/sector-drivers.md` names a different driver for the sector, that driver governs.

## Cell comments

"Source: [System/Document], [Date], [Reference], [URL if applicable]"

```csv
Item,Source Comment
Stock price,Source: get_company_overview 2025-10-12 close price
Shares outstanding,Source: fundamentals MCP get_financial_statements FY2024 diluted
Historical revenue,Source: fundamentals MCP get_financial_statements FY2024
Beta,Source: get_company_overview 2025-10-12 5-year monthly beta vs index
Risk-free rate,Source: macro MCP get_treasury_rates 2025-10-12 10Y yield
Consensus estimates,Source: get_company_overview analyst consensus N=12 as of 2025-10-12
```

WACC inputs additionally carry the basis fields from Step 6.

An assumption driver reads "Assumption: ..." and a solved value reads "Solved: ...", both in the form the xlsx skill's Provenance paragraph gives. Write the comment as each value is created: the source is unrecoverable an hour later, and `audit.py` fails the workbook under `provenance`.

## Sensitivity grids

Three grids, vertically stacked at the bottom of the DCF sheet with one or two blank rows between them: WACC against terminal growth, revenue growth against EBIT margin, beta against risk-free rate, unless Step 10 chose a different pair for the question at hand. Conditional formatting on a colour scale across each grid.

These are NOT Excel's "Data Table" feature (Data, What-If Analysis, Data Table), which cannot be automated through openpyxl. They are plain grids: row headers, column headers, and a regular formula in every data cell.

With a base WACC of 9.0 percent, base terminal growth of 3.0 percent and a step of 0.5pp per axis:

```csv
WACC vs Terminal Growth,2.0%,2.5%,3.0%,3.5%,4.0%
8.0%,[formula],[formula],[formula],[formula],[formula]
8.5%,[formula],[formula],[formula],[formula],[formula]
9.0%,[formula],[formula],[CENTRE],[formula],[formula]
9.5%,[formula],[formula],[formula],[formula],[formula]
10.0%,[formula],[formula],[formula],[formula],[formula]
```

The middle row header is the base WACC and the middle column header the base terminal growth, so `[CENTRE]` evaluates to the model's own implied share price under the centre rule in Step 10. Apply the same construction to the other two grids.

Each cell recalculates the full DCF for its combination, taking WACC from its row header (`$A88`) and terminal growth from its column header (`B$87`):

`=([SUM of PV FCFs using $A88 as the discount rate] + [Terminal Value using B$87 as g and $A88 as WACC] - [Net Debt]) / [Diluted Shares]`

```python
# Pseudocode for populating a sensitivity table.
# The axis values are written once, into the header cells; every grid formula reads them.
for row_idx in range(len(wacc_range)):
    r = start_row + row_idx
    wacc_ref = f"$A{r}"                                  # row header, column absolute
    for col_idx in range(len(term_growth_range)):
        c = start_col + col_idx
        g_ref = f"{get_column_letter(c)}${header_row}"   # column header, row absolute
        formula = f"=<DCF recalc discounting at {wacc_ref}, terminal growth {g_ref}>"
        ws.cell(row=r, column=c).value = formula
```

An axis value interpolated into the formula instead fails `audit.py` `formula_hardcode` on all 75 cells, and leaves the block unrecognisable as a grid, since detection turns on every cell reading the two headers that cross on it.

Write a formula for every cell in every grid, 75 in total. The grids must work the moment the user opens the file, with no manual step. The relationships are not linear, so an approximation is a wrong number in a professional-looking grid; each formula follows one pattern with two substitutions, and a Python loop writes them all.

## WACC sheet

```csv
COST OF EQUITY,,
Risk-Free Rate (10Y Treasury),X.XX%,[macro MCP get_treasury_rates]
Beta,X.XX,[input with basis comment]
Equity Risk Premium,X.XX%,[macro MCP get_market_risk_premium]
Cost of Equity,X.XX%,[formula]
COST OF DEBT,,
Credit Rating,AA-,[input]
Pre-Tax Cost of Debt,X.XX%,[input with basis comment]
Tax Rate,XX.X%,[link to DCF]
After-Tax Cost of Debt,X.XX%,[formula]
CAPITAL STRUCTURE,,
Current Stock Price,$XX.XX,[link to DCF]
Diluted Shares (M),XX.X,[link to DCF]
Market Capitalization ($M),"X,XXX",[formula]
Total Debt ($M),XXX,[input]
Cash & Equivalents ($M),XXX,[input]
Net Debt ($M),XXX,[formula]
Enterprise Value ($M),"X,XXX",[formula]
WACC,Weight,Cost,Contribution
Equity,XX.X%,X.X%,X.XX%
Debt,XX.X%,X.X%,X.XX%
WEIGHTED AVERAGE COST OF CAPITAL,X.XX%,[formula, bold, BDD7EE fill]
```

The beta matches its use: the cost of equity takes a beta levered to the model's own capital structure, so an unlevered beta is relevered before it enters.

## Assumption requirements

The requirements the layouts above do not carry. Each is a line a reviewer tests.

- **Growth**: projected growth stays consistent with history and with what the addressable market can hold, and a break from either carries its reason on the sheet
- **Terminal margins**: the terminal year runs at a steady state the business can hold, not at the peak of the ramp
- **Reinvestment**: D&A and capex percentages move together, and a widening gap carries the reinvestment story that explains it
- **Tax**: one tax rate across the projection years, and a rate that changes between years carries its stated reason
