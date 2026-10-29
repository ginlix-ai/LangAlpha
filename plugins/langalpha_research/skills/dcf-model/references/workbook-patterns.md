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
9,Selected-case block header
10-20,Selected-case drivers
28,Income statement header
43,Cash flow header
51/65/79,Bear / Base / Bull block headers
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

**Scenario assumptions**: the three blocks under *Scenario blocks and the selected-case block* below, carrying revenue growth, gross margin, the opex rates the income statement uses (S&M, R&D, G&A as a percent of revenue, or the sector's driver), tax rate, D&A percent, capex percent, NWC change percent, terminal growth and WACC across the five projection years, plus the selected-case block that mirrors them. Every driver has its own row there and every projection year its own column, so no two drivers share a cell:

```csv
Selected case (row 9 header; columns B:F = FY1 to FY5),FY1 2024E,FY2 2025E,FY3 2026E,FY4 2027E,FY5 2028E
10 Revenue Growth,[=CHOOSE($B$4,B53,B67,B81)],...,...,...,...
11 Gross Margin,...,...,...,...,...
12 S&M % revenue,...,...,...,...,...
13 R&D % revenue,...,...,...,...,...
14 G&A % revenue,...,...,...,...,...
15 Tax Rate,...,...,...,...,...
16 D&A % revenue,...,...,...,...,...
17 CapEx % revenue,...,...,...,...,...
18 NWC change % of revenue change,...,...,...,...,...
19 Terminal Growth,...,,,,
20 WACC,...,,,,
```

The projection formulas below read it with a row-anchored mixed reference (`B$10`, not `$B$10` or `$E$10`): FY1 sits in column B and 2024E in column F, a constant four-column offset, so a formula written once in F and copied across to J walks the years on its own. The row numbers are illustrative.

**Financials**

Actuals occupy columns B:E and the five projection years F:J, the same five years the valuation block discounts, so every FCF the valuation reads is formula-driven. Rows are numbered as listed, with row 28 as the header; the first projection column is shown and the rest copy across.

```csv
Row,Income Statement ($M),2020A,2021A,2022A,2023A,2024E (F),2025E (G) ... 2028E (J)
29,Revenue,XXX,XXX,XXX,XXX,[=E29*(1+B$10)],copy across
30,  % growth,XX%,XX%,XX%,XX%,[=F29/E29-1],copy across
31,Gross Profit,XXX,XXX,XXX,XXX,[=F29*B$11],copy across
32,  % margin,XX%,XX%,XX%,XX%,[=F31/F29],copy across
33,Operating Expenses:,,,,,,
34,  S&M,XXX,XXX,XXX,XXX,[=F29*B$12],copy across
35,  R&D,XXX,XXX,XXX,XXX,[=F29*B$13],copy across
36,  G&A,XXX,XXX,XXX,XXX,[=F29*B$14],copy across
37,  Total OpEx,XXX,XXX,XXX,XXX,[=F34+F35+F36],copy across
38,EBIT,XXX,XXX,XXX,XXX,[=F31-F37],copy across
39,  % margin,XX%,XX%,XX%,XX%,[=F38/F29],copy across
40,Taxes,(XX),(XX),(XX),(XX),[=F38*B$15],copy across
41,NOPAT,XXX,XXX,XXX,XXX,[=F38-F40],copy across
```

```csv
Row,Cash Flow ($M),2024E (F),2025E (G) ... 2028E (J)
44,NOPAT,[=F41],copy across
45,(+) D&A,[=F29*B$16],copy across
46,(-) CapEx,[=F29*B$17],copy across
47,(-) Change in NWC,[=(F29-E29)*B$18],copy across
48,Unlevered FCF,[=F44+F45-F46-F47],copy across
```

Each cash-flow column reads the same year's column in the income statement (2024E is column F in both), so the first forecast year never starts from the last actual, and the cash-flow schedule runs to 2028E because the valuation block discounts five explicit years. Confirm the row numbers against the actual layout before writing, then test one column and copy across.

**Discounting and valuation output**

```csv
DCF Valuation,2024E,2025E,2026E,2027E,2028E,Terminal
Unlevered FCF ($M),[=F48],[=G48],[=H48],[=I48],[=J48],
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

## Scenario blocks and the selected-case block

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

**Reference them through the selected-case block:**

1. Case selector cell (B4 in the layout above) holds 1 = Bear, 2 = Base, 3 = Bull
2. The selected-case block has the same rows and year columns as a scenario block, and each cell pulls the same cell from the selected block: `=CHOOSE($B$4, B53, B67, B81)`, where B53, B67 and B81 are the FY1 growth cells of the Bear, Base and Bull blocks in turn. The blocks sit below the cash-flow schedule in this layout so no block row collides with a projection row; the row numbers are illustrative. The choice runs across blocks, never along a block's year columns: an INDEX over `B53:F53` would pick a year, not a scenario
3. Projection formulas reference the selected-case block with a row-anchored mixed reference: `Revenue 2024E: =E29*(1+B$10)`, where E29 is prior-year revenue and `B$10` is the selected FY1 growth. Copied to G the reference becomes `C$10`, the FY2 growth
4. Each block carries the full set of DCF assumptions across the projection years

The selected-case block centralises the logic and makes the model auditable. Check that it pulls from the intended block before building the projections on it.

## FCF and opex formula patterns

```csv
Item,Formula,Reference
D&A,=F29*B$16,B$16 = selected-case D&A % for the same year
CapEx,=F29*B$17,B$17 = selected-case CapEx % for the same year
Change in NWC,=(F29-E29)*B$18,B$18 = selected-case NWC % for the same year
Unlevered FCF,=F44+F45-F46-F47,F44=NOPAT F45=D&A F46=CapEx F47=change in NWC
```

Confirm the scenario block row locations and set up the selected-case block before writing any of these.

For a percentage-of-revenue expense driver, multiply same-period revenue by the selected scenario's same-period rate held in a labelled input cell: `S&M: =F29*B$12`, where F29 is same-period revenue and `B$12` is the selected case's S&M rate for that period. The rate lives in that input cell so a reader can find it and move it. Where `.agents/skills/dcf-model/references/sector-drivers.md` names a different driver for the sector, that driver governs.

## Cell comments

"Source: [System/Document], [Date], [Reference], [URL if applicable]"

```csv
Item,Source Comment
Stock price,Source: get_company_overview 2025-10-12 close price
Shares outstanding,Source: fundamentals MCP get_financial_statements FY2024 diluted
Historical revenue,Source: fundamentals MCP get_financial_statements FY2024
Beta,Source: yf_fundamentals MCP compare_valuations 2025-10-12 beta field 5-year monthly vs index
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
