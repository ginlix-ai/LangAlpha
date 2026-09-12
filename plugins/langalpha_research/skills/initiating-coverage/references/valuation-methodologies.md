# Valuation Methodologies for Equity Research

Deferred reference for `initiating-coverage`. Load it in Task 3 when choosing a method, when the business is capital intensive or financed by someone else, or when building the target-price bridge. Mechanics for DCF, trading comparables and precedent transactions follow the method sections below.

## Choosing a method

Method choice is an argument, not a default, and each method has a caveat that travels with its output wherever that output appears. State the caveat in the section that uses the method and in any summary that repeats its number.

| Method | Fits when | Caveat that travels with it |
|---|---|---|
| DCF | cash flows are projectable and the capital structure is stable | the terminal value's share of total value, the discount-rate build, and the sensitivity range |
| Trading comparables | five or more genuinely comparable peers report on a basis that can be aligned | peer-set quality and calendarisation: which four quarters went into each LTM and NTM figure, per company, per `.agents/skills/research-conventions/references/market-data-rules.md` |
| Sum of the parts | segments are separately disclosed and separately valuable | segment disclosure quality, corporate-cost allocation, and leakage between segments |
| Precedent transactions | a live market prices this asset class and the deals are recent | deal vintage, the control premium, and the synergy assumption inside each multiple |
| Book value and asset based | the assets are marked and the marks mean something | mark reliability and reserve adequacy, stated per asset class |
| Event driven | value turns on a dated binary outcome | the probability and timing assumptions, shown separately from the payoffs |
| Revenue multiple | see the demotion rule under trading comparables | it is preliminary market context, not valuation support, while material debt, leases or dilution sit outside the comparison |

---

## Discounted Cash Flow (DCF) Analysis

### Overview

DCF analysis values a company based on the present value of its projected future cash flows. This is considered the most theoretically sound valuation method as it's based on fundamental value creation.

### Step-by-Step DCF Process

#### 1. Historical Financial Analysis
- Collect 3-5 years of historical financials
- Calculate historical FCF = EBIT(1-Tax Rate) + D&A - CapEx - Change in NWC
- Analyze historical growth rates and margins
- Identify trends and cyclicality

#### 2. Build Revenue Projections (5-10 years)
**Approaches:**
- **Top-down**: Start with market size (TAM) → Market share → Revenue
- **Bottom-up**: Units sold × Price per unit
- **Hybrid**: Combine multiple drivers

**Key Considerations:**
- Management guidance and historical growth
- Industry growth rates and market trends
- Competitive dynamics and market share evolution
- Product pipeline and new market opportunities
- Macroeconomic factors

#### 3. Project Operating Expenses
- **COGS**: As % of revenue (analyze historical margins)
- **SG&A**: Often semi-fixed; model as % of revenue with scale effects
- **R&D**: Critical for tech/pharma; model as % of revenue
- **D&A**: Based on CapEx assumptions

**Calculate EBIT** = Revenue - COGS - Operating Expenses

#### 4. Calculate Unlevered Free Cash Flow
```
EBIT
× (1 - Tax Rate)
= NOPAT (Net Operating Profit After Tax)
+ Depreciation & Amortization
- Capital Expenditures
- Increase in Net Working Capital
= Unlevered Free Cash Flow (UFCF)
```

**CapEx Assumptions:**
- Maintenance CapEx: Required to maintain current operations (typically 2-4% of revenue)
- Growth CapEx: Required for expansion
- Consider industry benchmarks and company guidance

**Net Working Capital:**
- NWC = (Accounts Receivable + Inventory) - Accounts Payable
- Model as % of revenue or days (DSO, DIO, DPO)
- An increase in NWC is a use of cash

#### 5. Determine Terminal Value

**Method A: Perpetuity Growth Method**
```
Terminal Value = FCF(final year) × (1 + g) / (WACC - g)
```
- g = perpetual growth rate (typically 2-3%, not exceeding GDP growth)
- Use when company has reached stable, mature growth

**Method B: Exit Multiple Method**
```
Terminal Value = EBITDA(final year) × Exit Multiple
```
- Exit multiple based on current trading comps
- More appropriate for cyclical businesses

#### 6. Calculate Weighted Average Cost of Capital (WACC)

```
WACC = (E/V × Cost of Equity) + (D/V × Cost of Debt × (1 - Tax Rate))
```

**Cost of Equity (using CAPM):**
```
Cost of Equity = Risk-Free Rate + Beta × Equity Risk Premium
```
- Risk-Free Rate: 10-year Treasury yield
- Beta: Regression of stock returns vs. market (or use comparable beta)
- Equity Risk Premium: Call `get_market_risk_premium` and print its as-of date beside it.

**Cost of Debt:**
```
Cost of Debt = Risk-Free Rate + Credit Spread
```
- Use company's current borrowing rate or implied from bonds
- Adjust for credit rating if no bonds outstanding

**Capital Structure:**
- E/V = Market value of equity / Total value
- D/V = Market value of debt / Total value
- Use target capital structure, not current (if significantly different)

#### 7. Discount Cash Flows to Present Value

```
PV = Σ [FCFt / (1 + WACC)^t] + [Terminal Value / (1 + WACC)^n]
```

#### 8. Calculate Enterprise Value and Equity Value

```
Enterprise Value = PV of Projected FCF + PV of Terminal Value
Less: Net Debt (Total Debt - Cash)
Plus: Non-operating Assets
Less: Minority Interest
Less: Preferred Stock
= Equity Value

Price Per Share = Equity Value / Diluted Shares Outstanding
```

### DCF Sensitivity Analysis

Always perform sensitivity analysis on key variables:

1. **Two-way sensitivity table**: WACC vs. Terminal Growth Rate
2. **Revenue growth scenarios**: Base / Bull / Bear cases
3. **Margin assumptions**: Operating leverage scenarios
4. **Terminal multiple sensitivity**: If using exit multiple method

**Example Sensitivity Table:**
```
           Terminal Growth Rate
WACC      2.0%    2.5%    3.0%
8.0%      $45     $48     $52
9.0%      $40     $43     $46
10.0%     $36     $39     $41
```

### Common DCF Pitfalls to Avoid

1. **Double-counting growth**: Don't project high growth without corresponding investment (CapEx, NWC)
2. **Unrealistic terminal growth**: Should not exceed long-term GDP growth
3. **Ignoring cyclicality**: Normalize earnings for cyclical businesses
4. **Wrong cash flow definition**: Use unlevered FCF, not net income
5. **Inconsistent assumptions**: Match discount rate to cash flows (unlevered FCF → WACC)

---

## Trading Comparables Analysis

### Overview

Trading comps values a company based on how similar companies are valued in the public markets. This reflects current market sentiment and relative valuation.

### Step-by-Step Comps Process

#### 1. Select Comparable Companies

**Selection Criteria:**
- Same industry/sector (primary criterion)
- Similar business model and revenue streams
- Comparable size (market cap, revenue)
- Similar growth profile and margins
- Similar end markets and geographies

**Typical Universe:**
- Start with 8-15 companies
- Remove companies with unique circumstances
- Final set of 5-10 companies

#### 2. Gather Financial Information

**Required Data:**
- Current stock price and shares outstanding
- Latest fiscal year financial statements
- NTM estimates: the sum of the next four quarterly consensus mean estimates, per `.agents/skills/research-conventions/references/market-data-rules.md`
- Historical growth rates

**Calculate Market Metrics:**
- Market Cap = Share Price × Shares Outstanding
- Enterprise Value = Market Cap + Debt + Minority Interest + Preferred - Cash
- Net Debt = Total Debt - Cash & Equivalents

#### 3. Calculate Valuation Multiples

**Enterprise Value Multiples:**
- **EV/Revenue**: Good for early-stage/high-growth companies
- **EV/EBITDA**: Most common; good for capital-intensive businesses
- **EV/EBIT**: Useful when D&A varies significantly

**Equity Value Multiples:**
- **P/E (Price/Earnings)**: Most widely used
- **P/B (Price/Book)**: Good for financial institutions
- **P/S (Price/Sales)**: For unprofitable companies

**Calculate for:**
- Last Twelve Months (LTM) - historical
- Next Twelve Months (NTM) - forward-looking (preferred)

#### 4. Analyze and Select Multiples

**Create Comparable Company Table:**

| Company | Market Cap | EV/Revenue | EV/EBITDA | EV/EBIT | P/E (NTM) | Revenue Growth | EBITDA Margin |
|---------|-----------|------------|-----------|---------|-----------|----------------|---------------|
| Comp A  | $10B      | 3.5x       | 12.0x     | 18.0x   | 22.0x     | 15%            | 28%           |
| Comp B  | $8B       | 3.0x       | 10.5x     | 16.0x   | 19.0x     | 12%            | 27%           |
| ...     | ...       | ...        | ...       | ...     | ...       | ...            | ...           |
| Median  | -         | **3.2x**   | **11.0x** | **17.0x** | **20.5x** | 13%            | 27.5%         |

**Adjustments:**
- Remove outliers (typically >2 standard deviations)
- Consider using median instead of mean (less affected by outliers)
- Weight multiples if some comps are more comparable
- Adjust for differences in growth, margins, risk

#### 5. Apply Multiples to Target Company

**Example Calculation:**
```
Target Company NTM EBITDA = $500M
Selected EV/EBITDA Multiple = 11.0x
Implied Enterprise Value = $500M × 11.0x = $5,500M

Less: Net Debt = $1,000M
Equity Value = $4,500M

Shares Outstanding = 100M
Implied Price Per Share = $45.00
```

#### 6. Select Appropriate Multiple

**Choose based on:**
- **EV/Revenue**: High-growth, unprofitable companies (tech, biotech pre-profit)
- **EV/EBITDA**: Most common; capital-intensive industries (manufacturing, telecom)
- **P/E**: Profitable companies with stable cap structure (consumer, retail)
- **Sector-specific**: P/B for banks, EV/Production for oil & gas, EV/Subscriber for media

### The revenue-multiple demotion rule

An equity-value-to-revenue multiple prices the top line and ignores everything between it and the equity holder. While material debt, capitalised leases, prospective dilution or a customer-funded buildout sit outside the comparison, a revenue multiple is **preliminary market context**: label it that way in the heading, in the table and in every summary that repeats it, keep it out of the football field, and do not let a target price rest on it.

It becomes valuation support once the comparison moves to enterprise value including leases, on a fully diluted share count, against peers computed the same way.

### EPS basis for a P/E target

A P/E target is a multiple times an earnings number, so the earnings number needs the same defence as the multiple.

- Where GAAP EPS is distorted by non-recurring items, use recurring or operating EPS and show the bridge from GAAP to it.
- Where GAAP EPS is used anyway, say why it is the right basis for this company.
- Where an adjusted figure is used, cite the reconciliation and state, adjustment by adjustment, which ones we accept, which we reject and which are unproven. Stock-based compensation is a recurring cost of employing people; excluding it is a decision to state, not one to inherit from the company's presentation.
- Peer multiples in the same table use the same basis, or the table names which peer sits on which basis.

### Premium/Discount Analysis

Apply premiums or discounts based on:
- **Growth premium**: Higher growth → higher multiple
- **Profitability**: Higher margins → higher multiple
- **Size**: Larger companies typically trade at premium (liquidity)
- **Market position**: Market leaders → premium
- **Geographic**: Developed vs. emerging markets

---

## Precedent Transactions Analysis

### Overview

Precedent transactions values a company based on prices paid for similar companies in M&A transactions. This reflects control premiums and strategic value.

### Step-by-Step Process

#### 1. Identify Relevant Transactions

**Selection Criteria:**
- Same or similar industry
- Similar size (within 0.5x to 2x target's size)
- Similar business characteristics
- Recent transactions (last 3-5 years preferred)
- Announced and closed deals (avoid withdrawn deals)

**Typical Universe:**
- 5-10 transactions minimum
- Focus on recent deals (weight more recent higher)

#### 2. Gather Transaction Details

**Required Information:**
- Transaction date (announcement and close)
- Acquisition price and structure (cash, stock, mixed)
- Target's financials at time of transaction
- Strategic rationale and synergies
- Control premium paid

**Sources:**
- SEC filings (S-4, 8-K, proxy statements)
- Press releases and investor presentations
- Deal announcements and merger filings via `get_sec_filing` and `WebSearch`

#### 3. Calculate Transaction Multiples

**Same multiples as trading comps, but based on transaction value:**

```
Transaction Value = Equity Purchase Price + Assumed Debt - Cash Acquired

EV/Revenue (LTM) = Transaction Value / Target's LTM Revenue
EV/EBITDA (LTM) = Transaction Value / Target's LTM EBITDA
EV/EBIT (LTM) = Transaction Value / Target's LTM EBIT
```

**Calculate Control Premium:**
```
Control Premium = (Offer Price - Unaffected Price) / Unaffected Price
```
- Unaffected Price = Target's stock price 1-2 days before announcement
- Typical range: 20-40%

#### 4. Analyze Precedent Transactions

**Create Precedent Transactions Table:**

| Date | Target | Acquirer | Deal Value | EV/Revenue | EV/EBITDA | Premium | Rationale |
|------|--------|----------|------------|------------|-----------|---------|-----------|
| Q1'24 | CompX | BuyerA | $5.0B | 4.0x | 14.0x | 35% | Market consolidation |
| Q3'23 | CompY | BuyerB | $3.5B | 3.5x | 12.5x | 28% | Strategic fit |
| Median | - | - | - | **3.8x** | **13.0x** | **31%** | - |

#### 5. Apply to Target Company

**Important Considerations:**
- Precedent multiples typically higher than trading comps (include control premium)
- Adjust for differences in transaction rationale
- Consider market conditions at time of transactions vs. current
- Weight recent transactions more heavily

**Example Calculation:**
```
Target Company LTM EBITDA = $450M
Selected EV/EBITDA Multiple = 13.0x (precedent)
vs Trading Comps Multiple = 11.0x

Implied EV (Precedent) = $450M × 13.0x = $5,850M
Implied EV (Trading) = $450M × 11.0x = $4,950M

Implied Control Premium = $5,850M / $4,950M - 1 = 18%
```

### Adjustments to Transaction Multiples

**Consider adjusting for:**
- **Market conditions**: Bull vs. bear market (M&A activity levels)
- **Deal structure**: Strategic vs. financial buyer
- **Synergies**: Transactions with high synergies command premiums
- **Competitive dynamics**: Single vs. multiple bidders
- **Time value**: Older transactions less relevant

---

## The financed-growth gate

Fires whenever the equity case depends on capital the company does not yet have: capital intensity, debt maturities, lease obligations, prospective dilution, or a buildout funded by customer prepayments. Growth financed by someone else is a different asset from growth funded out of operations, and the difference lands on the equity holder.

Until all five of these exist, the report carries no positive ownership conclusion:

1. **Pro-forma fully diluted capitalisation bridge.** Current shares, plus options, restricted stock, convertibles, warrants and any announced issuance, arriving at the diluted count the target price divides by.
2. **Enterprise-value bridge including leases.** Market cap, plus gross debt, plus capitalised operating leases, plus preferred and minority interest, less cash, each component sourced to a filing page.
3. **Interest and refinancing burden.** Interest cover at the current rate, the maturity ladder by year, and what refinancing at today's cost does to earnings and to covenant headroom.
4. **Maintenance against growth capex.** Split them: only the growth half buys the growth the thesis assumes, and only the maintenance half is unavoidable in a downturn.
5. **After-financing returns.** Return on the incremental capital net of its cost, with a sensitivity across financing cost, utilisation and dilution.

Where one of the five is missing, the output is a preliminary or watchlist initiation naming that specific gap, per `.agents/skills/initiating-coverage/references/report-modes.md`. A multiple that ignores the capital stack is not a substitute for the gate; it is the failure the gate exists to catch.

---

## Valuation Reconciliation

### Creating a Valuation Bridge

Present all three methods in a single framework:

**Example Valuation Summary:**

| Method | Enterprise Value | Equity Value | Price/Share | Weight | Implied Value |
|--------|------------------|--------------|-------------|--------|---------------|
| DCF Analysis | $5,200M | $4,200M | $42.00 | 50% | $21.00 |
| Trading Comps | $5,500M | $4,500M | $45.00 | 30% | $13.50 |
| Precedent Trans. | $5,850M | $4,850M | $48.50 | 20% | $9.70 |
| **Weighted Avg** | - | - | **$44.20** | - | **$44.20** |

### Weighting the Methods

**Typical Weighting:**
- **DCF**: 40-60% (fundamental value, but assumes accuracy of projections)
- **Trading Comps**: 25-40% (reflects current market sentiment)
- **Precedent Trans.**: 15-25% (less relevant unless M&A likely)

**Adjust weights based on:**
- **Confidence in forecasts**: Higher confidence → higher DCF weight
- **Market conditions**: Bull/bear market affects comps reliability
- **M&A likelihood**: Higher if company in play or industry consolidating
- **Company maturity**: Mature companies → higher weight on comps; growth → higher DCF weight

### Valuation Range

Always present a valuation range, not a point estimate:

**Approach:**
- **Base Case**: Most likely scenario
- **Bull Case**: Optimistic assumptions (revenue growth, margins)
- **Bear Case**: Conservative assumptions

**Example:**
```
Bear Case: $38 - $40
Base Case: $42 - $46
Bull Case: $48 - $52

Rating: Buy (add). Target price $42 to $46, point $45.
```

### The target-price bridge

A target price ships with the whole path from the current price to it. All eight, or the target is not ready:

1. Current price with its as-of, and the fully diluted share count behind the market cap.
2. The target as a range with a point inside it, rather than a bare point.
3. Implied return from the current price, and the horizon it is earned over.
4. The method that produced it, and why that method fits this company.
5. The three or four assumptions the target is most sensitive to.
6. The multiple the target implies, checked against the company's own history and against the peer set.
7. The sensitivity range: what the target becomes under the bull and bear input sets.
8. The consensus target where one is available, with the gap explained rather than noted.

A multi-year case that multiplies forward earnings by an exit multiple is not a return until it is discounted: show a present value with the discount rate stated, or an annualised return against a stated hurdle, per `.agents/skills/research-conventions/references/judgment.md`.

### Sanity Checks

**Cross-check valuation with:**
1. **Historical multiples**: Is current valuation in line with history?
2. **Peer comparison**: Justified premium/discount vs. peers?
3. **Implied growth**: What growth is market pricing in?
4. **Implied returns**: IRR from current price to target price
5. **Market cap analysis**: Does total market cap make sense?

---

## Conclusion

Using all three valuation methods provides a robust framework for determining fair value:

- **DCF** provides intrinsic value based on fundamentals
- **Trading Comps** reflects current market valuation
- **Precedent Transactions** indicates M&A value and control premium

The key is to understand the assumptions driving each method and to present a well-reasoned valuation range that considers multiple scenarios and methodologies.
