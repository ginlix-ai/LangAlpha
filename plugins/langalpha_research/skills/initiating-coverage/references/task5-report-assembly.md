# Task 5: Report Assembly - Detailed Workflow

This document provides step-by-step instructions for executing Task 5 (Report Assembly) of the initiating-coverage skill.

## Spare no tokens or effort

This is the final deliverable and the last task of the pipeline: use whatever token budget writing every section in full takes.
Wherever a placeholder, a summary or a pointer to the model would otherwise stand, write the analysis itself.
House style is institutional equity research: JPMorgan, Goldman Sachs, Morgan Stanley.

**DO:**
- ✅ **Write EVERY section in FULL** - the analysis itself, not a summary and not a reference to another file
- ✅ **Include every chart from Task 4** - embed all of them throughout the document
- ✅ **Extract every financial table from the model** and write it into the report
- ✅ **Copy ALL 6-8K words from Task 1** - Company 101 (pages 6-17) and Industry Overview (pages 31-36) content verbatim (40-50% of report)
- ✅ **Meet every count in Critical Sections with Word Counts** - the totals and the per-section minimums, Projection Assumptions and Scenario Analysis above all
- ✅ **Build the deliverable as a DOCX** through `.agents/skills/docx/SKILL.md`, with no markdown syntax left in it
- ✅ **Run the quality check below** before delivering

---

## Task Overview

**Purpose**: Write and assemble the comprehensive final DOCX report.

**Prerequisites**: ⚠️ Verify before starting - ALL PREVIOUS TASKS REQUIRED
- **Required**: Company research from Task 1
- **Required**: Financial model from Task 2
- **Required**: Valuation analysis from Task 3
- **Required**: Chart files from Task 4

**⚠️ CRITICAL: DO NOT START THIS TASK UNLESS ALL TASKS 1-4 ARE COMPLETE**

This is the final assembly task. It cannot be completed without all previous work products.

**IF ANY OF TASKS 1, 2, 3, OR 4 ARE NOT COMPLETE**: Stop immediately and inform the user which tasks need to be completed first. The specific requirements are:
- Task 1: Company research document (6-8K words)
- Task 2: Financial model holding every Task 2 sheet of the workbook manifest in `.agents/skills/initiating-coverage/SKILL.md`
- Task 3: Valuation analysis with price target and recommendation, or, in a preliminary or watchlist initiation, with the numbered register of evidence required before ownership can be underwritten
- Task 4: Charts zip file with 25-35 charts

Do not attempt to create placeholder content, substitute missing sections, or assemble an incomplete report. The report requires ALL inputs to be publication-ready.

**Output**: Comprehensive Equity Research Report (.docx), to the lengths in Critical Sections with Word Counts below.

---

## Input Verification (CRITICAL)

**BEFORE STARTING - ALL TASKS MUST BE COMPLETE:**

### Task 1 Verification:
- [ ] Company research document exists? (6-8K words)
- [ ] Management bios complete? (300-400 words × 3-4 execs)
- [ ] Competitive analysis complete? (5-10 competitors)
- [ ] Risk assessment complete? (8-12 risks)

### Task 2 Verification:
- [ ] Financial model exists and can be opened?
- [ ] Model has projections (5 years)?
- [ ] Scenarios exist (Bull/Base/Bear)?
- [ ] Revenue by product table complete (20-30 rows)?
- [ ] Revenue by geography table complete (15-20 rows)?

### Task 3 Verification:
- [ ] Valuation analysis complete?
- [ ] Price target determined? (preliminary or watchlist initiation: no target price anywhere, and the label stated)
- [ ] Rating set? (Buy/Hold/Sell, beside its action verb)
- [ ] DCF analysis complete with sensitivity table?
- [ ] Comparable companies analysis complete with statistical summary?

### Task 4 Verification:
- [ ] 25-35 chart files exist?
- [ ] All 4 mandatory charts present?
  - [ ] Revenue by product (stacked area)
  - [ ] Revenue by geography (stacked bar)
  - [ ] DCF sensitivity (heatmap)
  - [ ] Valuation football field
- [ ] Chart files accessible and can be opened?
- [ ] Chart index created?

**IF ANY VERIFICATION FAILS**: Stop and complete missing task first.

---

## Report Specifications

### Critical Sections with Word Counts

| Section | Minimum | Target | Critical? |
|---------|---------|--------|-----------|
| Investment Summary (Page 1) | 500 | 700 | |
| Investment Thesis | 800 | 1,200 | |
| Risk Factors | 600 | 900 | |
| Company Description | 800 | 1,200 | |
| Management Bios | 1,000 | 1,400 | |
| Products & Services | 700 | 1,000 | |
| **Projection Assumptions** | **2,000** | **3,000** | ⭐ YES |
| **Scenario Analysis** | **1,500** | **2,000** | ⭐ YES |
| Financial Analysis | 1,200 | 1,800 | |
| Valuation Methodology | 800 | 1,200 | |

**Total: 10,000-15,000 words across 30-50 pages, with 25-35 embedded charts and 12-20 tables.** The low end of each range is a floor, not a target. This table is the one place the counts are stated; every other length statement here and in `.agents/skills/initiating-coverage/references/task5-assembly-phases.md` points back to it.

---

## Report Structure

Full page-by-page layout: `.agents/skills/initiating-coverage/assets/report-template.md`. The sections below give what each part carries and how long it runs; Page 2 and Pages 18-20 are laid out in the template only.

### Page 1: Investment Summary (CRITICAL PAGE)

**This is the most important page. Must have:**

1. **"INITIATING COVERAGE" header** (NOT "Company Update")
2. **Thesis-focused title** (e.g., "AI Platform Leader Positioned for 40% CAGR")
3. **Rating box** with:
   - Rating (Buy/Hold/Sell) beside its action verb
   - Current price
   - Target price
   - 52-week range
   - Market cap
   - Enterprise value
4. **Research analyst information** with credentials
5. **Stock price performance chart** (Figure 1)
6. **3-4 detailed investment bullets** with ■ character
   - Each bullet has **bold topic header** + 3-5 sentences
   - Lead with key numbers
7. **Financial summary table** (2-3 years historical + 2-3 years projected)
   - Years noted as "A" for actual, "E" for estimate

**Bullet Format Example:**
```
■ **Vertical SaaS leadership and regulatory moat should enable $50bn+ TAM by 2030.**
Deep domain expertise in healthcare IT, strong customer retention (95%+ net revenue retention),
and cross-sell capabilities have driven Acme Health's market expansion. With the healthcare IT
market expected to reach $50bn+ by 2030, Acme Health is well-positioned to capture share given
its regulatory moat and high switching costs. Management has indicated that 70% of current
revenue comes from enterprise hospital systems, suggesting strong product-market fit.
```

### Pages 3-5: Investment Thesis & Risks

**Investment Thesis (800-1,200 words)**
- 3-5 key thesis pillars
- Each pillar: 200-300 words
- Lead with key statistic
- Quantify financial impact
- Include timeline

**Risk Assessment (600-900 words)**
- 8-12 identified risks
- Organized by category:
  - Company-specific risks (4-6)
  - Industry/market risks (3-4)
  - Financial risks (2-3)
  - Macroeconomic risks (2-3)
- Each risk: 50-100 word description

### Pages 6-17: Company 101

**Company Description (800-1,200 words)**
- What the company does (plain English)
- Business model and monetization
- Geographic presence
- Scale metrics

**Company History (800-1,200 words)**
- Founding story
- Timeline of major milestones
- Strategic pivots
- Recent developments

**Management Team (1,000-1,400 words)**
- 300-400 word bio for each of 3-4 key executives
- Include: role, background, accomplishments, education
- Governance structure

**Products & Services (700-1,000 words)**
- Detailed product portfolio
- Features and differentiation
- Target customers
- Pricing models

**Customers & Go-to-Market (500-700 words)**
- Customer segments
- Distribution channels
- Sales strategy
- Key partnerships

### Pages 21-30: Financial Analysis

**Historical Financial Analysis (1,200-1,800 words)**
- Revenue trends and drivers
- Margin evolution
- Cash flow analysis
- Key metrics trajectory
- Historical context

**Projection Assumptions (2,000-3,000 words)** ⭐ CRITICAL

**MUST be extremely detailed. Structure:**

**A. Revenue by Product Assumptions (1,000-1,500 words)**

For EACH major product category:
```
[Product Category A] Revenue Assumptions

We project [Product A] revenue to grow from $XXM in 2024A to $XXM in 2029E,
representing a XX% CAGR. This growth is driven by:

1. [Driver 1 with specific quantification]
   - Specific metric: from XX to XX
   - Timeline: achieving YY by 2026E
   - Basis: [source or rationale]

2. [Driver 2 with specific quantification]
3. [Driver 3 with specific quantification]
[... 8-12 detailed points total for this product ...]

Specific assumptions by year:
- 2025E: XX% growth driven by [specific factors]
- 2026E: XX% growth as [specific factors]
- 2027-2029E: XX% CAGR as [longer-term factors]

Key risks to these assumptions include [specific risks].
```

**Repeat for EACH major product category.**

**B. Geographic Revenue Assumptions (500-800 words)**

For EACH major region:
```
[Region] Revenue Assumptions

We project [Region] revenue to grow XX% CAGR from 2024-2029E, reaching $XXM, driven by:

1. [Market dynamic with quantification]
2. [Distribution expansion with specifics]
3. [Competitive positioning]
[... 6-8 detailed points total for this region ...]
```

**Repeat for EACH major geographic region.**

**C. Other Key Assumptions (500-700 words)**
- Gross margin evolution (with specific drivers and bridge)
- Operating expense assumptions (R&D, S&M, G&A as % of revenue)
- Working capital assumptions (DSO, DIO, DPO with specific days)
- CapEx as % of sales (with justification)
- Tax rate assumptions

**Scenario Analysis (1,500-2,000 words)** ⭐ CRITICAL

**MUST have specific parameters for each scenario. Structure:**

**Bull Case (500-700 words)**
```
Bull Case: [Title describing key optimistic scenario]

Probability: XX%

Key Assumptions:
- Revenue CAGR (2024-2029E): XX% (vs. XX% base case)
- 2029E Revenue: $X,XXXm (vs. $X,XXXm base)
- 2029E EBITDA Margin: XX% (vs. XX% base)
- Key product growth: XX% CAGR (vs. XX% base)
- Geographic expansion: [specific milestones and timeline]
- Market share: XX% by 2029E (vs. XX% base)

Catalysts Required for Bull Case:
1. [Specific catalyst] - Expected timing: [date/quarter]
2. [Specific catalyst] - Expected timing: [date/quarter]
3. [Specific catalyst] - Expected timing: [date/quarter]

Detailed Rationale:
[200-300 words explaining what needs to happen for bull case to materialize.
Be specific about product launches, market conditions, competitive dynamics, etc.]

Valuation Implications:
- DCF Value: $XX per share (XX% upside from current)
- Trading Comps: XX.Xx EV/EBITDA implies $XX per share
- Bull Case Target: $XX per share
```

**Base Case (300-500 words)**
```
Base Case: [Title describing most likely scenario]

Probability: XX%

Key Assumptions:
[Similar structure to Bull Case with base assumptions]

Rationale:
[Explain why this is most likely scenario]

Valuation:
- DCF Value: $XX per share
- Trading Comps: $XX per share
- Base Case Target: $XX per share (weighted average)
```

**Bear Case (500-700 words)**
```
Bear Case: [Title describing downside scenario]

Probability: XX%

Key Assumptions:
[Similar structure with downside parameters]

Downside Triggers:
1. [Specific risk event] - Likelihood: [%]
2. [Specific risk event] - Likelihood: [%]
3. [Specific risk event] - Likelihood: [%]

Rationale:
[200-300 words on what would cause bear case]

Valuation Implications:
- DCF Value: $XX per share (XX% downside from current)
- Trading Comps: $XX per share
- Bear Case Target: $XX per share
```

**Scenario Comparison (200-300 words)**
- Comprehensive comparison table with key metrics
- Analysis of probability-weighted outcomes
- Risk/reward assessment
- Path dependency discussion

**Growth Drivers (800-1,200 words)**
- 3-5 key growth drivers
- Each quantified with specific opportunity size
- Timeline and milestones
- Supporting data from model

### Pages 31-36: Industry Overview

Copied from Task 1 like Company 101, and placed after Financial Analysis, not inside Company 101.

**Industry Overview (800-1,200 words)**
- Industry definition and scope
- Market size and growth
- Key trends
- Regulatory environment

**Competitive Landscape (700-1,000 words)**
- 5-10 key competitors
- Market positioning
- Competitive advantages
- Market share analysis

**TAM Analysis (500-700 words)**
- Total addressable market sizing
- Market growth projections
- Company's serviceable market

### Pages 37-40: Valuation Analysis

**Valuation Methodology (800-1,200 words)**

**DCF Analysis (300-400 words)**
- Methodology explanation
- Key assumptions:
  - WACC: X.X% (calculation breakdown)
  - Terminal growth: X.X% (rationale)
  - Terminal margin: XX% (justification)
- Sensitivity analysis discussion
- DCF value: $XX per share

**Comparable Companies (300-400 words)**
- Peer selection rationale (why these 5-10 companies)
- Statistical summary (max/75th/median/25th/min)
- Multiple selection (why EV/EBITDA vs. EV/Revenue vs. P/E)
- Premium/discount justification (why target deserves premium/discount)
- Comparable companies value: $XX per share

**Precedent Transactions (200-300 words, if applicable)**
- Transaction relevance
- Control premium analysis
- Precedent transactions value: $XX per share

**Valuation Reconciliation (200-300 words)**
- Weighting rationale (e.g., DCF 50%, Comps 40%, Precedent 10%)
- Weighted average calculation
- Valuation range (low/base/high)
- Final price target: $XX

**Price Target & Recommendation (300-500 words)**
- Final rating (Buy/Hold/Sell) beside its action verb
- Price target: $XX (XX% upside from current $XX)
- Time horizon: 12 months
- Key catalysts (3-5 with specific timeframes)
- Key risks to price target (3-5 with impact quantification)

### Pages 41+: Appendices

**Data Sources & References**
- All sources listed with dates
- Organized by category:
  - SEC Filings (with EDGAR links)
  - Earnings Calls (with transcript links)
  - Company Materials
  - Industry Reports
  - News Articles
- **ALL URLs must be clickable hyperlinks**

**Detailed Financial Model Assumptions**
- Comprehensive assumptions detail
- Calculation methodologies
- Data sources for historical figures

**Additional Supporting Tables**
- Extended financial projections
- Detailed comparable companies data
- Sensitivity analyses

---

## Report Assembly Philosophy

**CRITICAL PRINCIPLE 1**: A good equity research report is **text-dense with lots of illustrating images**.

**Target density**: 60-80% page coverage
- Every page should have BOTH text AND visuals
- Charts should be interspersed throughout text, not grouped
- Average 1 chart per page minimum
- Tables should break up large text blocks

**CRITICAL PRINCIPLE 2**: Use the docx and xlsx skills (`.agents/skills/docx/SKILL.md`, `.agents/skills/xlsx/SKILL.md`) to programmatically create the report.

**REQUIRED TOOLS** (Claude has built-in skills for these):
- **DOCX skill** - To create and manipulate Word documents
  - Read Task 1 .md file → Convert to Word formatting
  - Insert images from Task 4 chart files
  - Create tables
  - Format text, headers, footers, page numbers
  - Add hyperlinks
- **XLSX skill** - To read data from Excel files
  - Extract tables from Task 2 financial model
  - Read Task 3 valuation tabs
  - Pull historical financials from Task 1
- **Direct file operations** - Work with actual files
  - Read: `[Company]_Research_Document_[Date].md`
  - Read: `[Company]_Financial_Model_[Date].xlsx`
  - Read: `chart_01.png`, `chart_02.png`, etc.
  - Write: `[Company]_Initiation_Report_[Date].docx`

**DO**: Use the docx and xlsx skills (`.agents/skills/docx/SKILL.md`, `.agents/skills/xlsx/SKILL.md`) to open files, extract data, and create the DOCX report

**Content Reuse Strategy**:
- **Task 1 content (40-50% of report)**: Read .md file → Convert to Word format → Add charts
- **Task 2/3 data (30-40% of report)**: Read .xlsx file → Extract tables → Write interpretation
- **Original writing (10-20% of report)**: Investment thesis, projection assumptions, scenario analysis

**This approach**:
- Maximizes efficiency (no rewriting 6-8K words that are already good)
- Maintains quality (Task 1 content is substantive, professional analysis)
- Focuses effort on value-add (quantitative interpretation and investment thesis)
- Uses actual files programmatically (not manual work)

---

## Assembly

Two rules decide what actually gets written, and both bind every step of the assembly:

**The mode's cut list wins.** Carry the Task 1 research document across in full, without rewriting, for every section the report mode keeps. The mode's cut list (`.agents/skills/initiating-coverage/references/report-modes.md`) decides which sections are kept; a section the mode cuts is left out, and a skeleton-mode section carries only what Task 1 established.

**A preliminary or watchlist initiation publishes no target.** Its Price Target and Recommendation section carries that label, the numbered evidence required before ownership can be underwritten, and the caveat on the valuation work the available inputs did support, in place of a target.

Now load `.agents/skills/initiating-coverage/references/task5-assembly-phases.md` and work through its six steps in order, from organizing the inputs to the DOCX build in phases A to G. Come back here for the quality check before delivering.

---

## Quality Check

**Run comprehensive verification:**

```
═══════════════════════════════════════════════════════════
REPORT QUALITY CHECKLIST
═══════════════════════════════════════════════════════════

LENGTH REQUIREMENTS:
- [ ] Page, word, chart and table counts recorded, each inside its range in
      Critical Sections with Word Counts (pages: ____, words: ____,
      charts: ____, tables: ____)

PAGE 1 FORMAT:
- [ ] "INITIATING COVERAGE" header present
- [ ] Thesis-focused title (not generic)
- [ ] Rating box complete with all elements
- [ ] Stock price chart (Figure 1) embedded
- [ ] 3-4 detailed bullets with ■ character
- [ ] Each bullet has **bold header** + 3-5 sentences
- [ ] Financial summary table included
- [ ] Years noted as "A" (actual) and "E" (estimate)

SECTION WORD COUNTS:
- [ ] Every section meets its minimum in Critical Sections with Word Counts,
      Projection Assumptions and Scenario Analysis included ⭐

MANDATORY CHARTS (4 TOTAL):
- [ ] Revenue by Product (stacked area) embedded ⭐
- [ ] Revenue by Geography (stacked bar) embedded ⭐
- [ ] DCF Sensitivity (heatmap) embedded ⭐
- [ ] Valuation Football Field embedded ⭐

MANDATORY TABLES:
- [ ] Page 1 financial summary table
- [ ] Full income statement (40-50 line items)
- [ ] Revenue by product table (20-30 rows)
- [ ] Revenue by geography table (15-20 rows)
- [ ] Comparable companies table with statistical summary ⭐
- [ ] DCF assumptions table
- [ ] Scenario comparison table
- [ ] Additional 5-13 tables

CITATIONS & HYPERLINKS:
- [ ] All figures have source lines
- [ ] All tables have source lines
- [ ] All URLs are clickable hyperlinks (NOT plain text)
- [ ] Open every unique hyperlink target once to verify it works. List every unverified link by name in the delivery. Then test 5-10 random hyperlinks as an extra check.
- [ ] Data Sources & References page included
- [ ] All sources have dates

DATA ACCURACY:
- [ ] All numbers match financial model exactly
- [ ] Revenue figures consistent across all tables/text
- [ ] Price target matches valuation analysis
- [ ] All growth rates calculated correctly
- [ ] All percentages sum to 100% where applicable

CONTENT REUSE (CRITICAL):
- [ ] Task 1 content used almost verbatim (not rewritten)
- [ ] Company 101 (pages 6-17) and Industry Overview (pages 31-36) sections copied from Task 1 with only formatting changes
- [ ] Writing effort focused on quantitative sections (financial analysis, projections, scenarios)

VISUAL DENSITY (CRITICAL):
- [ ] Every page has BOTH text AND visuals (not pure text pages)
- [ ] Charts interspersed throughout (not grouped at end)
- [ ] Average 1+ chart per page
- [ ] Charts appear every 200-300 words of text
- [ ] 60-80% page density achieved across entire report

FORMATTING:
- [ ] No markdown syntax visible (no #, ##, **, etc.)
- [ ] Professional fonts throughout
- [ ] Headers and footers present
- [ ] Page numbers present
- [ ] Section breaks appropriate
- [ ] Charts embedded (not just file paths)
- [ ] Tables formatted professionally

WRITING QUALITY:
- [ ] Lead with numbers (not generic statements)
- [ ] Use "vs." not "versus"
- [ ] Quantify everything
- [ ] Professional tone throughout
- [ ] No typos or grammatical errors
- [ ] Specific examples (not vague statements)

═══════════════════════════════════════════════════════════
FINAL VERIFICATION
═══════════════════════════════════════════════════════════

IF ALL ITEMS CHECKED: ✓ READY FOR DELIVERY

IF ANY ITEMS UNCHECKED: ✗ FIX BEFORE DELIVERY

═══════════════════════════════════════════════════════════
```

**IF ANY ITEM FAILS, DO NOT DELIVER. Fix before proceeding.**

---

## Writing Style Guidelines

### Lead with Numbers (CRITICAL)

✓ **CORRECT**: "Revenue increased 150% YoY to $250M in Q4 2024, driven by..."
✗ **INCORRECT**: "The company saw strong revenue growth this quarter..."

✓ **CORRECT**: "EBITDA margin expanded 500bps to 30% vs. 25% in FY2023"
✗ **INCORRECT**: "EBITDA margin expanded versus the prior year"

✓ **CORRECT**: "Market share increased 3 percentage points to 18% vs. 15% in 2023"
✗ **INCORRECT**: "Market share increased compared to last year"

✓ **CORRECT**: "Management expects 40-50% revenue growth in FY2025E"
✗ **INCORRECT**: "Management expects strong revenue growth"

### Professional Writing Standards

- **Front-load**: Most important information first
- **Data-driven**: Lead with numbers and metrics
- **Specific**: Concrete examples, not generic statements
- **Objective**: Present facts, acknowledge risks
- **Confident**: State views clearly with supporting evidence
- **Active voice**: "We estimate revenue will reach $500M"
- **Precise**: Avoid "might", "could", "possibly"

### Number Formatting

**Consistency:**
- Billions: $X.XB (e.g., "$2.5B")
- Millions: $XXXM (e.g., "$250M")
- Always specify: YoY, QoQ, CAGR
- Basis points for small margin changes: "500bps"
- Year format: "2024A" (actual), "2025E" (estimate)

### Use "vs." not "versus"
✓ **CORRECT**: "Gross margin of 65% vs. 60% in prior year"
✗ **INCORRECT**: "Gross margin of 65% versus 60%"

---

## Output Files

**Primary Deliverable:**
`[Company]_Initiation_Report_[Date].docx`

**Example**: `Tesla_Initiation_Report_2024-10-27.docx`

**Supporting Deliverable:**
`[Company]_Financial_Model_[Date].xlsx` (from Task 2)

**Both files should be packaged together for final delivery.**
