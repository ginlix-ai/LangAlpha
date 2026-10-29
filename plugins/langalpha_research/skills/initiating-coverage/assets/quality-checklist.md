# Quality Control Checklist for Initiation Reports

Before delivering an initiation report, verify all items below are complete.

## Critical Minimums - Reports Must Meet These

**CRITICAL DO NOT DELIVER IF:**
- ❌ DOCX report fewer than 30 pages → INCOMPLETE
- ❌ Fewer than 25 embedded charts → INCOMPLETE
- ❌ Fewer than 12 comprehensive tables → INCOMPLETE
- ❌ Fewer than 10,000 words → INCOMPLETE
- ❌ No XLS financial model → MISSING DELIVERABLE
- ❌ Charts are text descriptions, not actual PNG/JPG files → MAJOR FAILURE

## Where the words go

The minimums above are floors against a four-page stub, not a licence to pad. They are met in the **analytical** sections, the ones that carry the argument: investment thesis and debates, projection assumptions, scenario analysis, valuation, risks, and the competitive assessment. A report that reaches 10,000 words on company history and market sizing has met the count and failed the reader.

**What to cut, and in what order**: `.agents/skills/research-conventions/references/depth.md`. Two additions specific to an initiation:

- Management biographies run 300 to 400 words each only while governance or succession is one of the debates. Where it is not, keep the role, the tenure and the track record against stated targets, and move the rest to the appendix.
- Definitions and industry primers stay only where the mode requires them (`.agents/skills/initiating-coverage/references/report-modes.md`): a publishable initiation needs the primer, a buy-side deep dive does not.

When a section is cut, the words move into the analytical sections rather than out of the report.

## Deliverables Checklist

- [ ] DOCX report file created
- [ ] XLS financial model file created
- [ ] Both files named properly: `[Company]_Initiation_Report_[Date].docx` and `[Company]_Financial_Model_[Date].xlsx`

## DOCX Report - Length & Content

**Length Verification:**
- [ ] Report is 30-50 pages (count pages in final document)
- [ ] Word count is 10,000-15,000 words
- [ ] If under 30 pages: STOP and add more content

**Visual Elements:**
- [ ] 25-35 charts embedded (count them: _____ charts)
- [ ] All charts are actual PNG/JPG image files (NOT text descriptions)
- [ ] 12-20 comprehensive tables included (count them: _____ tables)
- [ ] Charts and tables interspersed throughout, not grouped at end

**Chart Requirements:**
- [ ] Revenue by product chart: Stacked Area format ✓
- [ ] Revenue by geography chart: Stacked Bar format ✓
- [ ] DCF sensitivity: 2-way Heat Map with color coding ✓
- [ ] Valuation football field: Horizontal bar chart ✓
- [ ] All other charts are actual image files ✓

**Table Requirements:**
- [ ] Full Income Statement (40-50 rows) with 5 years historical + 5 years projected
- [ ] Full Cash Flow Statement (30-40 rows)
- [ ] Full Balance Sheet (35-45 rows)
- [ ] Revenue by product table (20-30 rows)
- [ ] Revenue by geography table (15-20 rows)
- [ ] Revenue by channel table (10-15 rows)
- [ ] Comparable companies table with statistical summary (max/75th/median/25th/min)
- [ ] DCF calculation table (30-40 rows)
- [ ] WACC calculation table (8-10 rows)
- [ ] Two sensitivity tables
- [ ] 2-3 additional financial/competitive tables

## DOCX Report - Structure

**Page 1 Requirements:**
- [ ] "INITIATING COVERAGE" header present (NOT "Company Update")
- [ ] Thesis-focused title (NOT event-driven like "Strong Q4 Results")
- [ ] Rating box with rating, price, target price, 52-week range, market cap, EV
- [ ] 3-4 paragraph-length bullets with ■ character and bold headers
- [ ] Financial & valuation metrics table with 2-3 years historical, 2 years projected
- [ ] Table shows "A" suffix for actuals, "E" suffix for estimates
- [ ] Source lines on all visuals

**Content Sections:**
- [ ] Table of Contents (Page 2)
- [ ] Investment Thesis & Risks (3-5 pages)
- [ ] Company Overview (6-12 pages) including:
  - [ ] Company description
  - [ ] History and milestones
  - [ ] Management bios (300-400 words EACH for 3-4 executives)
  - [ ] Products/services detail
  - [ ] Competitive landscape
- [ ] Financial Analysis & Projections (10-15 pages)
- [ ] Valuation Analysis (8-12 pages)
- [ ] Assumptions section (2,000-3,000 words documenting ALL projection assumptions)
- [ ] Scenario Analysis (1,500-2,000 words with Bull/Base/Bear parameters)
- [ ] Appendices including Data Sources & References page

## DOCX Report - Formatting

**Figure & Table Formatting:**
- [ ] Every figure has caption above: "Figure X - [Company] [Descriptive Title]"
- [ ] Every figure has source line below: "Source: [Specific sources with dates]"
- [ ] Sequential figure numbering (Figure 1, 2, 3... no gaps)
- [ ] Every table has header row with shading
- [ ] Every table has source line at bottom
- [ ] All years use "A" for actual, "E" for estimate notation

**Professional Formatting:**
- [ ] Consistent fonts throughout (Calibri, Arial, or similar)
- [ ] Headers and footers with page numbers
- [ ] Dense layout: 60-80% page coverage, minimal white space
- [ ] Every page has both text AND visuals (charts or tables)
- [ ] Professional business report template used

## Citations & Sources ⭐⭐⭐ CRITICAL

**Source Attribution:**
- [ ] Every figure has specific source with document name and date
- [ ] Every table has specific source with document reference
- [ ] Key statistics throughout text have footnotes with sources
- [ ] NOT just generic "Company data" - must be specific

**Hyperlinks:** ⭐⭐⭐ MANDATORY
- [ ] ALL URLs are CLICKABLE HYPERLINKS (not plain text)
- [ ] SEC filings hyperlinked to EDGAR viewer
- [ ] Earnings transcripts hyperlinked to the source they were retrieved from
- [ ] Press releases hyperlinked to company IR page
- [ ] Presentations hyperlinked to PDF URLs
- [ ] Industry reports hyperlinked (if publicly available)
- [ ] Data behind a paywall noted as "(subscription required)" with the provider named
- [ ] No raw URLs displayed anywhere - all formatted as hyperlinks
- [ ] Open every unique hyperlink target once to verify it works. List every unverified link by name in the delivery. Then test 3-5 sample hyperlinks as an extra check (Ctrl+Click).

**Reference Page:**
- [ ] "Data Sources & References" page at end of report
- [ ] Lists ALL sources used in report
- [ ] Sources organized by category (SEC Filings, Earnings Transcripts, etc.)
- [ ] Every source has date
- [ ] Every source has clickable hyperlink (where applicable)

## XLS Financial Model - Structure

**File Structure** (the workbook manifest in `.agents/skills/initiating-coverage/SKILL.md`, restated so this gate runs standalone):
- [ ] Present, in this order: Cover, Revenue Model, Income Statement, Cash Flow Statement, Balance Sheet, Scenarios, DCF Inputs, DCF, Sensitivity Analysis, Comparable Companies, Valuation Summary, Checks
- [ ] Precedent Transactions present where the deal set supports one, between Comparable Companies and Valuation Summary
- [ ] Cover is the first visible sheet and Checks is the last

**Formatting:**
- [ ] Blue text for hardcoded inputs
- [ ] Black text for formulas
- [ ] Green text for links to other sheets
- [ ] Professional formatting with borders and shading
- [ ] Clear section headers and labels

**Model Functionality:**
- [ ] All numbers flow (change assumption → entire model updates)
- [ ] DCF links to assumptions and projections
- [ ] No circular references or errors
- [ ] All important cells/ranges are named
- [ ] Sensitivity tables work dynamically

## XLS Financial Model - Content

**Projections:**
- [ ] 3-5 years historical data
- [ ] 5 years forward projections (FY+1 through FY+5)
- [ ] Revenue broken down by product, geography, channel
- [ ] Full P&L with 40-50 line items
- [ ] Full cash flow with 30-40 line items
- [ ] Full balance sheet with 35-45 line items

**Valuation:**
- [ ] Complete DCF model with all calculations shown
- [ ] WACC calculation with all components
- [ ] Terminal value calculation
- [ ] Comparable companies analysis (5-10 companies)
- [ ] Precedent transactions analysis (5-10 deals) where the deal set supports one
- [ ] Scenario analysis (Bull/Base/Bear)
- [ ] Two sensitivity tables

## Cross-File Consistency

**CRITICAL**: Numbers must match EXACTLY between DOCX and XLS

- [ ] Revenue numbers match across both files
- [ ] EPS numbers match across both files
- [ ] Margin percentages match across both files
- [ ] Valuation numbers match across both files
- [ ] Price target matches across both files
- [ ] All projected years match across both files

**Verification Method**: Reconcile every model-backed figure in the DOCX report to its XLS model cell. Label each figure with no model cell as such. List every unverified figure by name in the delivery. Then spot check 10-15 key numbers as an extra check.

## Content Quality

**Investment Thesis:**
- [ ] 3-5 clear thesis pillars
- [ ] Each pillar supported with specific data and quantification
- [ ] Financial impact quantified for each pillar
- [ ] Catalysts identified with timelines

**Analysis Depth:**
- [ ] Comprehensive business model analysis
- [ ] Detailed competitive assessment
- [ ] 3-5 year financial trends analyzed
- [ ] 8-12 risks identified and quantified
- [ ] Management team analyzed (300-400 words per executive)

**Assumptions:**
- [ ] 2,000-3,000 words documenting ALL assumptions
- [ ] Revenue growth assumptions by category/geography
- [ ] Margin assumptions with bridge showing drivers
- [ ] Working capital assumptions
- [ ] CapEx assumptions
- [ ] Each assumption has specific quantification

**Scenarios:**
- [ ] 1,500-2,000 words on scenario analysis
- [ ] Bull case with specific parameters and catalysts
- [ ] Base case with detailed rationale
- [ ] Bear case with specific triggers
- [ ] Probability assessments for each scenario

## Posture, Evidence and Argument

**Cover fields:**
- [ ] Initiation mode named (`.agents/skills/initiating-coverage/references/report-modes.md`)
- [ ] Evidence confidence stated
- [ ] Underwriting status stated separately from evidence confidence
- [ ] Rating and target present, or the preliminary/watchlist label present with no target price
- [ ] Where preliminary or watchlist: the numbered list of evidence required before ownership

**Evidence:**
- [ ] Source register complete: id, name, type, publication date, access date, period covered, location in the document, tier, stale flag
- [ ] Every material claim carries an evidence label (`.agents/skills/research-conventions/references/evidence.md`)
- [ ] No `needs-source` label survives into the delivered report
- [ ] Conflicts between sources shown with the selection and its reason, never averaged and never picked silently
- [ ] Stale-data triggers checked: market data current as of the report date; the latest fiscal period included; post-earnings rather than pre-earnings estimates where the company has reported; transcript no older than one quarter for any current-setup claim; macro and industry data predating a structural event flagged

**Market data completeness (Task 3):**
- [ ] Current price with as-of, market cap, fully diluted share count, EV bridge inputs and consensus context all retrieved, or each gap labelled with the conclusion it blocks

**Argument (`.agents/skills/initiating-coverage/references/argument-standards.md`):**
- [ ] 3 to 5 thesis pillars, each with evidence, quantified consequence, counterargument and a monitored signal
- [ ] Key-debates table present, last column naming what would change our mind, with a date
- [ ] Each risk carries why it matters to this thesis, a leading indicator, a mechanical downside and a monitoring plan
- [ ] Catalysts separated into hard-dated, recurring, soft, thesis milestones, regulatory and estimate-revision groups
- [ ] 2 to 5 falsifiable claims, each with evidence, implication, test metric, falsifier and time to knowable
- [ ] Section headings state findings rather than topics

**Valuation (`.agents/skills/initiating-coverage/references/valuation-methodologies.md`):**
- [ ] Every method used carries its mandatory caveat
- [ ] Target-price bridge complete: all eight elements
- [ ] EPS basis for any P/E target stated and defended
- [ ] Revenue multiples labelled preliminary market context wherever the capital stack sits outside the comparison
- [ ] Financed-growth gate passed, or the preliminary posture taken and the missing element named
- [ ] Any multi-year exit-multiple case discounted, with the rate or the hurdle stated

## Writing Quality

**Style:**
- [ ] Lead with numbers ("Revenue grew 15% to $1.2B" not "Strong revenue")
- [ ] Use "vs." not "versus"
- [ ] Be direct and concise
- [ ] Professional institutional tone throughout
- [ ] No informal language

**Accuracy:**
- [ ] No typos in ticker symbol
- [ ] No typos in company name
- [ ] All dates accurate
- [ ] All calculations verified
- [ ] Charts match text descriptions
- [ ] All numbers properly formatted ($ signs, % signs, commas)

## The Senior Lens

Ten questions to answer before delivering. They are not checkboxes: each one is answered out loud, and an answer that cannot be given sends the section back.

1. Does this change how a portfolio manager would size the position, or only how they would describe the company?
2. Is the thesis different from what the market already believes, and where exactly does it differ?
3. Is the downside credible and specific, with a line item and a number, or is it a list of things that could go wrong?
4. Which single estimate line matters most to the target, and how confident are we in it?
5. Which operating metric is the tell, the one that moves first when the thesis is working or failing?
6. Where is management most likely to disappoint, and what would that cost?
7. What makes us admit we are wrong, and by when would we know?
8. If the target price is removed, does the report still say something useful?
9. Which part of this would a competitor's analyst attack first, and is that attack answered?
10. Would a reader who stops after page 1 have been told the truth?

## Pre-Delivery Final Check

Run through this quick final review:

1. **Deliverables**: Both DOCX and XLS files created ✓
2. **Length**: DOCX is 30-50 pages ✓
3. **Charts**: 25-35 actual PNG/JPG files embedded ✓
4. **Tables**: 12-20 comprehensive tables included ✓
5. **Words**: 10,000-15,000 words ✓
6. **Hyperlinks**: Open every unique hyperlink target once to verify it works. List every unverified link by name in the delivery. Then test 3-5 hyperlinks as an extra check. ✓
7. **Cross-check**: Reconcile every model-backed figure in the DOCX report to its XLS model cell. Label each figure with no model cell as such. List every unverified figure by name in the delivery. Then spot check 10 numbers as an extra check. ✓
8. **Page 1**: "INITIATING COVERAGE" header present ✓

If ANY item fails, DO NOT DELIVER. Go back and fix.

## Actual Count Verification

**Before delivery, fill in actual counts:**

DOCX Report:
- Page count: _____ pages (MUST BE 30-50)
- Chart count: _____ charts (MUST BE 25-35)
- Table count: _____ tables (MUST BE 12-20)
- Word count: _____ words (MUST BE 10,000-15,000)

XLS Model:
- Tab count: _____ tabs (MUST BE 12, or 13 with Precedent Transactions)
- Model years: _____ historical + _____ projected

If any count is below minimum, STOP and add content before delivery.
