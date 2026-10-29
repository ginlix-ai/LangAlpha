# Task 5: Assembly Phases

The assembly sequence for Task 5: six steps ending in the DOCX build.

Read `.agents/skills/initiating-coverage/references/task5-report-assembly.md` first. It holds the report structure, the per-section word counts (Critical Sections with Word Counts), the mode cut list, the preliminary-initiation branch, and the quality check this sequence ends on. Word counts and section content are not repeated here.

## Step 1: Organize All Inputs and Verify Files

**Verify all input files exist:**

Check:
- `[Company]_Research_Document_[Date].md` (Task 1)
- `[Company]_Historical_Financials_[Date].xlsx` (Task 1)
- `[Company]_Financial_Model_[Date].xlsx` (Task 2 with Task 3 tabs)
- `[Company]_Valuation_Analysis_[Date].md` (Task 3)
- `[Company]_Charts_[Date].zip` (Task 4) - **Extract this first**

**Step 1a: Extract Charts from Zip File**

Before proceeding, extract all chart files from the Task 4 zip:
- Locate `[Company]_Charts_[Date].zip`
- Extract all contents to a working directory (e.g., `task4_charts/`)
- Verify 25-35 PNG files were extracted
- Verify chart_index.txt is present

**Expected folder structure after extraction:**
```
[Company]_Report_Working/
├── [Company]_Research_Document_[Date].md
├── [Company]_Historical_Financials_[Date].xlsx
├── [Company]_Financial_Model_[Date].xlsx (Task 2 and Task 3 sheets, per the workbook manifest)
├── [Company]_Valuation_Analysis_[Date].md
├── [Company]_Charts_[Date].zip
├── task4_charts/ (extracted from zip)
│   ├── chart_01_stock_price.png
│   ├── chart_02_revenue_growth.png
│   ├── chart_03_revenue_by_product.png ⭐
│   ├── chart_04_revenue_by_geography.png ⭐
│   ├── ... (21-31 more charts)
│   ├── chart_28_dcf_sensitivity.png ⭐
│   ├── chart_32_valuation_football_field.png ⭐
│   └── chart_index.txt
└── sources_and_urls.txt
```

**Open and inspect files using Claude skills:**

1. **Read Task 1 markdown file** - Use Read tool to view content
2. **Open Task 2/3 Excel file** - Use XLSX skill to inspect tabs:
   - Verify every sheet of the workbook manifest exists (`.agents/skills/initiating-coverage/SKILL.md`)
3. **Read Task 3 markdown file** - Use Read tool to view valuation analysis
4. **Check chart files** - Verify all 25-35 PNG files present

**Note**: Task 2's financial model file now contains both the original modeling tabs (from Task 2) AND the valuation tabs (added by Task 3). This single Excel file contains all quantitative data needed for report assembly.

## Step 2: Extract Tables from Excel Using XLSX Skill

**Use the xlsx skill (`.agents/skills/xlsx/SKILL.md`) to extract data from Excel files:**

### Table 1: Page 1 Summary Financials

Use XLSX skill to:
1. Open `[Company]_Financial_Model_[Date].xlsx`
2. Read from `Income Statement` tab
3. Extract key rows: Revenue, Gross Profit, EBITDA, Net Income, EPS, FCF
4. Extract years: 2022A, 2023A, 2024A, 2025E, 2026E, 2027E
5. Create summary table with growth rates and margins

### Table 2: Full Income Statement (40-50 line items)

Use XLSX skill to:
1. Open `[Company]_Financial_Model_[Date].xlsx`
2. Read entire `Income Statement` tab
3. Extract all line items (40-50 rows)
4. Extract columns for historical (2020A-2024A) + projected years (2025E-2029E)
5. Include all margins and growth rates

### Table 3: Revenue by Product (20-30 rows)

Use XLSX skill to:
1. Open `[Company]_Financial_Model_[Date].xlsx`
2. Read from `Revenue Model` tab
3. Navigate to product section (typically starts ~row 5)
4. Extract 20-30 rows showing each product category
5. Include columns: Product name, historical years, projected years, % of Total, YoY Growth

### Table 4: Revenue by Geography (15-20 rows)

Use XLSX skill to:
1. Open `[Company]_Financial_Model_[Date].xlsx`
2. Read from `Revenue Model` tab
3. Navigate to geography section (typically starts ~row 40)
4. Extract 15-20 rows showing each geographic region
5. Include columns: Region, historical years, projected years, % of Total, YoY Growth

### Table 5: Comparable Companies
**Extract from:** Task 3 valuation tabs in Task 2's financial model (`Comparable Companies` tab)

Use XLSX skill to:
1. Open `[Company]_Financial_Model_[Date].xlsx`
2. Read from `Comparable Companies` tab (added by Task 3)
3. Extract full table with company names as row headers
4. **CRITICAL**: Verify statistical summary rows are present at bottom:
   - Maximum
   - 75th Percentile
   - Median
   - 25th Percentile
   - Minimum
5. If statistical summary is missing, report ERROR

**Expected format:**
```
Company      Ticker  Mkt Cap  EV/Rev  EV/Rev  EV/EBITDA  EV/EBITDA  P/E   Rev     EBITDA
                     ($B)     LTM     NTM     LTM        NTM        NTM   Growth  Margin
[5-10 peers plus target, then statistical summary]
```

### Additional Tables (7-15 more)
**Extract from Task 2 financial model (with Task 3 tabs):**

Use XLSX skill to extract these tables:

**DCF Assumptions Table** (Task 3 `DCF` tab)
- Open `[Company]_Financial_Model_[Date].xlsx`
- Read from DCF tab
- Extract columns A-C (Assumption, Value, Source)
- Extract first 20 rows

**DCF Sensitivity Matrix** (Task 3 `Sensitivity Analysis` tab)
- Read from Sensitivity Analysis tab
- Extract full sensitivity matrix
- WACC values as row headers
- Terminal growth rates as column headers

**Scenario Comparison Table** (Task 2 `Scenarios` tab)
- Read from Scenarios tab
- Extract full scenario table
- Metrics as row headers (Revenue, EBITDA, Margins, etc.)
- Columns: Bull, Base, Bear

**Other supporting tables to extract:**
- Cash flow statement
- Balance sheet highlights
- Key metrics dashboard
- Margin bridge
- Working capital schedule
- TAM sizing table
- Market share table

**Create every table with proper formatting, to the table count in Critical Sections with Word Counts.**

## Step 3: Write Quantitative Sections

These sections interpret the financial model.

**Write in this order:**

### A. Financial Analysis
- Analyze historical performance from model
- Discuss trends in revenue, margins, cash flow
- Reference specific charts and tables
- Lead with numbers

### B. Projection Assumptions ⭐ CRITICAL
- Follow the detailed structure in Report Structure
- Must be product-by-product (8-12 points per product)
- Must be region-by-region (6-8 points per region)
- Must include margin, opex, capex, working capital assumptions

### C. Scenario Analysis ⭐ CRITICAL
- Follow the detailed structure in Report Structure
- Bull case: specific parameters, catalysts, probability, valuation
- Base case: most likely scenario with rationale
- Bear case: downside triggers and parameters
- Comparison table and analysis
- **Must have specific quantified parameters for each scenario**

### D. Growth Drivers
- 3-5 key drivers with quantified opportunities
- Timeline and milestones
- Evidence from model

### E. Valuation Methodology
- DCF explanation with assumptions
- Comparables rationale
- Precedent transactions (if applicable)
- Reconciliation and weighting
- Price target derivation

## Step 4: Write Synthesis Sections

**Write in this order:**

### A. Investment Thesis
- 3-5 key pillars
- Each pillar: 200-300 words
- Lead with key statistic
- Quantify financial impact
- Include timeline

### B. Risk Assessment
- Pull from Task 1 research document
- Organize into 4 categories
- 8-12 risks total
- Each risk: 50-100 words

### C. Price Target & Recommendation
- Final recommendation
- Price target with upside %
- Key catalysts with timeframes
- Key risks to target

### D. Investment Summary - WRITE LAST
- Page 1 content
- 3-4 detailed bullets with bold headers
- Complete synthesis of all findings
- **Write this section LAST after full analysis complete**

## Step 5: Integrate Company Content from Task 1

Which Task 1 sections are carried across, and how a cut or a skeleton changes that, is the mode cut list in `.agents/skills/initiating-coverage/references/task5-report-assembly.md`.

**The company research from Task 1 (6-8K words) is already professional, substantive analysis. Objective:**
1. **Reformat for Word** - Convert markdown to DOCX formatting
2. **Insert charts inline** - Add relevant charts from Task 4 throughout the text
3. **Minor style adjustments** - Ensure consistent formatting with rest of report

**Extract these sections from Task 1 research document:**
- Company description → **Use verbatim, insert company overview charts**
- Company history → **Use verbatim, insert timeline chart**
- Management bios → **Use verbatim, insert org chart if available**
- Products & services → **Use verbatim, insert product portfolio charts**
- Customers & GTM → **Use verbatim, insert customer segmentation charts**
- Industry overview → **Use verbatim, insert market size evolution charts**
- Competitive landscape → **Use verbatim, insert competitive positioning charts**
- TAM analysis → **Use verbatim, insert TAM sizing charts**
- Risk assessment → **Use verbatim, format as Investment Thesis & Risks section**

**Chart Integration Strategy:**
- Every 200-300 words of text → Insert 1 chart
- Company 101 (pages 6-17) carries 6-8 charts and Industry Overview (pages 31-36) carries 6, per the chart allocation in `.agents/skills/initiating-coverage/assets/report-template.md`
- Place charts immediately after the paragraph that discusses the topic
- **Result**: Dense, visually rich pages (60-80% coverage)

## Step 6: Assemble DOCX Report

**CRITICAL**: Create actual DOCX file, NOT markdown.

**Assembly Order (Most Efficient):**

### Phase A: Create Structure & Add Page 1
1. Create DOCX document
2. Set up professional styling (fonts, headers, footers)
3. Create Page 1 - Investment Summary (write this LAST after all analysis complete)
4. Add Table of Contents placeholder

### Phase B: Copy Task 1 Content + Insert Charts
**This is 40-50% of the report - mostly copy/paste + chart insertion**

Use the docx skill (`.agents/skills/docx/SKILL.md`) to:

1. **Initialize new DOCX document**
   - Create new Word document
   - Set professional styling (fonts, margins)

2. **Read Task 1 markdown file**
   - Use Read tool: `[Company]_Research_Document_[Date].md`
   - Identify sections by markdown headers (## Section Title)

3. **Extract and convert each section from Task 1 to Word format:**

**SECTION 1: Investment Thesis & Risks**
- Add heading: 'Investment Thesis & Risks' (level 1)
- Extract 'Risk Assessment' section from Task 1 markdown
- Convert markdown formatting to Word formatting (remove ##, **, etc.)
- Add paragraphs to Word document (split by blank lines)
- Add new 'Investment Thesis' heading (level 2)
- Write new investment thesis content based on all analysis

**SECTION 2: Company 101 (Pages 6-17)**
Copy each section from Task 1 verbatim with formatting conversion:

- **Company Overview**
  - Add heading: 'Company Overview' (level 1)
  - Extract 'Company Overview' section from Task 1
  - Convert to Word paragraphs
  - Insert chart: `task4_charts/chart_05_company_overview.png` (6 inches wide)

- **Company History**
  - Add heading: 'Company History' (level 1)
  - Extract 'Company History' section from Task 1
  - Convert to Word paragraphs
  - Insert chart: `task4_charts/chart_06_key_milestones_timeline.png` (6 inches wide)

- **Management Team**
  - Add heading: 'Management Team' (level 1)
  - Extract 'Management Team' section from Task 1
  - Convert to Word paragraphs
  - Insert chart: `task4_charts/chart_07_organizational_structure.png` (5 inches wide)

- **Products & Services**
  - Add heading: 'Products & Services' (level 1)
  - Extract 'Products & Services' section from Task 1
  - Add first paragraph
  - Insert chart: `task4_charts/chart_08_product_portfolio.png` (6 inches wide)
  - Add remaining paragraphs

- **Customers & Go-to-Market**
  - Add heading: 'Customers & Go-to-Market' (level 1)
  - Extract section from Task 1
  - Convert to Word paragraphs
  - Insert chart: `task4_charts/chart_09_customer_segmentation.png` (6 inches wide)

**Result after Phase B**: Pages 6-17 complete (~12 pages, 6-8 charts embedded); the Industry Overview sections from Task 1 wait for Phase D

**Key Point**: Use DOCX skill to READ from Task 1's .md file and INSERT actual image files. No manual copy/paste required.

### Phase C: Add Financial Analysis with Data from Task 2
**This requires NEW WRITING interpreting quantitative data**

Use the docx and xlsx skills (`.agents/skills/docx/SKILL.md`, `.agents/skills/xlsx/SKILL.md`) to:

**SECTION 3: Financial Analysis (Pages 21-30)**

1. **Add section heading: 'Financial Analysis' (level 1)**

2. **Historical Financial Analysis - NEW WRITING**
   - Add heading: 'Historical Performance' (level 2)
   - Use XLSX skill to open `[Company]_Financial_Model_[Date].xlsx`
   - Read `Income Statement` tab to extract historical data
   - Read `Revenue Model` tab to extract revenue trends
   - Calculate key metrics (e.g., Revenue CAGR from 2020-2024)
   - Write analytical paragraphs interpreting the trends
   - Lead with specific numbers: "Revenue grew from $XXM in 2020 to $XXM in 2024, representing a XX% CAGR. This growth was driven by..."
   - Insert chart: `task4_charts/chart_02_revenue_growth_trajectory.png` (6 inches wide)

3. **Create Table: Full Income Statement**
   - Add heading: 'Historical Income Statement' (level 3)
   - Use XLSX skill to extract entire Income Statement tab (40-50 rows)
   - Create Word table with all columns (historical years 2020A-2024A + projected 2025E-2029E)
   - Include all line items: Revenue, COGS, Gross Profit, Operating Expenses, EBITDA, Net Income, etc.

4. **Add mandatory charts and tables for Revenue breakdown:**
   - Insert chart: `task4_charts/chart_03_revenue_by_product_stacked_area.png` (6.5 inches wide) ⭐ MANDATORY
   - **Create Table: Revenue by Product (20-30 rows)**
     - Use XLSX skill to extract from Revenue Model tab (product section, typically rows 5-35)
     - Create Word table showing each product category with historical and projected years
     - Include columns: Product name, historical years, projected years, % of Total, YoY Growth

   - Insert chart: `task4_charts/chart_04_revenue_by_geography_stacked_bar.png` (6.5 inches wide) ⭐ MANDATORY
   - **Create Table: Revenue by Geography (15-20 rows)**
     - Use XLSX skill to extract from Revenue Model tab (geography section, typically rows 40-60)
     - Create Word table showing each geographic region with historical and projected years

5. **Add additional financial charts:**
   - Insert chart: `task4_charts/chart_10_gross_margin_evolution.png` (6 inches wide)
   - Insert chart: `task4_charts/chart_11_ebitda_margin_progression.png` (6 inches wide)
   - Insert chart: `task4_charts/chart_12_free_cash_flow_trend.png` (6 inches wide)

6. **Projection Assumptions ⭐ CRITICAL - NEW WRITING**
   - Add heading: 'Projection Assumptions' (level 2)
   - Use XLSX skill to read Scenarios tab to inform assumptions
   - Use XLSX skill to read Revenue Model tab for specific product/geography projections
   - Add heading: 'Revenue Assumptions by Product' (level 3)
   - Write detailed product-by-product assumptions (8-12 points per major product)
   - Write detailed region-by-region assumptions (6-8 points per major region)
   - Include margin, opex, capex, working capital assumptions
   - **Every assumption specific and quantified**

7. **Scenario Analysis ⭐ CRITICAL - NEW WRITING**
   - Add heading: 'Scenario Analysis' (level 2)
   - Use XLSX skill to extract scenario data from Scenarios tab
   - Extract Bull/Base/Bear parameters for key metrics (2029E Revenue, EBITDA Margin, etc.)
   - Write Bull Case: specific parameters, catalysts, probability, valuation
   - Write Base Case: most likely scenario with rationale
   - Write Bear Case: downside triggers, parameters, probability, valuation
   - Write Scenario Comparison
   - Insert chart: `task4_charts/chart_14_scenario_comparison.png` (6 inches wide)
   - **Create Table: Scenario Comparison**
     - Use XLSX skill to extract from Scenarios tab
     - Create Word table with Bull/Base/Bear columns and key metrics as rows

8. **Growth Drivers - NEW WRITING**
   - Add heading: 'Key Growth Drivers' (level 2)
   - Write 3-5 key drivers with specific quantified opportunities
   - Include timelines and milestones
   - Reference specific data from financial model

**Result after Phase C**: Pages 21-30 complete (~10 pages, 5-7K words, 7-8 charts, 6-8 tables)

**Key Point**: Use XLSX skill to READ data from Task 2's Excel file, use the data to inform NEW analytical writing, and use DOCX skill to create Word tables from Excel data.

### Phase D: Copy Industry Overview from Task 1
**Copy/paste plus chart insertion, the same treatment as Phase B; it sits after Financial Analysis in the page order**

Use the docx skill (`.agents/skills/docx/SKILL.md`) to:

**SECTION 4: Industry Overview (Pages 31-36)**
Copy each section from Task 1 verbatim with formatting conversion:

- **Industry Overview**
  - Add heading: 'Industry Overview' (level 1)
  - Extract section from Task 1
  - Add first paragraph
  - Insert chart: `task4_charts/chart_15_market_size_evolution.png` (6 inches wide)
  - Add remaining paragraphs
  - Insert chart: `task4_charts/chart_18_competitive_benchmarking.png` (6 inches wide)

- **Competitive Landscape**
  - Add heading: 'Competitive Landscape' (level 1)
  - Extract section from Task 1
  - Add first paragraph
  - Insert chart: `task4_charts/chart_16_competitive_positioning.png` (6 inches wide)
  - Add remaining paragraphs
  - Insert chart: `task4_charts/chart_17_market_share.png` (5 inches wide)

- **Market Opportunity**
  - Add heading: 'Market Opportunity' (level 1)
  - Extract 'Market Opportunity' section from Task 1
  - Convert to Word paragraphs
  - Insert chart: `task4_charts/chart_15_market_size_evolution.png` (6 inches wide)

**Result after Phase D**: Pages 31-36 complete (~6 pages, 6 charts embedded)

### Phase E: Add Valuation Analysis from Task 3
**Mix of copying Task 3 analysis + inserting data from Excel**

Use the docx and xlsx skills (`.agents/skills/docx/SKILL.md`, `.agents/skills/xlsx/SKILL.md`) to:

**SECTION 5: Valuation Analysis (Pages 37-40)**

1. **Add section heading: 'Valuation Analysis' (level 1)**

2. **Read Task 3 markdown file**
   - Use Read tool: `[Company]_Valuation_Analysis_[Date].md`
   - Identify sections by markdown headers: DCF Analysis, Comparable Companies, Price Target

3. **DCF Analysis section**
   - Add heading: 'DCF Analysis' (level 2)
   - Extract 'DCF Analysis' section from Task 3 markdown
   - Convert markdown to Word paragraphs
   - Insert chart: `task4_charts/chart_28_dcf_sensitivity_heatmap.png` (6 inches wide) ⭐ MANDATORY

   - **Create Table: DCF Key Assumptions**
     - Add heading: 'DCF Key Assumptions' (level 3)
     - Use XLSX skill to open `[Company]_Financial_Model_[Date].xlsx`
     - Read DCF tab (columns A-C, first 20 rows: Assumption, Value, Source)
     - Create Word table from extracted data

   - **Create Table: DCF Sensitivity Matrix**
     - Use XLSX skill to read Sensitivity Analysis tab
     - Extract full sensitivity matrix (WACC values as rows, terminal growth as columns)
     - Create Word table showing valuation at different parameter combinations

   - Insert chart: `task4_charts/chart_29_dcf_waterfall.png` (6 inches wide)

4. **Comparable Companies section**
   - Add heading: 'Comparable Companies Analysis' (level 2)
   - Extract 'Comparable Companies' section from Task 3 markdown
   - Convert markdown to Word paragraphs

   - **Create Table: Comparable Companies ⭐ CRITICAL**
     - Add heading: 'Comparable Companies' (level 3)
     - Use XLSX skill to read Comparable Companies tab
     - Extract full table including:
       - 5-10 peer companies plus target company
       - Statistical summary rows (Maximum, 75th Percentile, Median, 25th Percentile, Minimum)
     - Create Word table with all columns: Ticker, Market Cap, EV/Revenue (LTM & NTM), EV/EBITDA (LTM & NTM), P/E (NTM), Revenue Growth, EBITDA Margin
     - **Verify statistical summary is included in table**

   - Insert chart: `task4_charts/chart_31_peer_multiples_comparison.png` (6 inches wide)

5. **Valuation Summary**
   - Insert chart: `task4_charts/chart_32_valuation_football_field.png` (6.5 inches wide) ⭐ MANDATORY

   - **Create Table: Valuation Summary**
     - Use XLSX skill to read Valuation Summary tab
     - Extract valuation methods (DCF, Comps, Precedent Transactions if applicable)
     - Create Word table showing: Method, Low Case, Base Case, High Case, Weight, Weighted Value

6. **Price Target & Recommendation**
   - Add heading: 'Price Target and Recommendation' (level 2)
   - Extract 'Price Target' section from Task 3 markdown
   - Convert markdown to Word paragraphs
   - Should include: final rating (Buy/Hold/Sell) beside its action verb, price target with % upside, key catalysts, key risks
   - In a preliminary or watchlist initiation, follow the preliminary branch in `.agents/skills/initiating-coverage/references/task5-report-assembly.md` instead

**Result after Phase E**: Pages 37-40 complete (~4 pages, 3-4K words, 5-6 charts, 4-5 tables)

**Key Point**: Use Read tool for Task 3's .md file to get written analysis, and use XLSX skill to READ from Task 3's Excel tabs (which were added to Task 2's model file) to create quantitative tables.

### Phase F: Add Appendices & Finalize

Use the docx skill (`.agents/skills/docx/SKILL.md`) to:

**SECTION 6: Appendices (Pages 41+)**

1. **Data Sources & References**
   - Add heading: 'Data Sources & References' (level 1)
   - List all sources used throughout the report
   - Organize by category:
     - SEC Filings (10-K, 10-Q, DEF 14A, 8-K with EDGAR links)
     - Earnings Calls (with transcript links)
     - Company Materials (investor presentations, press releases)
     - Industry Reports (Gartner, Forrester, etc.)
     - News Articles
   - **CRITICAL**: All URLs must be clickable hyperlinks (not plain text)
   - Include dates for all sources

2. **Additional Tables**
   - Add heading: 'Additional Tables' (level 1)
   - Add extended financial projections
   - Add detailed assumptions tables
   - Add any supporting tables that didn't fit in main sections

### Phase G: Write Page 1 Investment Summary
**NOW write Page 1 - after all analysis complete**
- INITIATING COVERAGE header
- Rating box
- 3-4 detailed bullets synthesizing entire report
- Financial summary table
- Stock price chart

### Phase H: Add Table of Contents & Page Numbers
- Auto-generate TOC
- Add page numbers to all pages

**Key formatting requirements:**
- Professional fonts (Calibri, Arial, or similar)
- Proper headers and footers with page numbers
- Section breaks between major sections
- Embed every chart inline throughout the text
- Insert every table inline with the text
- **All URLs as clickable hyperlinks**
- **60-80% page density** - Every page has text AND visuals

**Visual Density Strategy:**
```
Good page layout example:
┌─────────────────────────────┐
│ Section Header              │
│ Text paragraph (200 words)  │
│ [Chart embedded]            │
│ Text paragraph (200 words)  │
│ [Table embedded]            │
│ Text paragraph (200 words)  │
│ [Chart embedded]            │
└─────────────────────────────┘
```

Charts sit next to the text that discusses them, one or more per page, so every page carries both text and visuals.

**Result**: a text-dense report with illustrating images throughout, at the lengths in Critical Sections with Word Counts.

## File Operations Summary

**Throughout the entire assembly process, use the docx and xlsx skills (`.agents/skills/docx/SKILL.md`, `.agents/skills/xlsx/SKILL.md`) with actual file operations:**

**Reading Input Files:**
- ✓ Use Read tool: `[Company]_Research_Document_[Date].md` - Read Task 1 research
- ✓ Use XLSX skill: Open `[Company]_Financial_Model_[Date].xlsx` and read tabs - Extract tables from Task 2/3
- ✓ Use Read tool: `[Company]_Valuation_Analysis_[Date].md` - Read Task 3 analysis
- ✓ Use DOCX skill: Insert images from `task4_charts/chart_XX.png` files

**Writing Output File:**
- ✓ Use DOCX skill: Create new Word document
- ✓ Use DOCX skill: Add paragraphs (text from input .md files)
- ✓ Use DOCX skill: Create tables (data from Excel files read via XLSX skill)
- ✓ Use DOCX skill: Insert images (chart .png files)
- ✓ Use DOCX skill: Save final file as `[Company]_Initiation_Report_[Date].docx`

**Use the docx and xlsx skills for every transfer, rather than manual copy/paste:**
1. Read from .md files (Task 1, Task 3) using Read tool
2. Read from .xlsx files (Task 2 with Task 3 tabs) using XLSX skill
3. Read from .png files (Task 4) as image files
4. Write to .docx file (Task 5 output) using DOCX skill

This approach is efficient, reproducible, and ensures all data flows correctly from source files to final report.

Assembly is done. Return to `.agents/skills/initiating-coverage/references/task5-report-assembly.md` and run the quality check end to end before delivering.
