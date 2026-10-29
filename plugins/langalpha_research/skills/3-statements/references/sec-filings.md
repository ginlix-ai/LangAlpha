# SEC Filings Data Extraction Reference

**When to Use:** Only reference this file when a model template specifically requires pulling data from SEC filings (10-K, 10-Q). For templates that provide data directly or use other data sources, this reference is not needed.

---

## Extracting Data from SEC Filings (10-K / 10-Q)

When populating a model template with public company data, extract financials directly from SEC filings.

### Step 1: Locate the Filing

1. Call `get_sec_filing` for the company's 10-K or 10-Q.
2. Record the filing as the tier-1 source per `.agents/skills/research-conventions/references/evidence.md`.

### Step 2: Identify Filing Currency

Before extracting data, identify the reporting currency:
- Check the cover page or header for reporting currency
- Look at statement headers (e.g., "in thousands of U.S. dollars")
- Review Note 1 (Summary of Significant Accounting Policies)

**Common Currency Indicators**

| Indicator | Currency |
|-----------|----------|
| $, USD | US Dollar |
| €, EUR | Euro |
| £, GBP | British Pound |
| ¥, JPY | Japanese Yen |
| ¥, CNY, RMB | Chinese Yuan |
| CHF | Swiss Franc |
| CAD, C$ | Canadian Dollar |

Set model currency to match filing; document in Assumptions tab.

### Step 3: Navigate to Financial Statements

Within the 10-K or 10-Q, locate:
- **Item 8** (10-K) or **Item 1** (10-Q): Financial Statements
- Key sections to extract:
  - Consolidated Statements of Operations (Income Statement)
  - Consolidated Balance Sheets
  - Consolidated Statements of Cash Flows
  - Notes to Financial Statements (for schedule details)

### Step 4: Data Extraction Mapping

Map every caption to its canonical row per `line-items.md` in this directory.

### Step 5: Extract Supporting Detail from Notes

For schedules, pull from Notes to Financial Statements:
- **Note: Debt** → Maturity schedule, interest rates, covenants
- **Note: Property, Plant & Equipment** → Gross PP&E, accumulated depreciation, useful lives
- **Note: Revenue** → Segment breakdowns, geographic splits
- **Note: Leases** → Operating vs. finance lease obligations

### Step 6: Historical Data Requirements

Extract 3 years of historical data minimum:
- 10-K provides 3 years of IS/CF, 2 years of BS
- For 3rd year BS, pull from prior year's 10-K
- Use 10-Qs to fill in quarterly granularity if needed

### Data Extraction Checklist

- Identify reporting currency and scale (thousands, millions)
- 3 years historical Income Statement
- 3 years historical Cash Flow Statement
- 3 years historical Balance Sheet
- Verify IS Net Income = CF starting Net Income (each year)
- Verify BS Cash = CF Ending Cash (each year)
- Extract debt maturity schedule from notes
- Extract D&A detail or useful life assumptions
- Note any non-recurring / one-time items to normalize

### Handling Common Filing Variations

| Variation | How to Handle |
|-----------|---------------|
| D&A embedded in COGS/SG&A | Pull D&A from Cash Flow Statement |
| "Other" line items are material | Check notes for breakdown |
| Restatements | Use restated figures, note in assumptions |
| Fiscal year ≠ calendar year | Label with fiscal year end (e.g., FYE Jan 2025) |
| Non-USD reporting currency | Adapt model currency to match filing |
