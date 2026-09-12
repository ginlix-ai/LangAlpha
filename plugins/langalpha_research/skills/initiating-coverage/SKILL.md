---
name: initiating-coverage
description: "First-time coverage of a company, or a refresh of coverage already published, run one task per request by default: company research, financial model, valuation, charts, then a 30 to 50 page report with a model. Triggers on initiation report, initiate coverage, initiating coverage, full equity research report, refresh the initiation."
---

# Initiating Coverage

A first-time coverage report: the argument for or against owning a company, built from original research, a full model and a defended valuation. Five tasks, one per request by default, each verifying its inputs before it starts. The deliverables are a 30 to 50 page DOCX and an XLSX model, with typography set by `.agents/skills/docx/SKILL.md`.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## One task per request

This skill runs one task per request by default: execute the task, deliver its file, and stop, so the user can review before the next.

That stop is an intake exception and yields under `.agents/skills/research-conventions/references/intake.md`. When the user asked for the whole pipeline in one request ("do all five tasks", "run the whole initiation") or this skill is running as an input to another workflow, run the tasks in order without stopping between them, naming at each handoff the task just finished.

When the request names no task and does not ask for the whole run ("write an initiation report for X"), reply with the task list and ask which one to begin, in roughly this shape:

```
An initiation for [Company] runs as five tasks, one per request:

1. Company research      4. Chart generation
2. Financial modelling   5. Report assembly
3. Valuation analysis

Each one delivers its file and stops, so you can review before the next.
Which would you like to start with?
```

Either way, never assume a task the user did not ask for, and never start a task whose prerequisites are unverified.

## Task map

| Task | Prerequisites | Deliverable |
|---|---|---|
| 1 Company research | company name or ticker | `[Company]_Research_Document_[Date].md`, 6,000 to 8,000 words |
| 2 Financial modelling | financial statements or 10-K access | `[Company]_Financial_Model_[Date].xlsx`, the manifest's Task 2 sheets |
| 3 Valuation analysis | Task 2 | `[Company]_Valuation_Analysis_[Date].md` plus the manifest's Task 3 sheets, added to the Task 2 workbook |
| 4 Chart generation | Tasks 1, 2, 3 | `[Company]_Charts_[Date].zip`, 25 to 35 charts at 300 DPI |
| 5 Report assembly | Tasks 1, 2, 3, 4 | `[Company]_Initiation_Report_[Date].docx` |

Tasks 1 and 2 are independent of each other and can run in either order. Tasks 3, 4 and 5 have hard dependencies: verify the named inputs exist and can be opened, and when one is missing, stop and say which task supplies it rather than building a placeholder.

## Workbook manifest

Tasks 2 and 3 build one file, `[Company]_Financial_Model_[Date].xlsx`. Its sheets, in this order, are enumerated here and pointed at from everywhere else:

`Cover`, `Revenue Model`, `Income Statement`, `Cash Flow Statement`, `Balance Sheet`, `Scenarios`, `DCF Inputs`, `DCF`, `Sensitivity Analysis`, `Comparable Companies`, `Precedent Transactions` (optional), `Valuation Summary`, `Checks`.

Task 2 builds `Cover`, the six core operating tabs `Revenue Model` through `DCF Inputs`, and `Checks`. Task 3 adds the four valuation tabs `DCF`, `Sensitivity Analysis`, `Comparable Companies` and `Valuation Summary`, adds `Precedent Transactions` where the deal set supports one, and extends `Cover` and `Checks` to cover them. Twelve sheets, thirteen with `Precedent Transactions`. What `Cover` and `Checks` each hold, and why the file opens on one and closes on the other, are in `.agents/skills/xlsx/SKILL.md`.

## Standing rules

These bind every task.

**Deliver one file per task, plus the delivery message Task 5 specifies, and no additional files.** No standalone executive summary, no quick-reference guide, no next-steps document. Extras cost context and are not part of the workflow.

**Never overwrite the user's own file.** When the user supplied a model or a draft, produce a versioned copy or an additive companion, and mark what changed.

**The minimums are floors, not targets.** Word counts, chart counts and table counts below are the point at which a section stops being a stub. They are met by analysis, never by padding, and `assets/quality-checklist.md` says where the words go and what to cut.

**Every material claim carries its evidence label and its source.** Labels, source tiers, the conflict protocol and the staleness thresholds live in `.agents/skills/research-conventions/references/evidence.md`. Keep a source register as you work, one row per source: id, name, type, publication date, access date, period covered, location inside the document, tier, and whether it is stale. The register becomes the report's data-sources page, so building it during research costs nothing and rebuilding it at the end costs a day.

**Evidence confidence and underwriting status are two different fields.** High confidence in the numbers does not mean the position can be underwritten: the numbers can be excellent and the capital structure, the valuation or the disclosure still leave ownership unsupportable. Both appear on the report cover, and neither substitutes for the other.

## Step 0: Classify the initiation

Before Task 1, name the mode. It decides which content is required, how long each section runs and what tone the argument takes: a buy-side deep dive is short on boilerplate and long on falsifiability, a long-only initiation centres durability and capital allocation, a sector launch maps the battlefield before naming winners.

The eight modes, what each one requires and what each one cuts: `references/report-modes.md`. Read it once at Step 0 and carry the mode through every later task.

**Complete when** the mode is written down in one line with the reader it serves, and every later task's content list has been checked against it.

## Task 1: Company research

Business, management, competitive position, industry, TAM and risks. Detailed workflow in `references/task1-company-research.md`; the shape a thesis pillar, a debate and a risk row must take is in `references/argument-standards.md`.

**Thin context.** When the company is too thinly covered to research properly (a recent listing, a foreign private issuer with sparse English disclosure, a carve-out with no standalone history), do not fill the gap with plausible detail. Deliver a skeleton instead: the research agenda, the model architecture the company would need, the specific source requests that would unblock it, and the thesis hypotheses to be tested. Say plainly that it is a skeleton and what would upgrade it.

**Complete when** the document runs 6,000 to 8,000 words and carries: the company overview and history; 300 to 400 word bios for each of 3 to 4 executives; products and services; the industry; 5 to 10 competitors analysed rather than listed; TAM with its build; 8 to 12 risks across the four categories; and three to five thesis pillars each with the four parts required by `references/argument-standards.md`.

**A skeleton completes instead** when it carries all four items named in the thin-context paragraph above, says on its first page that it is a skeleton and what would upgrade it, and contains no invented detail. The word count, the bios and the competitor and risk counts do not apply to it.

## Task 2: Financial modelling

Extract the historicals, then build the projection model. Detailed workflow in `references/task2-financial-modeling.md`. Workbook construction rules, live formulas and the Checks sheet belong to `.agents/skills/xlsx/SKILL.md`.

**Before starting**, confirm one of: access to the 10-K or the statements; or historical financials the user supplied covering income statement, cash flow and balance sheet for 3 to 5 years.

**Complete when** the workbook opens and holds every Task 2 sheet of the workbook manifest, with 3 to 5 years of history and 5 years of projections; the revenue model carries 20 to 30 product rows and 15 to 20 geography rows; the income statement carries 40 to 50 line items; and all three scenarios differ in their parameters rather than in their labels.

## Task 3: Valuation analysis

DCF, comparables and, where the market supports it, precedent transactions. Workflow in `references/task3-valuation.md`; method selection with its mandatory caveats, the financed-growth gate, the revenue-multiple demotion rule, the EPS-basis rule and the target-price bridge are in `references/valuation-methodologies.md`.

**Prerequisite gate.** Task 2 must be complete and its workbook openable. Without it, stop and say so: a valuation built on placeholder projections is worse than no valuation, because it looks finished.

**Market-data completion.** Before any conclusion, actively retrieve and timestamp: current price (`get_daily_prices` or the ambient feed), market cap, fully diluted share count, the enterprise-value bridge inputs (gross debt, capitalised leases, preferred, minority interest, cash), and the available consensus context (`get_company_overview`). Anything that cannot be retrieved is labelled with the specific conclusion it blocks, not left blank.

**Preliminary and watchlist posture.** When the valuation or the capital stack cannot be established, the honest output is a preliminary or watchlist initiation: label the document as such, list the evidence required before ownership can be underwritten, and publish no target price. An invented target is the single most damaging thing this skill can produce, because every downstream reader treats it as underwritten.

**Complete when** the price target exists as a range with a point inside it and carries every element of the target-price bridge; at least two methods contribute, each with its mandatory caveat stated; the DCF sensitivity matrix and the comps table with its statistical summary (max, 75th, median, 25th, min) are built; the financed-growth gate has either passed or forced the preliminary posture; and the written analysis runs 4 to 6 pages.

**A preliminary or watchlist initiation completes instead** when the document carries that label, holds no target price anywhere, lists as a numbered set the evidence required before ownership can be underwritten with the conclusion each missing input blocks, and states the caveat on whatever valuation work the available inputs did support.

## Task 4: Chart generation

25 to 35 charts at 300 DPI. Workflow and per-chart specifications in `references/task4-chart-generation.md`.

**Prerequisite gate.** Tasks 1, 2 and 3 all complete: Task 1 supplies 9 charts, Task 2 supplies 8, Task 3 supplies 6, and market data supplies 2. Market data comes from `get_daily_prices` for the price history and `get_historical_valuation` for the multiple bands, each with its retrieval date recorded for the source line.

**Complete when** at least 25 charts exist as real image files, the four mandatory charts are among them (chart_03 revenue by product as stacked area, chart_04 revenue by geography as stacked bar, chart_28 DCF sensitivity as a two-way heat map, chart_32 valuation football field as horizontal bars), every file follows `chart_NN_description.png`, and the zip contains them plus `chart_index.txt`.

## Task 5: Report assembly

Write and assemble the DOCX. Workflow in `references/task5-report-assembly.md`, page-by-page structure in `assets/report-template.md`, and the delivery gate in `assets/quality-checklist.md`. Build through `.agents/skills/docx/SKILL.md` and read the model through `.agents/skills/xlsx/SKILL.md`.

**Prerequisite gate.** All four prior deliverables exist and open. Verify each one before writing a word, and when one is missing, name it and stop.

This is the task where effort shows. Write every section in full: 2,000 to 3,000 words on projection assumptions, 1,500 to 2,000 on scenarios, and the Task 1 research carried across rather than re-summarised. "This section would cover..." and "see the model for detail" are both failures. Embed every chart from Task 4, not a selection. Where `references/task5-report-assembly.md` calls for carrying a Task 1 section across near-verbatim and the mode in `references/report-modes.md` cuts that section, the mode's cut list wins.

**The cover carries five fields**: rating or posture, price target or the preliminary label, evidence confidence, underwriting status, and the initiation mode from Step 0.

**One rating ladder.** The report prints one rating, `Buy`, `Hold` or `Sell`, beside the action verb from `.agents/skills/research-conventions/references/judgment.md`.

**Convert the argument into falsifiable claims** before delivery: two to five claims, each with evidence, implication, test metric, falsifier and time to knowable, per `.agents/skills/research-conventions/references/judgment.md`. Fewer than two means the report holds no view.

**Section titles state the finding, not the topic.** "Contract structure caps downside through 2029" beats "Contract overview". A heading that could sit above any company's section is a heading that says nothing.

**Complete when** the report is 30 to 50 pages and 10,000 to 15,000 words with 25 to 35 embedded charts and 12 to 20 tables; every number matches the model exactly; every citation is a working hyperlink; the cover carries all five fields; and `assets/quality-checklist.md` passes end to end.

Deliver with a short message naming the mode, the thesis pillars, the open debates, the valuation method and target, the catalysts, the disconfirming signals and the open questions, so the next skill inherits the argument rather than re-deriving it.

## Reference files

Load only the file for the task in front of you; they are large.

- Running a task: `references/task1-company-research.md`, `references/task2-financial-modeling.md`, `references/task3-valuation.md`, `references/task4-chart-generation.md`, `references/task5-report-assembly.md`.
- Classifying the initiation at Step 0, or checking what a mode requires: `references/report-modes.md`.
- Writing a thesis pillar, a key-debates row, a risk row or a catalyst group, in Task 1 or Task 5: `references/argument-standards.md`.
- Choosing a valuation method, valuing a capital-intensive or financed-growth business, or building the target-price bridge, in Task 3: `references/valuation-methodologies.md`.
- Page-by-page report layout in Task 5: `assets/report-template.md`.
- Before delivering Task 5: `assets/quality-checklist.md`.

## Working across sessions

Outputs from earlier tasks in the same session are available directly. Across sessions, the request names the path: "Task 3 with the model at [path]". Keep the five deliverables in one folder per company so a later task can find them:

```
[Company]/
  Task1_Research/   [Company]_Research_Document.md
  Task2_Model/      [Company]_Financial_Model.xlsx
  Task3_Valuation/  [Company]_Valuation_Analysis.md
  Task4_Charts/     chart_01..chart_35.png, chart_index.txt
  Task5_Report/     [Company]_Initiation_Report.docx
```
