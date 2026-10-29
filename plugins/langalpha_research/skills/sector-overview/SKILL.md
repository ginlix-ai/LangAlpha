---
name: sector-overview
description: "Sector and industry landscape report, built on the sector's own archetypes, metrics and valuation lenses. Triggers on sector overview, industry landscape, industry primer, sector deep dive, market map."
---

# Sector Overview

A landscape report on an industry: how it earns money, who is winning, what it is worth, and what would change that. The quality of the report is decided in the first step, not the last: a set classified into the wrong archetype produces the right-looking report with the wrong metrics, the wrong multiple and the wrong risks.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## Workflow

### 1. Scope it

- **Sector or subsector**, and how narrowly the boundary is drawn.
- **Purpose**: a client report, a research packet, pitch material, idea generation.
- **Angle**: a neutral landscape, or a thematic thesis the report is testing.
- **Universe**: public names only, or private included.
- **Depth**: default `working analysis`; a sector screen is a `first pass`. Bands and what to cut first: `.agents/skills/research-conventions/references/depth.md`.

### 2. Classify into an archetype before analysing

Write down how each company in the set actually earns money, and put it in an archetype. Business model beats label: two names carrying the same sector tag and different revenue mechanics share no metric set, no valuation lens and no risk list. Classify from the revenue recognition note and the segment note of the largest names in the set, not from the sector tag.

Load the lens for the set before Step 3:

- Software, internet, marketplace or advertising: `.agents/skills/sector-overview/references/sectors/software-and-internet.md`.
- Banks, lenders and capital-markets firms: `.agents/skills/sector-overview/references/sectors/banks.md`.
- Producing, moving, refining or selling energy: `.agents/skills/sector-overview/references/sectors/energy.md`.
- Any other sector, and adding a lens for it: `.agents/skills/sector-overview/references/sectors/README.md`.

Every company in the set carries one archetype label before a metric is pulled, and the comparison table in Step 4 is the archetype's metric set rather than a generic one.

### 3. Size the market, and say which kind of number each figure is

**Market size and growth.** Current size, historical growth, forecast growth with the assumption behind it, and the segmentation that matters (product, geography, end market, customer type).

Two kinds of figure, labelled on the face of the table:

- **Sized**: built bottom-up from disclosed units, prices or revenue that can be added up and checked.
- **Extrapolated**: a growth rate applied to a base someone else sized.

An extrapolated figure supports direction and never carries a conclusion that a sized figure would have to support. A forecast growth rate whose method is not stated is an `assumption` and is labelled one. State the addressable market the company can actually reach beside the total, and where the two differ, the gap is the interesting number.

**Staleness.** Sector size and share research stays fresh for twelve months and always prints its vintage; every sized figure carries its as-of in the table, not in a footnote at the end. A study past twelve months either gets refreshed or gets a line saying what has changed since it was published. Thresholds by data type and the six staleness states: `.agents/skills/research-conventions/references/evidence.md`.

**Structure and drivers.** Fragmented or consolidated, with the top-five share and its source. The value chain, and where in it the margin actually sits. Barriers to entry (capital, regulatory, technical, network). Three to five secular drivers, the headwinds against them, the technology and regulatory vectors, and the consolidation pattern.

### 4. Map the competitive landscape

Profile the top five to ten names on the archetype's metrics, not on a generic revenue and margin table:

| Company | Archetype | [KPI 1] | [KPI 2] | [KPI 3] | Share | As-of | Label |
|---|---|---|---|---|---|---|---|

Each name also gets a short profile: what it does in two sentences, where it is positioned and why that holds, what changed recently, and a valuation snapshot on the sector's lead lens. Then the dynamics: how the companies compete, who is gaining and losing share and through which mechanism, and where the disruption comes from.

**Row-merge discipline.** The rule for sharing a row in an exposure or driver table is in `.agents/skills/research-conventions/references/judgment.md`. In a sector table its common failure is thematic adjacency: two companies reached by the same theme through different line items look mergeable and are two rows.

### 5. Value the sector in its own lens order

Lead with the first lens from the sector file, and say in one sentence which generic lens is wrong here and why, so the omission reads as a choice.

**The wrong-lens failure.** A revenue-growth-and-margin frame applied to a bank, an insurer or a REIT produces an answer that is confident, internally consistent and wrong: a bank's debt is raw material rather than financing, an insurer's earnings are a reserve estimate, and a REIT's reported earnings are rents suppressed by depreciation. One sentence before the valuation section is written settles it: what is this sector's multiple a function of? When the answer is not the lens on the page, the lens is the thing to change.

Then the sector context: current multiples against their own historical range, what drives the premium or discount across the set, recent transaction multiples with the basis stated, and how the sector prices against the broader market. Every multiple carries its as-of, its LTM or NTM label and one common base date across the table: `.agents/skills/research-conventions/references/market-data-rules.md`.

### 6. Draw the implications, and separate the orders of effect

Where the report turns on a shock (a rate move, a tariff, an input-cost move, a regulation), report the orders of effect in separate rows at separate confidence rather than blended into one conclusion:

- **Direct**: names the transmission channel and the line item it lands on.
- **Second order**: names whose behaviour has to change in between.
- **Third order**: the competitive or structural response, at the lowest confidence and often past the horizon.

When the channel cannot be named, the claim is not ready to write and the next step is source work rather than prose. The full rule, with the action vocabulary and the falsifiable-claims table: `.agents/skills/research-conventions/references/judgment.md`.

Close on where the risk and reward sit, which thematic bets the sector can express, the two or three live debates stated at their strongest on both sides, and the dated catalysts that would settle them.

### 7. Build the deliverable

A document or a deck carrying one readiness posture stated once near the top, the market overview and sizing, the landscape map, the comparison table, the valuation summary, and the charts that do the work: market growth, share trend, and a valuation scatter on the sector's lead lens. A workbook appendix carries the company data behind the tables.

Format is owned by the deliverable skill: `.agents/skills/docx/SKILL.md`, `.agents/skills/pptx/SKILL.md`, `.agents/skills/xlsx/SKILL.md`.

## Important notes

- Charts carry this report. A sizing waterfall, a positioning map and a valuation scatter say in one look what three paragraphs say slowly.
- Where the reader has a specific situation (a target list, a market-entry question, a positioning problem), the "so what" section is written to that situation rather than to the sector in general.
- A sector report with no view is a directory. Name which archetype the structure favours over the horizon, and what would reverse it.
