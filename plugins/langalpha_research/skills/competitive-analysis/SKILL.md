---
name: competitive-analysis
description: "Competitive landscape analysis: positioning, scorecards, moat assessment, market share trends. Triggers on competitive analysis, competitive landscape, competitor benchmarking, moat assessment, market share, who are the competitors."
---

# Competitive Landscape Mapping

Who competes, on what, and which advantages survive. The product is a comparison, so the comparison has to be real: same period, same metric definition, same basis, with each figure's provenance visible. A table of numbers that were measured differently looks like analysis and is not one.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## Reference files

- The deliverable is a deck or a formatted document, or the request specifies titles, chart types or exact figures: `.agents/skills/competitive-analysis/references/presentation.md`.
- The set is judged on customer economics rather than units shipped or stores opened: `.agents/skills/competitive-analysis/references/unit-economics.md`, which carries the benchmark bands (Rule of 40 on EBITDA margin, NDR, LTV:CAC, CAC payback), the cohort matrix and the traps that make two companies look comparable when they are not.
- Building an M&A transaction table, a scenario table, or a slide skeleton: `.agents/skills/competitive-analysis/references/schemas.md`.
- Choosing the two axes for a positioning matrix: `.agents/skills/competitive-analysis/references/frameworks.md`.

## Data standards

These bind every run, whatever the deliverable is.

### Provenance on every competitor metric

Each cell in a competitor table carries the figure, its **as-of**, and one label from the closed set in `.agents/skills/research-conventions/references/evidence.md`, which also holds the freshness thresholds and the six staleness states. Three of the seven carry most of a competitor table: `fact` for the company's own filed statements, `company claim` for a management figure that is not in them (market share, customer count, addressable market), and `street estimate` for a consensus or single-analyst number, with its vintage and analyst count.

A third-party sizing of a private company is a `street estimate`: `fact` is reserved for a primary document or a data-tool figure for a period that has closed. A research house's published sizing carries the house as its estimator and the publication date as its vintage, and anything with no traceable publisher stays `needs-source` until one is found.

The as-of is the fiscal period for a reported figure, the publication date for an estimate, and the retrieval date for anything built on price. A private-company figure carries a label and an as-of like any other cell, and prints the reader-facing word its label maps to in `.agents/skills/research-conventions/references/evidence.md`.

### When a competitor metric does not exist

Label the gap rather than filling it. The cell reads `not disclosed`, and the table note says what was searched. Where the missing metric is load-bearing, meaning the ranking or the moat conclusion moves without it, take one of the two exits in the unsupported-claim rule: drop the conclusion that rests on it, or carry a bound ("the range consistent with the reported total is A to B"), label it `assumption`, and re-read the posture from the ladder in `.agents/skills/research-conventions/SKILL.md` against the new input state. A plausible-looking estimate with no flag is exactly the failure that rule exists to catch: `evidence.md`.

### Comparability

- **Periods match.** Every competitor metric comes from the same fiscal period, and an exception is flagged in the cell: "(FY24)" against "(H1 2024)". Fiscal-period labelling, LTM and NTM windows, one common base date for comparative returns, and margin numerator and denominator pairing: `.agents/skills/research-conventions/references/market-data-rules.md`.
- **Definitions match.** One calculation methodology across the set, stated once where two companies would define the metric differently.
- **Currency normalised.** Convert to USD for international sets, and note the rate and the date used.
- Missing data reads `not disclosed` per the gap rule in Step 1.
- **Every number cites its source**, in the form `[Company] [Document] ([Date])`.

### Source files provided by the user

- **Extract values directly.** Use the numbers as they appear rather than recomputing them.
- **Keep one value per metric** across every slide and table in the deliverable.
- **Verify anything you are asked to calculate** against related figures in the same source.
- **Match the source's precision.** Round as it rounds.

Where a source file and a filing disagree, the conflict register in `evidence.md` decides it and the artifact shows the selection.

### Source hierarchy

The general tiers by claim family are in `evidence.md` and govern. Two additions specific to this work: sell-side research is the usual route to a private competitor's size and is a `street estimate` with its vintage, and industry research houses are the usual route to a share figure and carry the house and the publication date.

### Depth

Default `working analysis`: 8 to 12 pages, or 12 to 20 slides, plus the comparison workbook. A rapid competitive read is a `first pass` at up to five slides. Bands and cut order: `.agents/skills/research-conventions/references/depth.md`.

## Workflow phases

**Phase 1, scope it.** Confirm: single-company deep dive or multi-company comparison; deck or written memo; the specific competitors, dimensions or strategic question in play; whether an investment context needs scenarios and signposts; and which source files exist and which values come out of them.

**Phase 2, research, outline, review, build.** Run Steps 0 through 9 below, show the outline with the real numbers already in it, and build the final artifact after the outline has been reviewed. That review is an intake exception and yields under `.agents/skills/research-conventions/references/intake.md`: when this skill runs as an input to another workflow, or the user asked for the finished artifact in one request, present the outline and keep building without waiting on it.

## Analysis workflow

### Step 0: Identify the industry-defining metrics

Before any pull, name the three to five metrics this industry is actually judged on:

| Industry | Key metrics |
|---|---|
| SaaS | ARR, NRR, CAC payback, LTV/CAC, Rule of 40 on EBITDA margin |
| Payments | GPV, take rate, attach rate, transaction margin |
| Marketplaces | GMV, take rate, buyer/seller ratio, repeat rate |
| Retail | Same-store sales, inventory turns, sales per square foot |
| Logistics | Volume, cost per unit, on-time delivery, capacity utilisation |

For an industry not listed, take the three to five metrics investors and operators use to benchmark it. Use the same set for every company in the comparison.

### Step 1: Market context

Market size now and projected, with the source and its vintage. Growth drivers, headwinds, and the trends reshaping the industry.

**Correct**: "The embedded payments market is $80B to $100B in 2024, growing 20% to 25% a year (research house, 2024)."
**Not usable**: "The market is large and growing rapidly."

### Step 2: Industry economics

Map where the value flows, in the shape the industry actually has:

- **Vertically structured**: the value chain layers, with typical margin at each.
- **Platform or network**: the participants and the value moving between them.
- **Fragmented**: the consolidation dynamic, and how margin differs with scale.

### Step 3: Target company profile

| Metric | Value | As-of | Label |
|---|---|---|---|
| Revenue | $4.96B | FY2024 | fact |
| Growth | +26% y/y | FY2024 | fact |
| Gross margin | 45% | FY2024 | fact |
| Profitability | $373M adj. EBITDA | FY2024 | fact |
| Customers | 134K | Q4 FY2024 | company claim |
| Retention | 92% | Q4 FY2024 | company claim |
| Market share | ~15% | 2024 | street estimate |

For a multi-segment company, add the segment breakdown:

| Segment | Revenue | Rev y/y | Rev % | EBITDA | EBITDA y/y | Margin |
|---|---|---|---|---|---|---|
| Seg A | $25.1B | +26% | 57% | $6.5B | +31% | 26% |
| Seg B | $13.8B | +31% | 31% | $2.5B | +64% | 18% |
| Seg C | $5.1B | -2% | 12% | -$74M | -16% | -1% |
| Total | $44.0B | +18% | 100% | $6.5B | | 15% |

Note unallocated corporate costs where the segments do not foot to the total.

### Step 4: Competitor mapping

Group the set with whichever cut is real for this industry: by business model (platform, vertical, horizontal), by segment served (enterprise, SMB, consumer), by posture (direct, adjacent, emerging), or by origin (incumbent, disruptor, new entrant).

### Step 5: Positioning visualisation

| Visualisation | Best for |
|---|---|
| 2x2 matrix | Two dominant competitive factors |
| Radar | Multi-factor comparison |
| Tier diagram | Natural clustering into strategic groups |
| Value chain map | Vertical industries |
| Ecosystem map | Platform markets |

### Step 6: Competitor deep dives

**Metrics**, on the Step 0 set, each row carrying its as-of and label as in Step 3.

**Qualitative:**

| Category | Assessment |
|---|---|
| Business | What they do, one sentence |
| Strengths | Two or three bullets |
| Weaknesses | Two or three bullets |
| Strategy | Current priorities |

### Step 7: Comparative analysis

| Dimension | Company A | Company B | Company C |
|---|---|---|---|
| Scale | ●●● $160B | ●●○ $45B | ●○○ $8B |
| Growth | ●●○ +26% | ●●● +35% | ●●○ +22% |
| Margins | ●●○ 7.5% | ●○○ 3.2% | ●●● 15% |

**Row-merge discipline.** Two competitors share a row only where "Row-merge discipline" in `.agents/skills/research-conventions/references/judgment.md` allows it.

### Step 8: Strategic context

M&A transactions with their multiples and the strategic logic, partnership and integration patterns, capital-raising activity, and regulatory developments.

### Step 9: Synthesis

**Moat assessment.** The rating is a `judgement` and is written as one: each row shows what was observed, then what we conclude from it.

| Moat type | Observed | Rating | Why the observation supports it |
|---|---|---|---|
| Network effects | the flywheel evidence, cross-side or same-side | Strong / Moderate / Weak | one clause |
| Switching costs | integration depth, contractual lock-in, habit | | |
| Scale economies | unit cost at volume, minimum efficient scale | | |
| Intangible assets | brand, proprietary data, licences, patents | | |

A `Strong` with an empty observed column is an opinion in a table. Keep the observation and the rating in separate columns so a reader can disagree with the second while keeping the first. How far the evidence lets the language go: `evidence.md`.

**Then three things:** the durable advantages, mapped to the rows above; the structural vulnerabilities that are hard to fix; and the current state against the trajectory, which is where the two diverge.

**For an investment context:**

| Scenario | Probability | Key driver |
|---|---|---|
| Bull | 30% | Share gains, margin expansion |
| Base | 50% | Current trajectory continues |
| Bear | 20% | Competitive pressure, margin compression |

The probability set follows "Probabilities" in `.agents/skills/research-conventions/references/judgment.md`, and each scenario names the competitive driver that produces it.

## Quality checklist

Verify before delivery. Deck and document formatting has its own checklist in `.agents/skills/competitive-analysis/references/presentation.md`.

**Comparability**
- Every competitor metric is from the same fiscal period, with exceptions flagged in the cell.
- One metric definition across the whole set.
- International figures converted at a stated rate and date.

**Provenance**
- Every figure carries an as-of and one evidence label.
- Every number cites its source in `[Company] [Document] ([Date])` form.
- Missing metrics read `not disclosed` with a table note saying what was searched, and no cell holds an unlabelled estimate.
- Values taken from user-supplied files match those files exactly, and one metric shows one value everywhere it appears.

**Analysis**
- Moat ratings sit beside their observed evidence.
- Scenario probabilities sum to one, or the weighting is withheld.
- The comparison table's rows merge only where the hub's row-merge discipline allows.
- The artifact is inside its depth band and states its readiness posture.
