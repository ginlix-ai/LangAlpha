---
name: check-deck
description: "QC an investment deck (a .pptx) before it circulates: number consistency, chart and narrative alignment, source coverage, language, then a circulation verdict. Triggers on check this deck, deck QC, review my presentation, is this ready to send, proofread the pitch book."
---

# Deck Check

QC that ends in a decision. Finding a discrepancy is the easy half; the work is saying what it proves, how confident we are, and whether the deck can go out. Every consequential finding carries a confidence label, and the pass ends in one readiness posture for the whole document.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first finding.

## Step 1: Establish the artifact set and the evidence rules

Name the **controlling artifact** (the deck under review) and list every **supporting artifact** with its role: the model the numbers tie to, the filings behind the market claims, the prior version, the data pulls. A finding's confidence depends on which artifact it came from, so this classification happens before any checking.

The controlling model is the subject under test, and the deck is checked against it: a deck figure that does not match the model is a finding about the deck. Where the model and a source disagree, the sources resolve down the tiers in `.agents/skills/research-conventions/references/evidence.md`, the filing first, then the data tools, then company materials labelled as such, then third-party summaries. Two conflicting sources are never averaged and never silently resolved in favour of the flattering one.

Read the deck out with the `pptx` skill's extractor, which keeps the slide structure the
checks depend on:

```bash
python .agents/skills/pptx/scripts/extract.py presentation.pptx > deck.json
```

The result is one JSON object with `file`, `slide_size_in` and `slides`. Each slide carries
its `index`, `title`, `layout`, its `text` lines in shape order, a `shapes` inventory of
names, positions and font sizes, `tables` as `cells` (a list of rows of strings), `charts`
as `series` values against `categories`, and speaker `notes`. Keeping that structure is the
point: a figure can then be reported as slide 4, `Table 3 r2c2`, and a table cell can be read
through the row label and column header that say what it is, neither of which survives a
prose export.

Render the slides to images with the `pptx` skill workflow. **Every Critical or Important finding that rests on visible content is looked at on the rendered page before it is written**, because the extractor sees text and cells while the reader sees a layout. Where a chart or table is an embedded image, the data is unextractable: say so in the report as a scope limit rather than passing over the page in silence.

### Finding confidence

Every consequential finding takes exactly one label:

| Label | Holds when | Wording it licenses |
|---|---|---|
| `confirmed internal mismatch` | the supplied materials contradict each other, and both sides are quoted with their locations | "these two figures disagree" |
| `externally verified error` | a controlling primary source proves the claim wrong, and that source is cited | "this is incorrect; the filing says X" |
| `needs review` | suspected from the materials at hand, unprovable with them | "this may be wrong; here is the specific support required" |

An external fact, an identifier or a third-party claim is never called wrong because the supplied files disagree with it or because the number looks implausible. Internal disagreement proves an internal disagreement. Anything beyond that needs the external source in hand, and without it the finding is `needs review` with the exact document that would settle it named.

Done when the controlling artifact is named, every supporting artifact carries its role, `deck.json` exists, the pages are rendered, and any image-only content is listed as a scope limit.

## Step 2: Number consistency

```bash
python .agents/skills/check-deck/scripts/extract_numbers.py deck.json --check
```

The script also takes `presentation.pptx` directly when the extractor above is reachable,
and falls back to a markdown or text export where a `## Slide 3` line starts a slide.

Every number is keyed by the metric word and the period or scenario token nearest to it
(`revenue|fy2024a`, `ebitda margin|fy2025e`, `irr|basecase`), and two numbers are compared
only when the key and the unit class (percent, bps, multiple, currency, count) both match.
So `$1,200m` and `$1.2bn` tie out, an FY25E forecast never reads as a contradiction of the
FY24A actual, and a percent is never weighed against a multiple. It reports:

| check | level | meaning |
|---|---|---|
| `value_conflict` | fail | one metric and period carrying two different values |
| `phrase_conflict` | warn | the same wording ahead of two different values |
| `unit_mixing` | warn | one metric written at two scales on a single slide |
| `source_missing` | warn | a slide of five or more figures with no source line |

Output is a JSON report: `status`, `file`, `stats`, every `numbers` entry with its slide,
location, raw text, normalised value, unit class and key, then `findings`. With `--check`
the exit code is 1 when any fail exists, so it can gate a loop.

### Reading the findings

Two figures are expected to match only when **all seven** agree: entity, metric definition, period, currency, scale, scenario, and source version. A finding that fails one of the seven is a definition difference, and the deck's fix is a clearer label rather than a changed number.

Worked non-mismatches, each of which looks like a `value_conflict` and is not one:

- An adjusted metric against a lender-defined or covenant-defined version of the same metric. Different definitions, both correct, both needing their basis named on the slide.
- Net leverage against first-lien net leverage. Different numerators.
- A figure in basis points against the same figure as a decimal percent (250bp and 2.50%). Different presentation, same value.
- A change stated in percentage points written as a percent move ("margin rose 12%" for a 120bp move). This is a convention error, logged as unit and period ambiguity rather than as an arithmetic error, per `.agents/skills/research-conventions/references/market-data-rules.md`.

### Verify by hand what the script cannot

- Calculations are correct (totals, percentages, growth rates).
- Bridges and waterfalls add up to the totals they claim.
- Scale notation holds across pages ($M vs $MM, $B vs $Bn), rather than only within one.
- A basis-point move is written in basis points, and a margin delta in percentage points.
- Fiscal and calendar periods are not mixed inside one table, and each is labelled.
- Reported, adjusted, street and model versions of a metric are labelled where they appear, rather than used interchangeably.
- A denominator change or a sign-convention change is explained where it happens.
- Every data slide carries a source or as-of line.

Flag pattern:

```
ISSUE: EBITDA margin mismatch (key ebitda margin|fy2024a)
CONFIDENCE: confirmed internal mismatch
- 24.5% on Slide 2 (BodyBox) and Slide 3 (Table 3 r3c2)
- 24.9% on Slide 5 (BodyBox)
ACTION: Reconcile to a single figure
```

Done when every `value_conflict` is either a written finding or dismissed with the specific matching condition that fails, every hand-verification line above has been run, and every finding carries a confidence label.

## Step 3: Data, narrative and charts

Map each claim to the data that supports it: trend statements to chart direction, market-position claims to share data, factual assertions to a source.

```
ISSUE: Narrative contradicts data
CONFIDENCE: confirmed internal mismatch
- Slide 4: "declining margins"
- Slide 7 chart: margins 18% to 22%
ACTION: Update narrative or verify data
```

**Per chart**, record and check:

1. Title, period, units, series names and source line, all present.
2. The visual direction against the title and against the takeaway bullet above it.
3. Labelled values against the underlying table, model or data pull.
4. Whether a truncated axis baseline exaggerates the move, and whether that is disclosed.
5. Whether each series is actuals, estimates, guidance or model output, and whether the chart says so.
6. Whether the chart is an embedded image, in which case it is logged as unextractable and checked by eye.

**Unsupported claims** to trace or log: market-leadership claims with no share data; through-cycle resilience claims that a downturn year in the same deck contradicts; manageable-balance-sheet claims with no liquidity or maturity support; attractive-valuation claims with no comparator; plausibility failures ("#1 player in a $100B market" beside $200M of revenue is 0.2% share).

Done when every chart carries its six-point record, every superlative or positioning claim is traced to data or logged as unsupported, and each finding carries its confidence label.

## Step 4: Language polish

Scan for casual phrasing ("pretty good", "a lot of"), vague quantifiers with no figure, contractions, exclamation points, and terminology that shifts between pages. Replacement patterns: [references/ib-terminology.md](references/ib-terminology.md).

```
ISSUE: Casual language (Slide 12)
- "This deal is a no-brainer"
to "The transaction presents a compelling value proposition"
```

Done when every flagged phrase carries its replacement.

## Step 5: Source and footnote coverage

Grade every page, and report the grade rather than only the failures:

| Grade | Means |
|---|---|
| `complete` | every figure and claim on the page is sourced, and the as-of is present where the data moves |
| `partial` | the page carries a source line that does not cover all of its figures |
| `missing` | data or claims with no source at all |
| `not applicable` | a divider, agenda, process or contents page carrying no data |

Done when every page holds one of the four grades and every `missing` page lists which figures lack support.

## Step 6: Formatting QC

Audit each slide for chart source citations, axis labels and legends; consistent fonts and size hierarchy; consistent number formatting (1,000 against 1K); one date format; and footnote placement. The `shapes` inventory carries font sizes and positions per slide, so typography drift and overlapping boxes are visible in the same JSON.

Formatting findings stay in the formatting section. One is promoted to a substance finding only when it obscures the analysis or actively misleads (a legend that mislabels a series, a footnote that contradicts the chart), and the promotion says which of the two it is.

Done when every slide has been audited and no formatting finding sits in a substance section without a stated reason.

## Step 7: Verdict and delivery

The pass ends in one posture for the document, read from the ladder in `.agents/skills/research-conventions/SKILL.md` against the review's input state.

A deck review supplies the inputs. A confirmed internal mismatch or an externally verified error on a load-bearing figure, a source gap on a decision-critical claim, or unit and period ambiguity in a number the reader acts on each leave a load-bearing claim unsupported, which the ladder reads as `not-ready`. A deliberately partial review, a subset of pages or a single dimension, is thin coverage stated as such in the review-scope table, which reads `screen-grade`. A controlling artifact that cannot be read at all, missing or image-only, is an input that cannot be obtained, which reads `blocked`.

**Say what could not be proven.** A number that may be wrong and cannot be shown wrong from the available files is reported as `needs review` with the specific document, model tab or data pull that would settle it. Dropping it because it is unprovable ships the risk silently.

**Route the work that is not QC.** A finding whose fix is a model change goes to `.agents/skills/model-update/SKILL.md`, a model that needs auditing to `.agents/skills/check-model/SKILL.md`, a valuation to rebuild to `.agents/skills/dcf-model/SKILL.md` or `.agents/skills/comps-analysis/SKILL.md`, and a claim that needs fresh research to the skill that owns it. The QC pass names the owner and the input needed rather than solving it inline.

Severity stays orthogonal to confidence: **Critical** (number mismatches, factual errors, a contradicted narrative), **Important** (language, alignment, source gaps), **Minor** (formatting).

Present findings using the template in [references/report-format.md](references/report-format.md), which carries the review-scope table, the decision-critical tie-out that leads the report, the remediation order, and the compressed fast-readout variant. Field requirements per issue type, and the full issue-type list: [references/issue-taxonomy.md](references/issue-taxonomy.md), read while writing up any consequential finding, a plain number mismatch included.

Done when the report carries one posture with its reason, every finding carries a severity and a confidence label, every `needs review` finding names the support that would settle it, and the review-scope table states what was inspected visually, what was tied out, what was externally verified and what was not verified at all.
