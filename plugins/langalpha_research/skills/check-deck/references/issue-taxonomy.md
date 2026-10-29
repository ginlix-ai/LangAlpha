# Issue Taxonomy

Deferred reference for `check-deck`. Read it while writing up any consequential finding, a plain number mismatch included, and when deciding whether a finding belongs in the substance sections or the formatting section.

Each type states what it covers and the fields a finding of that type must carry. A finding missing a required field is not yet reportable: the missing field is usually the one the fixer needs. Every consequential finding also carries its confidence label (`confirmed internal mismatch`, `externally verified error`, `needs review`) and its severity (Critical, Important, Minor).

## Substance

### Number mismatch

One metric, one period, two values in the deck.

**Fields**: every location (slide, shape or `Table r c`), the conflicting values as printed, the script key where it came from `extract_numbers.py`, which value the supporting artifacts indicate is controlling and why, and the fix. Where no artifact settles it, the finding is `needs review` and names the model tab or filing that would.

### Calculation error

A total, growth rate, margin, bridge or waterfall that does not equal its own components.

**Fields**: the location, the arithmetic as printed, the arithmetic recomputed step by step, the source of the inputs, and whether downstream pages inherit the error.

### Unit and period ambiguity

A figure whose meaning depends on a convention the page does not state: a basis-point move written as a percent, a margin delta as a percent rather than percentage points, scale notation that shifts between pages, fiscal and calendar periods inside one table, or reported, adjusted, street and model versions of a metric used interchangeably.

**Fields**: the location, the ambiguous expression, both readings it permits, which one the surrounding data supports, and the label that removes the ambiguity.

### Source gap

A figure or claim with no source, or a source line that does not cover the figures on its page.

**Fields**: the claim or figure, the page's current source line if any, the specific missing element (document, period, tool call, as-of), and the page's coverage grade.

### Source conflict

Two artifacts, or an artifact and a primary document, disagreeing on the same figure.

**Fields**: both sources with their values and dates, the tier of each per the source tiers in `.agents/skills/research-conventions/references/evidence.md`, the selected value and why, and what would settle it if nothing does. Averaging the two is never the resolution.

### Chart and narrative mismatch

A chart whose direction, magnitude or series contradicts the title above it, the takeaway bullet beside it, or the body text elsewhere in the deck.

**Fields**: the chart location, the claim quoted, what the chart shows including the values read off it, whether the chart or the claim is wrong, and the fix on the side that is wrong.

### Narrative contradiction

Two statements in the deck that cannot both hold.

**Fields**: both statements quoted with locations, the fact at issue, which one the supporting artifacts support, and the fix.

### Unsupported claim

A positioning, resilience, balance-sheet or valuation claim with nothing behind it: market leadership with no share data, through-cycle resilience contradicted by a downturn year in the same deck, a manageable balance sheet with no liquidity or maturity support, an attractive valuation with no comparator.

**Fields**: the claim quoted with its location, the evidence that would support it, whether that evidence exists in the supporting artifacts, and either the citation to add or the softer claim the evidence does support.

### Factual or entity error

A name, title, date, identifier, transaction detail or third-party fact that a primary source contradicts.

**Fields**: the claim, the primary source that contradicts it with its citation, and the correction. Without that source in hand the finding is `needs review`: the deck disagreeing with itself does not establish an external fact.

### Caveat and disclosure gap

A figure that needs a stated basis to be read correctly and does not carry it: pro-forma adjustments, a non-GAAP bridge, an unusual accounting treatment, a projection presented without its assumption set, survivorship in a track record.

**Fields**: the figure, the missing caveat, where the caveat is required (the page itself or a footnote), and the wording to add.

### Missing support file

The deck references a model, appendix, data pull or prior version that was not supplied.

**Fields**: what is referenced, where, what it would let us verify, and which findings stay `needs review` until it arrives.

## Presentation

Findings here stay in the formatting section unless they obscure the analysis or actively mislead, in which case the promotion to a substance finding says which of the two it is.

### Chart label and format issue

Missing axis label, missing legend, missing units, a truncated baseline that is not disclosed, a colour scheme that stops distinguishing series, or a data label that disagrees with its own bar.

**Fields**: the chart location, the defect, whether it misleads or merely reads poorly, and the fix.

### Formatting consistency

Font, size, colour, number format, date format, alignment, capitalisation or footnote-marker drift.

**Fields**: the convention the deck mostly follows, every location that departs from it, and the single convention to standardise on.

### Readability and accessibility

Type below the deck's floor size, text overflowing its shape, overlapping shapes, contrast too low to read when projected, or a table too dense for the space it sits in.

**Fields**: the location, what is unreadable and at what size, and the remedy (font, wording length, or the boundary between elements).
