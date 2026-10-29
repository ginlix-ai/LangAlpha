# Deck Check Report Format

The template, the order findings appear in, and the fast variant. Confidence labels and issue-type field requirements come from `SKILL.md` and [issue-taxonomy.md](issue-taxonomy.md).

## Report Template

```markdown
# Deck Check Report: [Presentation Name]

## Verdict
- Posture: [decision-grade / review-ready / screen-grade / not-ready / blocked], because [the finding or gap that set it]
- Must fix before circulation: X
- Total issues: X (Critical X, Important X, Minor X)

## Review Scope
| Dimension | Covered | Not covered |
|---|---|---|
| Pages inspected visually | slides 1-18 rendered and read | appendix 19-24 |
| Tied out to the model | valuation, EBITDA bridge | segment build |
| Externally verified | market size (filing), peer multiples (data pull) | management quotes |
| Not independently verified | third-party market forecasts | |

## Decision-Critical Tie-Out
The valuation, target price, rating or recommendation, checked against its controlling source, before anything else. State the deck figure, the controlling figure, whether they agree, and the impact if they do not. Where the deck carries no such figure, say so in one line.

## Critical Issues

### Number Consistency

One row per occurrence. Key and location come straight from `extract_numbers.py`, so the
reader can go from the report to the box on the slide without hunting for it.

| Key | Unit | Value | Slide | Where | Confidence | Action |
|---|---|---|---|---|---|---|
| ebitda margin\|fy2024a | percent | 24.5% | 2 | BodyBox | confirmed internal mismatch | Reconcile to a single figure |
| ebitda margin\|fy2024a | percent | 24.5% | 3 | Table 3 r3c2 | | |
| ebitda margin\|fy2024a | percent | 24.9% | 5 | BodyBox | | |

Numbers the script keys as `unknown` are never compared. Check those by eye.

### Data-Narrative Alignment
1. **[Issue name]** (Slides X, Y) | [confidence]
   - Claim: "[quoted text]"
   - Data shows: [contradiction]
   - Action: [recommendation]

## Important Issues

### Source Coverage
| Page | Grade | Missing |
|---|---|---|
| 4 | partial | market-size figure has no source |

### Language Polish
1. **[Issue type]** (Slide X)
   - Current: "[quoted text]"
   - Suggested: "[replacement]"

## Minor Issues

### Formatting
1. **[Issue type]** (Slide X)
   - [Description and fix]

## Open Items (needs review)
| Item | Why unprovable here | Support required |
|---|---|---|

## Routed Elsewhere
| Finding | Owner skill | Input needed |
|---|---|---|

## Final Checklist
- [ ] Numbers reconciled
- [ ] Narrative matches data
- [ ] Charts checked against their underlying data
- [ ] Language meets IB standards
- [ ] Source or as-of text on every data slide
- [ ] One scale per metric per slide
- [ ] Formatting consistent
```

## Remediation Order

Fix in this sequence, because each stage changes what the next one is checking:

1. Numbers and their sources: mismatches, calculation errors, source gaps on load-bearing figures.
2. Chart and narrative mismatches, once the numbers underneath them are settled.
3. Unit, period and caveat ambiguity.
4. Footnotes and source lines.
5. Language.
6. Formatting.
7. Re-run the QC pass from step 2 of `.agents/skills/check-deck/SKILL.md`, including `python .agents/skills/check-deck/scripts/extract_numbers.py deck.json --check`, and update the posture.

## Fast Readout

When the user wants the answer rather than the report:

```markdown
**Posture**: [rung], because [one clause]
**Must fix before circulation**: [1-5 items, each with its slide]
**Should fix**: [items]
**Looks clean**: [what was checked and passed]
**Not verified**: [what could not be checked, and what would let us]
```

## Issue Severity Classification

Severity is about consequence; confidence is about proof. A finding carries both.

**Critical** (must fix before the deck circulates):
- Number mismatches and calculation errors
- Factual inaccuracies (names, titles, dates, identifiers)
- Data contradicting the narrative
- A missing source on a decision-critical claim

**Important** (should fix):
- Casual or informal language
- Vague claims with no specifics
- Terminology inconsistency
- Missing chart sources, partial page coverage

**Minor** (polish):
- Font and colour inconsistencies
- Date format variations
- Spacing and alignment
- Orphaned text
