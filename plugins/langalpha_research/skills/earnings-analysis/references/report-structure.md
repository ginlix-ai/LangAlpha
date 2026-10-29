# Report Structure

Deferred reference for `earnings-analysis`. Load it while writing the DOCX, or when a section needs its exact shape. Page ranges are the eight-to-twelve-page deep dive; the one-pager mode compresses the same order into its stated length.

| Pages | Section |
|---|---|
| 1 | Summary, decision box, investment impact, updated estimates |
| 2 to 3 | Results analysis: revenue, profitability, recurring-EPS bridge |
| 4 to 5 | Operating metrics, guidance, cash quality |
| 6 to 7 | Debate map, thesis impact, transcript Q&A map |
| 8 to 10 | Valuation and estimate detail |
| 11 to 12 | Appendix, optional |

## Page 1: Summary

Header block: company and ticker, quarter and fiscal year, report date, rating with its direction (maintain, raise, lower), price with its as-of, and the price target old against new.

Then the results box:

```
Q[X] FY[YEAR] RESULTS: BEAT / IN LINE / MISS

                Reported    Consensus    Variance
Revenue         $X,XXX      $X,XXX       +$XXX (+X%)
EPS (adj)       $X.XX       $X.XX        +$X.XX
[Key metric]    XXX         XXX          +X%
```

### The decision box

Directly under the results box, four fixed lines and nothing else. This is what a reader who stops after page 1 takes away.

| Line | Says |
|---|---|
| Thesis | strengthened, weakened, unchanged or mixed, in one clause with the reason |
| Estimate revision | the direction and rough size for the next four quarters, and when the revision lands |
| Stock and valuation skew | what the print did to the risk and reward at the current price |
| Next catalyst | the dated event that settles the open question, with its date |

The next-catalyst line names a real scheduled event or a reporting cadence. When nothing is scheduled, it says so and names the observable that would move first.

### Investment impact

Three or four bullets, each a bold conclusion followed by a paragraph carrying the evidence. Lead every one with a number.

```
- **Revenue beat 3% on direct-to-consumer, and the mix shift looks structural**

  Revenue of $13.5B beat consensus of $13.2B by $300M (2%), with direct-to-consumer
  up 18% y/y against our 12% estimate and wholesale down 5% against flat. Management
  attributed the gap to digital demand and two product launches. Direct-to-consumer
  is now 42% of revenue against 38% a year ago, which is the channel-shift line of
  the thesis showing up in the numbers rather than in the commentary.
```

### Updated estimates table

Old against new for the current and next fiscal year, with a change column, ending on the multiple the target rests on. Rate lines change in basis points, level lines in percent. The reason each line moved goes in the body, not in the table.

## Pages 2 to 3: Results analysis

**Revenue.** The beat or miss against both our estimate and consensus, then the decomposition by segment, geography, product or channel, then the trend against the prior four quarters and against guidance. One quarterly progression table, columns Q[X-3] through Q[X] with year-over-year and sequential change.

**Profitability.** Gross, operating and net margin over the same four quarters, each change in basis points, with the drivers split into what helped and what hurt. Below-the-line items get their own paragraph: interest, other income, mark-to-market, FX remeasurement, tax rate.

### The recurring-EPS bridge

Include this whenever the EPS-quality screen in `.agents/skills/earnings-analysis/references/workflow.md` step 5 fired. Five rows, three columns:

| Step | Amount per share | Source and treatment |
|---|---|---|
| Reported diluted EPS | $X.XX | 10-Q, page N, consolidated statements of operations |
| Non-operating and non-recurring items | ±$X.XX | each item named, with its source and whether we accept, reject or flag the adjustment as unproven |
| Tax normalisation | ±$X.XX | effective rate used against the guided or trailing rate, with the arithmetic |
| Share-count normalisation | ±$X.XX | diluted share count used and why it differs from the reported count |
| Estimated recurring EPS | $X.XX | model-derived, per the evidence labels |

Close with one line naming the consensus basis actually used for the surprise, since a bridge against the wrong basis restates the same error more precisely.

When the screen did not fire, the section carries the single line "no material trigger identified" and the trigger list is not reproduced.

## Pages 4 to 5: Metrics, guidance and cash

**Operating metrics.** The metrics this company is run on, four quarters wide, with our estimate and the variance beside the current quarter. A metric the company stopped disclosing keeps its row and carries the absence word.

**Guidance.** New against prior against consensus, in one table, with the implied quarterly path where only an annual figure was given. Then our assessment: what has to be true for the guide to hold, this management's history against its own guides, and whether the conservatism is steady or fading. Call-only guidance is labelled in the table itself.

**Cash quality.** Operating cash flow, capex, free cash flow and the working-capital swing by component, with the same four-quarter window. State in one sentence whether cash confirms or contradicts the reported earnings.

## Pages 6 to 7: Debate and thesis

### Debate map

The section that carries the disagreement rather than resolving it prematurely.

| Side | The case in one sentence | What changed this quarter | Falsifier | Catalyst that settles it |
|---|---|---|---|---|
| Bull | | | | |
| Bear | | | | |

Each falsifier is an observable with a threshold and a source that will show it. Each catalyst is a scheduled event or a real reporting cadence, never an invented date.

### Thesis impact

One block per thesis pillar: the pillar as originally written, its status (strengthened, unchanged, weakened), and 150 to 200 words on the specific evidence from this print that moved it. A pillar the quarter said nothing about is marked unchanged and given one line, not a paragraph.

### Transcript Q&A map

The table from `.agents/skills/earnings-analysis/references/workflow.md` step 9, ordered by how much of the call each topic consumed. Quotes carry the speaker and a transcript line range. When there is no transcript, the section states which artifact is missing, when it is expected, and which questions it would answer.

## Pages 8 to 10: Valuation and estimates

Updated DCF inputs, each changed input showing old against new. Updated comparable multiples with the peer set named and each peer's basis stated per `.agents/skills/research-conventions/references/market-data-rules.md`. Then the price target: the method, the weights, the implied multiple, and the arithmetic from the multiple to the target.

The detailed estimate table runs the full P&L progression for the current and next fiscal year, old against new against change, with segment lines where the model carries them.

## Pages 11 to 12: Appendix, optional

Quarterly model detail, extended transcript excerpts, peer results where the peer set has reported, and anything that supports a page-1 claim without belonging on page 1.

## Formatting

- **Charts**: caption "Figure N. Title" above, source line below, units matching the table beside them.
- **Tables**: header row shaded, source line at the bottom, one currency and one scale throughout.
- **Notation**: A for actual, E for estimate, applied to every period label (Q3'24A, Q4'24E).
- **Style**: lead with the number, "vs." rather than "versus", and no sentence that would be equally true of any company in the sector.
- **Section titles state the finding**, not the topic: "Margin expansion is mix, not cost" beats "Margin analysis".
- **Hyperlinks**: display text rather than raw addresses, blue and underlined, SEC filings pointing at the EDGAR viewer, and every link opened once to confirm it resolves.

## Citation examples

The evidence contract in `SKILL.md` requires the artifact plus a location pointer. In practice:

```
Revenue of $2.45B beat consensus of $2.39B by $60M (2.5%).
  Consensus: get_company_overview, as of the close before the release.
  Reported: Q3 FY2024 earnings release, page 1, summary table. [linked]

Management raised FY2024 revenue guidance to $9.8-10.0B from $9.5-9.7B.
  Q3 FY2024 earnings call, CFO prepared remarks, lines 84-97. Call-only guidance:
  the range does not appear in the 8-K exhibit. Prior range: Q2 release, page 2. [linked]

Enterprise customers grew 23% y/y to 845, with net retention at 128%.
  Q3 FY2024 10-Q, page 23, supplemental metrics table. [linked]
  Investor presentation, slide 8, same figures. [linked]
```
