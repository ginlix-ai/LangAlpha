# Quality Gates and Failure Modes

Deferred reference for `earnings-analysis`. Load it before delivery, and whenever a headline or a claim needs calibrating.

Three tiers, run in order. A hard fail stops delivery. The checklist is mechanical completeness. The judgement gate asks whether the note was worth reading.

## Tier 1: Hard fails

Any one of these sends the draft back. They are the failures a reader cannot detect and cannot recover from.

- A user-facing number without a findable citation, meaning artifact plus location pointer.
- A figure compared across bases with no label: adjusted against GAAP, net revenue against gross billings, reported against constant currency.
- The transcript cited as the source for a filed number.
- An unresolved placeholder anywhere in the document: XX, TBD, a bracketed template token, a chart captioned but not embedded.
- Two sections naming different fiscal periods for the same event.
- A surprise percentage computed against a zero, near-zero or negative expectation.
- Guidance presented as company guidance when it exists only in call commentary and carries no call-only label.
- A price target or rating that is neither explicitly changed nor explicitly maintained.

## Tier 2: Delivery checklist

**Analysis**
- [ ] Beat, in line or miss stated in the first sentence, with the variance quantified for revenue and EPS.
- [ ] Two to three load-bearing drivers named, each with what moved, why, and the forward consequence.
- [ ] EPS-quality screen concluded: either the recurring-EPS bridge or the "no material trigger identified" line.
- [ ] Cash quality stated as confirming or contradicting the reported result.
- [ ] Guidance bridged (new against prior against consensus), or its absence stated.
- [ ] Estimates revised for the current and next fiscal year, old against new, with a reason per changed line.
- [ ] Decision box carries four filled lines.
- [ ] Debate map carries a falsifier on each side, each tied to a dated catalyst.
- [ ] Every thesis pillar carries a status.

**Sources**
- [ ] Every figure, table and quote carries its artifact and location pointer.
- [ ] Consensus names its estimate set and as-of.
- [ ] Prior guidance cites the previous quarter's materials.
- [ ] Every non-GAAP figure shows its GAAP comparable and the reconciliation source.
- [ ] Absent data carries one of the four absence words.
- [ ] All links are hyperlinks with display text, SEC filings pointing at EDGAR, each one opened once to confirm it resolves.
- [ ] The closing Sources section lists every material with its date.

**Build**
- [ ] Deep dive only: length inside the budget of 8 to 12 pages, 3,000 to 5,000 words, 1 to 3 tables, 8 to 12 charts. A short mode is checked against the contract in its row of the mode table instead.
- [ ] Every chart numbered, captioned and sourced, with units matching the adjacent table.
- [ ] Reported figures match the release exactly, and every derived figure recomputes from the raw pull.
- [ ] Period labels, ticker and company name spelled the same way throughout, with A and E notation applied.
- [ ] Published inside 48 hours of the release, or the delay acknowledged in the note.

## Tier 3: Judgement gate

A note that passes tiers 1 and 2 and fails here is accurate, complete and not worth a reader's time. Answer all five:

1. **What changed?** The note says what is different now, not what happened. A chronological account of the quarter fails this gate on its own.
2. **Does it move anything?** A reader can name one estimate, one thesis pillar or one position decision that is different after reading.
3. **Is the beat real?** The note distinguishes the part of the result that recurs from the part that does not, and the estimate revision follows that split.
4. **Is the disagreement visible?** The strongest case against our conclusion appears at its strongest, then is answered with one of the four verdicts in `.agents/skills/research-conventions/references/judgment.md`: weaker, later, already priced, or it wins, in which case the conclusion changes.
5. **Is the next check dated?** Every open question names the observable and the real event that resolves it.

The seven questions in `.agents/skills/research-conventions/references/judgment.md` are the fuller version of this gate; the five above are the ones a quarterly note fails most often.

## Named failure modes

These are the specific ways an accurate earnings note goes wrong. Each one reads as competent work.

| Failure | What it looks like | The fix |
|---|---|---|
| Mixed bases | an adjusted figure compared to a GAAP estimate, or one company's net revenue to another's gross billings | fix the pair before comparing, and label the basis in the header |
| Capitalising a one-time beat | forward estimates raised on an item the EPS-quality screen identified as non-recurring | revise on the recurring bridge, and say in the reason column that the item is excluded |
| Transcript as the source of record | a filed number cited to what the CFO said about it | cite the filing, and use the quote as the explanation beside it |
| Surprise against nothing | a percentage surprise computed on a near-zero or negative expectation | show the absolute delta and state why the percentage is unavailable |
| An invented range | "modest growth" turned into a 3 to 5% figure that then feeds the model | keep qualitative guidance qualitative, and mark the model input an assumption |
| A quote clipped past its meaning | a fragment that reverses or flattens what the speaker said | quote the full clause, with the line range so a reader can check |
| The summary that decides nothing | every metric narrated, no thesis, revision or skew | run the judgement gate above and rewrite around what changed |
| Sector wallpaper | sentences equally true of any company in the sector | replace with the number that is true only of this one |

## Headlines

The headline is the note. It carries the finding, the driver and the action.

**Strong**
- "Q2 FY24: direct-to-consumer offsets wholesale weakness, maintaining Overweight, target $95"
- "Q3'24: production ramp ahead of plan, raising estimates, target to $285"
- "Q1 FY24: services beat, hardware miss, mixed quarter, lowering target to $185"

**Weak**
- "Quarterly update" and "Q3 results analysis": the topic, not the finding.
- "Strong quarter": an adjective where a number belongs.
- "Company reports earnings": true of every note ever written.

## Delivery message

Short, and it never restates the report:

```
[Company] Q[X] FY[Year] earnings update

Result: BEAT / IN LINE / MISS
  Revenue $X.XB (beat by $XXM, X%); EPS $X.XX (beat by $X.XX)

Decision box
  Thesis: [strengthened / weakened / unchanged / mixed], because [one clause]
  Estimates: FY[Year]E EPS $X.XX from $X.XX; FY[Year+1]E $X.XX from $X.XX
  Skew: [one clause at the current price]
  Next catalyst: [event, date]

Rating [MAINTAINED / RAISED / LOWERED]; target $XXX from $XXX
Model update delivered as: [driver packet / applied to your workbook copy]
Assumed defaults: [any default taken during intake, with the one-line change if wrong]

File: [Company]_Q[X]_[Year]_Earnings_Update.docx
```
