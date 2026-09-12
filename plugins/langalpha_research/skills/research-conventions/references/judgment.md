# Judgement

Deferred reference for `research-conventions`. Load it before writing a valuation, a thesis, a recommendation, a scenario set, or any section that ends in what to do.

Evidence discipline decides what may be said. This file decides whether what is said is worth a reader's time.

## The seven questions

A substantial output answers all seven somewhere, in whatever order its own structure wants. A section that answers none of them is background, and background is the first thing to cut when the artifact runs long.

1. **Variant view.** What do we believe that the market does not, and which evidence creates the difference?
2. **What is priced.** At the current price, what does the market already assume? Apply the `priced in` test below.
3. **Estimate path.** Where do estimates go over the next two to four quarters, and which way does revision risk lean?
4. **Mechanical downside.** What breaks, through which line item, to what number? A narrative risk is not a downside case.
5. **Catalyst.** Which dated event converts the view into price, and is it scheduled, likely or speculative?
6. **Falsifier.** Which observation would make us wrong, and by when is it observable?
7. **Action.** What does the reader do, at what level, and what would change it?

## Action language, gated by inputs

One closed vocabulary, and the verb comes from it: `add`, `press`, `hold`, `trim`, `exit`, `hedge`, `watchlist`, `wait for proof`, `re-underwrite`.

Each verb needs its inputs in hand before it is available:

| Verb | Available once you have |
|---|---|
| `add`, `press` | a live price, a valuation anchor, and a catalyst inside the horizon |
| `hold`, `trim`, `exit` | a known existing position to act on |
| `hedge` | the instrument, its cost, and a statement of the basis risk it leaves |
| `watchlist` | a trigger level or a dated event |
| `wait for proof` | the named proof and where it will show up |
| `re-underwrite` | the specific input that broke |

Three vocabularies stay apart. The action verbs above are position actions. Rating words (`upgrade`, `downgrade`, `maintain`) describe a change to a published view and sit beside an action verb, never in place of one. Benchmark stance (`overweight`, `underweight`, `avoid`) is the long-only rendering of `add`, `trim` and `exit` and appears only in that audience mode.

Two artifacts carry a registered extension, and no other skill adds to the vocabulary. An initiation report prints a rating on one ladder, `Buy`, `Hold` or `Sell`, beside the action verb, because its readers expect a rating line. A trade pitch states size as a fraction of a stated loss budget, never a dollar amount or a share count, because the expression is the artifact.

Without holdings, sizing inputs or live execution data, which is the usual case, the honest verbs are `watchlist`, `wait for proof` and `re-underwrite`. Name the missing input in the same sentence and say what would convert the screen into an action. These skills produce analysis: the vocabulary stops short of personalised investment advice and of execution instructions, which is why it ends at `re-underwrite` rather than at a size.

## Price is belief, not truth

The current price says what the market expects, not what is so. Keep the two claims apart in the writing: "the multiple implies growth of X" is a statement about belief; "the company grows at X" is a statement about the company. A view is the gap between them, so blurring the two erases the view.

## Priced in

A claim is `priced in` when the affected names' price move and their estimate revisions both already carry the channel's effect on the line item, in the right direction and at roughly the right size. A multiple that sits where the news implies corroborates the label; on its own it is not the test. Anything short of both reads `needs proof`, and the label names the print, filing or release that would put the claim in the numbers, with its date. Ground the test in the affected names and their revisions, never in an index move.

## The strongest counterargument

Every conclusion carries the best case against it, put at its strongest as someone who believes it would put it, then answered with one of four verdicts: weaker, later, already priced, or it wins. When it wins, the conclusion changes to match and the artifact says the counterargument moved it. A counterargument that can be dismissed in a clause was not the strongest one available.

## Name the binding constraint

When several lenses give different answers (valuation, liquidity, mandate, conviction, risk budget), the answer is the most restrictive credible one, and the output names which lens bound it. That name is what lets the answer be re-derived when one input moves.

## Every remedy carries its failure

Any proposed mitigation, a hedge, a staged entry, a stop, a diversification, ships with at least one scenario in which the mitigation itself does not work, and beside the plain alternative of taking less risk instead.

## Monitoring items

A monitored item names five things: the metric, the threshold, the source that will show it, the date or window when it becomes observable, and the action each side of the threshold triggers. As a table that is `| Metric | Threshold | Source | Window | Action if crossed | Action if not |`; a skill that tracks items over time adds columns (a confirming and a disconfirming signal, provenance) and keeps these six. "Watch the next print" is a placeholder; "net revenue retention below 108% in the Q3 release on 2026-11-04 retires pillar two" is an item.

## Falsifiable claims

Before an artifact is finished, convert its narrative into two to five claims. Each carries six fields:

| Claim | Evidence today | Implication if true | Test metric | Falsifier | Time to knowable |
|---|---|---|---|---|---|

Fewer than two claims means the artifact holds no view. More than five means the view was never prioritized.

## Asymmetric readiness

Evidence sufficient to decline is not evidence sufficient to initiate. A short, well-sourced answer saying the work does not support a position, and naming what would, is a finished answer rather than a failure to deliver. Say which side of the asymmetry the evidence reaches.

## Discounting

A case built on forward earnings times an exit multiple is not a return until it is discounted. Show a present value with the discount rate stated, or an annualised return against a stated hurdle, and state the horizon either way. Terminal appreciation left undiscounted flatters every multi-year case by exactly the time value it skipped.

## Probabilities

A probability set is complete, meaning its states are mutually exclusive and cover the space, and it sums to one. When it does not, withhold the weighted figure, show the scenarios unweighted, label them an illustrative skew, and say why the set is incomplete. An expected value computed from a set that does not sum to one supports no conclusion, however carefully the scenarios were built.

## Orders of effect

Report direct, second-order and third-order effects in separate rows or paragraphs, at different confidence. A direct effect names the channel and the line item it lands on. A second-order effect names whose behaviour has to change in between. When the transmission channel cannot be named, the claim is not ready to write and the next step is source work rather than prose.

## Row-merge discipline

In any exposure, driver or comparison table, two names share a row only when the channel (or dimension), the first line item it lands on, and the direction all match. Thematic adjacency is not a merge: two names touched by the same shift through different line items are two rows, and a name exposed through two channels with different signs or lags is two rows. A merged row that hides a direction difference is the failure this rule exists to catch.

## Audience modes

The same facts, a different first paragraph. Take the mode from the mandate memory (`.agents/skills/research-conventions/references/intake.md`) or from what the user asked for, and lead with what that reader reads first.

| Mode | Reads first | Ends on |
|---|---|---|
| Long-only, benchmark-aware | the position against the benchmark weight, and what the estimate path does to it | the benchmark stance (overweight, underweight or avoid) rendered from the action verb, with the benchmark risk named |
| Long/short | the variant view, and whether the short leg is implementable at all | the expression and its falsifier |
| Event | the dated event, its conditions and the payoff around it | timing risk, and what breaks the event |
| Index-aware | flow, weight changes and what is mechanically forced | the dates the flow happens on |
