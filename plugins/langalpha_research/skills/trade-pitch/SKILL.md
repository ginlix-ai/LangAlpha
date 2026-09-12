---
name: trade-pitch
description: "Turn finished analysis on one named security into a position: variant view, falsifiable evidence, scenario tree, expression, risk and monitoring. Triggers on pitch this name, should I buy, a long idea or short idea on a named stock, position, trade recommendation, risk reward."
---

# Trade Pitch

The terminus of the research. A finished analysis becomes a **position**: a direction, a variant view, the evidence that carries it, a scenario tree, an expression, a level that says we were wrong, and a plan for watching it. The pitch is research output for a professional reader, and its verbs describe what the analysis supports; personalised advice and execution instructions sit outside it, which is why the vocabulary ends at `re-underwrite` rather than at a size, and why size appears only as a fraction of a stated loss budget, per the registered extension in `.agents/skills/research-conventions/references/judgment.md`.

Every figure in a pitch is load-bearing, which makes this the deliverable where unsupported precision does the most damage. Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first figure enters the pitch.

Instrument mechanics for anything other than long common stock, the short-side gate, and the worked reconciliation of two loss limits: `.agents/skills/trade-pitch/references/expression.md`.

## Step 1: Intake, and the posture the inputs support

Read the investor-mandate memory first per `.agents/skills/research-conventions/references/intake.md`, then ask only about the forks it leaves open. Four things are fixed before any work starts:

| Slot | Taken from | Default when unstated |
|---|---|---|
| Direction | the request | the direction the source analysis already leans, stated as an assumption |
| Horizon | mandate memory, then the catalyst that closes the gap | two to eight quarters |
| The analysis it rests on | a named artifact: a `dcf-model` workbook, a `comps-analysis`, an `earnings-analysis` print, an `initiating-coverage` initiation, or the user's own stated view | none; see below |
| Mandate slots | mandate memory: vehicle, benchmark, risk posture, scope, instruments allowed | disclosed in the delivery message |

**The pitch rests on something named.** Record the artifact, its as-of, and which of its outputs the pitch uses (the fair-value range, the peer multiple, the revision path). Where no finished analysis exists, say so in one sentence, name which skill would produce it, and either build that first or deliver a `screen-grade` pitch that says the underwriting is still owed.

Posture is stated once near the top of the artifact, read from the ladder in `.agents/skills/research-conventions/SKILL.md` against the input state. For a pitch the inputs that count include a live price, a valuation anchor, a dated catalyst, an established borrow on a short leg, and the sizing inputs in `.agents/skills/trade-pitch/references/expression.md`.

**Done when** direction, horizon, the source artifact with its as-of, and every mandate slot carry a value or a disclosed default, and the posture is written with the specific input that set it.

## Step 2: The variant view

Four fields, and a pitch without all four is a summary of someone else's work:

1. **What the market believes.** At the current price, in the units of the driver the position rests on: the growth rate the multiple requires, the margin the price embeds, the estimate path consensus is carrying. Ground it in the affected names and their revisions, never in an index move.
2. **What we believe.** The same driver, our number, sourced to the analysis from step 1.
3. **Why the gap exists.** The mechanism that lets it persist: a disclosure lag, a KPI the street reads on the wrong basis, a misclassified business, a forced seller, a horizon mismatch, an event nobody has dated. A gap with no mechanism is usually an error in our own work.
4. **What closes it.** The dated event or the evidence arrival that converts belief into price, marked scheduled, likely or speculative.

The seven questions this section has to satisfy, the rule that price is belief rather than truth, and the requirement to answer the strongest opposing case: `.agents/skills/research-conventions/references/judgment.md`, read before writing this step.

**Done when** all four fields are written, the market's belief is stated in the driver's own units and sourced to the affected names, the gap names a mechanism, and the closer carries a date or a window and its confidence.

## Step 3: Evidence as falsifiable claims

Convert the argument into two to five claims, in the table of `.agents/skills/research-conventions/references/judgment.md`, with one column added: the evidence label from `.agents/skills/research-conventions/references/evidence.md` on each claim's evidence. The working table carries the research label; the delivered document prints the reader-facing word for it.

| Claim | Evidence today | Label | Implication if true | Test metric | Falsifier | Time to knowable |
|---|---|---|---|---|---|---|

A `needs-source` label is drafting state: before delivery the figure is sourced, or the claim takes the unsupported-claim exit in `evidence.md`.

**Done when** the table holds between two and five claims, each carries all six fields plus one evidence label, no `needs-source` label survives, and removing any one claim would change the recommendation.

## Step 4: Scenario tree

Three cases, each built from a driver rather than from a haircut to the base target:

| Case | Driver and level | Arithmetic to price | Return from spot | Probability | Provenance |
|---|---|---|---|---|---|

Each case moves one named driver to a stated level and carries the arithmetic through to a price, so a reader can move the driver and watch the case move. Provenance is the model, a source, the user, our judgement or illustration, with an as-of. Returns are discounted or annualised against a stated hurdle per the discounting rule in `judgment.md`, because a multi-year target price is not a return.

**Probabilities.** A complete set is mutually exclusive, covers the space and sums to one; the rule and what to do otherwise are in `judgment.md`. When the probabilities are assumed rather than underwritten, the table is labelled an **illustrative skew**, the weighted figure is withheld, and the posture is re-read from the ladder in `.agents/skills/research-conventions/SKILL.md` against that input state.

**Bear-case survivability.** A lower price is not a downside case. State what breaks, through which line item, to what number, and then whether the company survives it: liquidity through the trough, covenant headroom against the stressed metric, and the maturity wall inside the horizon. A company that cannot fund its own bear case has a different downside from one that can, and the difference is the whole risk.

**The exit in the bear case uses stressed liquidity, not normal.** Normal participation and stressed participation are separate lines, never one blended figure, and where the case is a gap risk the exit price is the post-move price rather than the level we would have liked. Mechanics and the arithmetic: `.agents/skills/trade-pitch/references/expression.md`.

**Done when** each case names its driver, its level and the arithmetic to a price; the bear case states liquidity, covenants and maturities through the trough; the probability set sums to one or the table is labelled an illustrative skew with the posture re-read against that state; and the bear-case exit carries a stressed participation figure beside the normal one.

## Step 5: Trade expression

| Field | Carries |
|---|---|
| Instrument | common, an options structure, or a pair, with the basis risk it leaves |
| Entry | a rule rather than a quote ("at or below 48"), with the reference price and its as-of beside it, and the tranches where it is staged |
| Target | from the valuation anchor in step 1, as a range rather than a point, discounted or annualised against a stated hurdle, saying which end the position underwrites |
| Exit | three levels. A **stop** is a price rule, set where the price itself is evidence, its distance stated in units of the name's own volatility. A **`re-underwrite` level** is a claim rule naming the input that broke, taken from the falsifier column. A **time stop** is the horizon date at which the absence of the catalyst is itself the answer |
| Size | a fraction of a loss budget, per the rule below |
| Binding constraint | which lens set the size: valuation, liquidity, mandate, conviction or the loss limit |

**Size against a loss budget, not a book.** We hold no positions, no net asset value and no mandate limits, so a percentage of a book would be a number we invented. State size as what it risks against a budget the reader brings: "at the stop this position spends a third of a 150bp loss budget" travels to any book size, and names the binding constraint that set it per `judgment.md`.

**The two loss limits are different constraints.** A limit stated *inside* a stress scenario ("lose no more than this by the time the bear case has played out") and an absolute cap that must hold *beyond* it ("lose no more than this in any state") give different sizes and sometimes different answers about whether the position exists at all. Resolve which one the mandate means before recommending anything; where the mandate does not settle it, show both branches rather than picking one. The worked example is in `.agents/skills/trade-pitch/references/expression.md`.

**The short-side gate.** A short pitch carries a gate block with six rows, each marked `cleared`, `not cleared`, `missing`, `n/a` or `illustrative`: catalyst, valuation anchor, liquidity, borrow and carry, option cost, hedge ratio. Any applicable row below `cleared` is named in the recommendation itself rather than in a footnote, and an `n/a` row is not a gap. An unhedged short cannot satisfy an absolute cap, because there is no price at which the loss stops, so under that reading the answer is a priced defined-loss structure or no position. What clears each row: `.agents/skills/trade-pitch/references/expression.md`.

**Every hedge carries the scenario in which it fails**, per `judgment.md`, and two rules sit beside it: a hedge that removes the reason the position exists is rejected rather than priced, and the alternative of taking no hedge and sizing down is always stated so the reader can compare them.

**Done when** the six expression fields are filled, size is stated against a loss budget with its binding constraint named, the two loss limits are resolved or both branches are shown, a short carries all six gate rows with one of the five verdicts each, and every proposed hedge carries a failure scenario beside the size-down alternative.

## Step 6: Monitoring

| Trigger | Metric | Threshold | Source | Date or window | Action each side |
|---|---|---|---|---|---|

`judgment.md` sets the bar for a monitored item; the pitch adds that every claim in step 3 is testable by at least one trigger, so a falsifier without a trigger is an intention rather than a plan.

**What would change our mind** is its own short block: the two or three observations that would retire the position, each with the date it becomes observable, drawn from the falsifier column rather than written fresh.

Hand the running ledger to `.agents/skills/thesis-tracker/SKILL.md`: the pitch's claims become its pillars, the step 3 falsifiers become its break thresholds, and the triggers here become its monitoring rows. The pitch is a dated document; the tracker is what keeps it honest afterwards.

**Done when** every trigger row carries its metric, threshold, source and date, with an action stated on each side of the threshold; every step 3 claim is covered by at least one trigger, and the hand-off names the tracker and the first review date.

## Step 7: Output

**Actionability leads, background follows.** The order, top to bottom: the recommendation line (action verb, direction, instrument, entry, target, exit, posture and as-of), the variant view in one sentence, the missing-inputs block, the scenario table, the expression and its risk, the claim table, monitoring, then the analysis the pitch rests on. A reader who stops after the first screen has the position; a reader who continues gets the underwriting.

**The action verb** comes from the closed vocabulary in `judgment.md` and is available only once its inputs are in hand. Without a live price, without sizing inputs, and with no position to act on, the honest verbs are `watchlist`, `wait for proof` and `re-underwrite`, each naming the missing input in the same sentence and saying what would convert the idea into an action.

**Reader-facing evidence vocabulary.** The label taxonomy stays in the working research and the document prints the reader's word for each label, one per label, per **Reader-facing labels** in `.agents/skills/research-conventions/references/evidence.md`.

**The missing-inputs block** is one compact block near the top, not a table of blanks. Each line names what is absent, what would supply it, and what it would change about the recommendation. A section whose input is unavailable keeps its heading and carries its labelled gap, because a short pitch reads as a thin idea while a full pitch with three labelled gaps reads as the truth.

Deliver one to three pages as Word through `.agents/skills/docx/SKILL.md`, or a deck through `.agents/skills/pptx/SKILL.md` when the user asked for slides. Save to `$WORK_DIR/work/{task}/`. Depth bands and what to cut first when the draft runs long: `.agents/skills/research-conventions/references/depth.md`.

**Done when** the first screen carries the action verb, the position, the posture and the variant view; every label the delivered document prints is the reader-facing word for its research label; the missing-inputs block names each absent input with what would supply it; and the file is written to the task directory.
