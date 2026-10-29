# Trade Expression

Deferred reference for `trade-pitch`. Load it when the expression is anything other than long common stock, when the position is a short, or when the two loss limits have to be reconciled.

Step 5 of the skill states what the expression block contains and settles the fields every pitch carries. This file settles what changes once the instrument is not plain long common stock.

## Instrument choice

Pick the instrument from the shape of the payoff the thesis needs, then price it. A thesis that needs a dated move buys time; a thesis that needs a durable rerating buys participation.

| Instrument | What it buys | What it costs | Basis risk it leaves |
|---|---|---|---|
| Long common | full participation, no expiry, no premium | the full downside, and the capital | none: the exposure is the thesis |
| Long call, or a call spread | participation above the strike for a known premium | premium and time decay; the spread caps the upside | being right and late, which pays nothing |
| Put spread | a defined maximum loss, being the net premium | premium, and a payoff capped at the lower strike | the move landing after the tenor |
| Short common | participation on the way down | borrow fee, recall risk, dividend and corporate-action carry, and a loss with no upper bound | none on the view, all of it on the financing |
| Short common plus a long call | converts the unbounded loss into a known one | the call premium, which is the price of the cap | the strike distance, which is loss taken before the cap starts |
| Single-name pair | isolates the variant view from the factor both names share | two sets of financing, and a second thesis you now own | the hedge leg moving for reasons of its own |
| Basket or index hedge | cheap, liquid removal of market beta | tracking that is loose by construction | the largest of any hedge here: the name falls while the index does not |

**A hedge that removes the reason the position exists is rejected rather than priced.** Hedging a long whose thesis is margin expansion with a short in the peer that expands margin on the same driver leaves a position with no exposure to the thing we did the work on. State the exposure the hedge is meant to remove, then check it against the variant view in step 2: where they are the same variable, the honest alternative is a smaller unhedged position.

Every hedge proposal ships with a scenario in which the hedge itself fails, and beside the plain alternative of taking less risk instead, per `.agents/skills/research-conventions/references/judgment.md`.

## Entry and exit mechanics

Step 5 of the skill defines the entry rule, the target range and the three exits. What follows is what changes once the expression is not plain long common stock.

**Staged entry is a mitigation, so it carries its own failure.** The failure is a thesis that works immediately and leaves most of the intended size unfilled, and the discipline problem of averaging into a claim that has since broken. State the tranches, the level or the event that releases each, and the point at which an unfilled tranche is abandoned rather than chased.

**Set the stop where the price itself is evidence**: the level at which the market is pricing a state the claims in step 3 say cannot happen. Where the expression cannot be stopped out cleanly, which is an option position, an illiquid line, or a pair whose legs move apart, the re-underwrite level is the exit that matters instead.

**For an options expression the tenor is the time stop** unless the roll is priced, so state the roll cost or accept the expiry.

## The short-side gate

Six rows, each carrying one verdict. Any applicable row below `cleared` is named in the recommendation itself.

| Verdict | Means |
|---|---|
| `cleared` | the input is in hand, with its as-of, and it permits the trade |
| `not cleared` | the input is in hand and it blocks or constrains the trade, or the input is required here and cannot be obtained |
| `missing` | the input applies to this expression, has not been sought, and its absence does not block the trade |
| `n/a` | the row does not apply to the chosen expression at all, so it is neither cleared nor a gap |
| `illustrative` | a figure carried to show the shape of the trade, never a basis for the recommendation |

| Row | Cleared when | Not cleared when |
|---|---|---|
| Catalyst | a scheduled or likely dated event inside the horizon converts the view into price | the only candidate is speculative, or the nearest dated event sits beyond the horizon |
| Valuation anchor | a level the short is short *to*, from the named source analysis, with its as-of | the anchor rests on an assumed driver, or there is none and the short is a momentum bet |
| Liquidity | the size clears a stated participation of average daily traded value inside a stated number of days, normal and stressed both shown | the stressed exit runs longer than the horizon or longer than the catalyst window |
| Borrow and carry | current availability, an indicative fee, recall risk, and dividend and corporate-action cost over the horizon | the fee makes the carry larger than the expected move, **or no borrow figure can be obtained at all** |
| Option cost | a live quote or a chain snapshot from the current session, per the freshness thresholds in `.agents/skills/research-conventions/references/evidence.md` | the structure is not listed, or the premium exceeds the payoff it buys |
| Hedge ratio | computed off a stated relationship with a stated lookback and as-of | the relationship is unstable over the lookback, so the ratio is a number rather than a hedge |

**Unobtainable borrow is `not cleared`, not `missing`.** The two verdicts send opposite messages: `missing` invites the reader to supply the input, while `not cleared` says the short is not implementable on what we know today. Short interest and borrow publish at their own lag, so an old figure is evidence of a prior state rather than of today's availability; where nothing current can be sourced, the row reads `not cleared`, the recommendation says the short cannot be sized until borrow is confirmed, and the posture, re-read from the ladder in `.agents/skills/research-conventions/SKILL.md` against that input state, reads `not-ready`.

A row at `illustrative` leaves one input open, so the posture is re-read from the same ladder against that state. Rows that do not apply to the chosen expression, which is option cost and hedge ratio on a plain common short, read `n/a` with the reason in the cell, and they are not what "any row below cleared" counts.

## The two loss limits, worked

A limit stated **inside** a stress scenario and an absolute cap that must hold **beyond** it are different constraints. They give different sizes, and on a short they sometimes give different answers about whether the position exists.

Take a short entered at 50, a bear-case target of 30, a stated stress case of an adverse move to 70, and a reader whose loss budget for one idea is 150bp.

**Reading one, a limit inside the stress scenario.** Size so that the position has spent the budget by the time the stress case has played out. The adverse move to 70 is 40%, so the exposure is 150bp / 0.40, which is 375bp. At 70 the position is down 150bp, and the limit is satisfied on its own terms.

**Reading two, an absolute cap.** The same 375bp of exposure loses 375bp at 100 and 750bp at 150. Nothing in the structure stops the loss, because a short has no upper bound, so no unhedged size satisfies a cap that must hold in every state. The resolution is a priced defined-loss structure: the short paired with a long call, or a put spread instead of the short. Now the maximum loss is known at entry, being the premium plus the distance to the cap, and it can be compared with the 150bp budget. Where the priced structure costs more than the budget allows for the payoff it buys, the honest output is no position, and saying so is a finished answer rather than a failure to deliver.

**Where the mandate does not settle which reading applies**, show both branches rather than picking one: "under a limit inside the stress case, 375bp of exposure; under an absolute cap, no unhedged size qualifies and the defined-loss alternative prices at X for a payoff of Y." Naming the binding constraint is the point, so the reader can re-derive the size when one input moves.

## Stressed liquidity

Normal participation and stressed participation are separate lines in the pitch, never one blended figure, because the exit that matters happens in the case where everyone wants the same exit.

**Days to exit** is the position divided by the product of participation and average daily traded volume. A 500,000-share position against 2m shares of average volume at 20% participation exits in about 1.3 days. In the bear case, take volume down to what the name traded in its last drawdown and take participation down for crowding: at 60% of normal volume and 10% participation, the same position needs about 4.2 days. Where that runs past the catalyst window or past the horizon, the liquidity row of the gate reads `not cleared` and the size comes down until it does not.

**Gap risk moves the exit after the move.** Where the downside case is a dated release rather than a drift, the stop does not fill at the stop. Continuing the example above: a stop at 70 on a post-release print of 84 exits at 84, so the 375bp position loses 68% of its exposure, which is 255bp against a 150bp budget. The scenario limit was satisfied on paper and breached in fact. For any position whose downside arrives on a date, price the exit at the plausible gap rather than at the stop, and let that number set the size.
