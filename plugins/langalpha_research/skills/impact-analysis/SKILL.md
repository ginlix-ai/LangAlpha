---
name: impact-analysis
description: "Translate a macro, policy, rate, commodity, geopolitical or industry shock into equity exposure through a named transmission channel. Triggers on what does X mean for, impact of, exposure to, who benefits from, tariff, rate cut, oil shock, new regulation."
---

# Impact Analysis

One object carries this skill: the **transmission chain**, written as event, channel, driver, line item, action. A shock reaches a share price only by reaching a named landing, a financial line item or the valuation input a discount rate moves directly, so a claim that cannot name where it lands is a mood about a theme and does not ship. This is the work between `.agents/skills/morning-note/SKILL.md`, which says something happened, and the models, which say what a company is worth.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

**The exposure map is the default output.** Absent a position list the user supplied, the report names candidates in research-queue language and says in one sentence what would convert the screen into an action. Every name in it is a research candidate until the user says otherwise.

## Step 1: Frame the shock and freeze the as-of

Write the event in one sentence carrying four things: what changed, who decided it, the date it takes effect, and what was expected before it. The last one is the whole analysis in miniature: a fully anticipated shock reaches the line item and never reaches the price.

**Timing bucket.** Every claim in the report carries one of four, and the headline claim carries its bucket in the first two sentences:

| Bucket | Window | What lands here |
|---|---|---|
| `immediate` | days | the multiple, sentiment, anything that reprices before a number moves |
| `this quarter` | the current reporting period | a line item that moves before the next print |
| `this year` | the next four quarters | contract resets, hedge roll-offs, inventory turning over |
| `structural` | beyond four quarters | capacity siting, supply-chain relocation, a changed cost curve |

**Freeze the as-of.** Record one moment for the whole set of time-sensitive inputs (prices, consensus, the macro series, the policy text as published) and state it at the top. An input with no as-of is a gap rather than an estimate.

**Read the mandate before asking anything.** Sectors and geographies in scope, horizon, benchmark and delivery preferences come from user-tier memory per `.agents/skills/research-conventions/references/intake.md`; ask only about the forks it leaves open, at most three questions, and take the recommended option when the user skips.

**Depth.** Default `working analysis`. A single "what does this mean for X" is a `first pass`. Bands and cut order: `.agents/skills/research-conventions/references/depth.md`.

**Done when** the event sentence carries its effective date and what was expected before it, the headline claim carries one timing bucket, the freeze-time as-of is written down, and the output mode is stated as an exposure map or as a named list the user supplied.

## Step 2: Name the channel or hold the claim

Every impact is one written chain with five links:

**Event** → **channel** (the mechanism that carries it) → **driver** (the operating quantity that moves: units, price per unit, input cost per unit, the rate paid, the days of working capital) → **line item** → **action**.

The landing comes from a closed set: revenue (net interest income for a bank or a lender), gross margin, opex, interest, tax, capex, working capital, share count, and the valuation input itself (the discount rate, and the multiple that carries it) for a shock that reaches the price through no operating line at all. Which of them a given channel reaches first is governed by `.agents/skills/impact-analysis/references/channels.md`.

Worked shape: a 100bp policy-rate cut runs through the rates channel, lowers the coupon on the floating-rate half of the debt, lands in interest expense from the next reset date, and the action is to watch interest expense in the first print after that reset.

**A claim with no channel goes back to source work.** The next move is a search, not a softer sentence: the primary text of the rule or release, the company's segment and geographic disclosure, and the input-cost or hedging note. Where the chain still will not close, the item ships as an observation with no implication attached, and the report says what would close it.

The event runs through a channel not already mapped, or the first affected line item is not obvious from the disclosure: read `.agents/skills/impact-analysis/references/channels.md`, which carries the standing channels with the line items each reaches first, its typical lag and the tell that it is happening.

**Done when** every impact claim in the draft is one chain with all five links written out, and every claim that will not complete the chain is either removed or listed as source work naming the document that would complete it.

## Step 3: Separate the orders of effect

Three orders, reported in separate rows or paragraphs, at declining confidence, never blended into one conclusion.

| Order | What it must name | Confidence ceiling |
|---|---|---|
| direct | the channel and the line item on the company's own statements | as strong as the disclosure that sizes it |
| second | whose behaviour has to change in between (a third party, never the subject), and what they do differently | below the direct claim behind it, always |
| third | both intermediaries, and the price, policy or competitive position that shifted between them | directional only: a sign and a mechanism, never a magnitude |

A second-order claim inherits the uncertainty of the direct claim under it and adds a behavioural assumption, so a conclusion blending a direct and a third-order effect at one confidence reads as evidence while being arithmetic on a guess. Where a third-order effect is the whole reason a name is in the table, say so in the row.

**Done when** every claim carries its order, the orders sit in separate rows or paragraphs, and no sentence states a second or third-order claim in the language `.agents/skills/research-conventions/references/evidence.md` reserves for a sourced one.

## Step 4: Build the exposure table

**Find the names from disclosure rather than reputation:** a name enters the table because a disclosed figure puts it there.

- `screen_stocks` for the candidate universe by sector, size and geography.
- `get_sec_filing` for the segment note, the geographic revenue split, the input-cost and hedging notes, the debt maturity schedule, and the risk-factor section, which often names the exposure in the company's own words.
- `get_financial_statements`, `get_financial_ratios` and `get_growth_metrics` for the line item the channel lands on and its trailing sensitivity.
- `get_company_overview` for consensus and the estimate history, `get_daily_prices` for moves since the event, `get_historical_valuation` for the multiple against its own range.
- `get_economic_indicator`, `get_treasury_rates` and `get_market_risk_premium` for the macro series behind the shock; `get_economic_calendar` for the releases that will confirm or deny it.
- `WebSearch` and `WebFetch` for the primary text: the rule, the tariff schedule, the agency release, the company statement. The text of the instrument beats every summary of it.

| Name | Order | Channel | First line item | Direction | Magnitude | Timing | Confidence | Priced-in | Label | As-of |
|---|---|---|---|---|---|---|---|---|---|---|

Rank by sign first, then magnitude, timing, confidence, directness and priced-in status, so the most exposed and least priced names sit at the top.

**Magnitude comes from a disclosed figure with its arithmetic shown**, in the units the line item uses: basis points for a margin, percent or currency for a level, per `.agents/skills/research-conventions/references/market-data-rules.md`. Where the disclosure will not size it, write the bound instead of a point ("imported inputs are under 20% of cost of sales, so the gross-margin effect is at most 180bp") and name the disclosure that would size it.

**Row-merge discipline.** The rule for sharing a row is in `.agents/skills/research-conventions/references/judgment.md`. The case it protects here is one name reached by one shock through two channels with different signs and lags: that is two rows, never a merged one.

**Both signs, or the reason there is only one.** A shock with only losers usually means the search stopped at the obvious side: the domestic substitute, the toller who passes the cost through, the supplier of the workaround. A one-sided table carries the sentence explaining why the other side is empty.

**Done when** every row names its channel, first line item and direction; every magnitude is either sized with its arithmetic shown or written as a bound naming the disclosure that would size it; every row carries an evidence label and an as-of; both signs are populated or the one-sided reason is written; and no two rows were merged on theme alone.

## Step 5: Split priced-in from needs-proof

For each name in the top rows, pull three readings against the freeze-time as-of and one common base date set at the event:

- **The move since the event**, from `get_daily_prices`, with the same base and end dates for every name in the table.
- **The revision direction**, from the consensus and estimate history in `get_company_overview`, with its vintage stated.
- **The multiple against its own range**, from `get_historical_valuation`.

Then one of two labels per name, decided by the test in `.agents/skills/research-conventions/references/judgment.md` under "Priced in":

- **Priced in**: both the move and the revisions carry the channel's effect, and the row says which move and which revisions.
- **Needs proof**: anything short of both, with the print, filing or release that would put the claim in the numbers and its date.

**Attribute the move.** A move nobody can attribute is a question rather than a confirmation: attribute what the data supports and say what stays unattributed, keeping price as belief per `.agents/skills/research-conventions/references/judgment.md`.

**Done when** every top-ranked name carries its move with the common base date, its revision direction with the consensus vintage, and exactly one of the two labels, and every needs-proof label names a dated event that would settle it.

## Step 6: Answer the strongest counterargument

State the best case against the conclusion as someone who holds it would state it, then answer it with exactly one of the four verdicts in `.agents/skills/research-conventions/references/judgment.md`: **weaker** (the channel is real and smaller than it looks), **later** (the lag pushes it past the horizon the report is written for), **already priced** (step 5 found it in the revisions), or **it wins**, in which case the conclusion changes to match and the report says the counterargument moved it. A counterargument dismissed in a clause was not the strongest one available.

Five mechanisms defeat impact claims often enough to test by name:

1. **Pass-through.** Who actually absorbs the cost. A supplier with pricing power moves the effect to its customer, and the name in the table is then the wrong one.
2. **Substitution and re-routing.** The input, the route or the jurisdiction changes, and the exposure the filing describes stops being the one the company runs.
3. **Contracts and hedges.** A hedge book or a fixed-price contract can delay the whole effect past the horizon, which converts a magnitude claim into a timing claim.
4. **Reversal and carve-outs.** Exemptions, transition provisions, phase-ins and litigation move the effective date that step 1 recorded.
5. **Offsetting exposure inside one name.** The same shock reaching a company through two channels with opposite signs nets to less than either row implies.

**Done when** the counterargument names a mechanism rather than a mood, the answer is one of the four verdicts with the evidence behind it and the conclusion has changed wherever the verdict was that it wins, and each of the five mechanisms is either tested or listed as open with what would test it.

## Step 7: Deliver

Sections in order: the answer in the first two sentences; the event, its effective date and the freeze-time as-of; posture and depth in one sentence; the transmission map; orders of effect; the exposure table; priced-in against needs-proof; the counterargument; monitoring; missing evidence.

**Format.** A short ask is answered inline at `first pass` depth. A document is built through `.agents/skills/docx/SKILL.md` when the user wants one or when the work runs to `working analysis` or deeper. Save to `$WORK_DIR/work/{task}/`.

**Posture.** One readiness posture, near the top, from the table in `.agents/skills/research-conventions/SKILL.md`. Below `decision-grade` it names the input responsible, which for this skill is usually the disclosure that would size a magnitude or the effective date the rule text has not fixed yet.

**Monitoring table**, one row per item, all five fields present:

| Metric | Threshold | Source | Date or window | Action each side |
|---|---|---|---|---|

The metric is the one the channel moves first, so the row is the channel's tell from Step 2 turned into a dated observation. The bar for a monitored item is set in `.agents/skills/research-conventions/references/judgment.md`.

**Missing evidence**, present even when empty: each missing input, the tool or document that supplies it, and the conclusion it blocks.

**Action.** One verb from the closed vocabulary, gated by the inputs in hand. With an exposure map and no holdings, the honest verbs are `watchlist`, `wait for proof` and `re-underwrite`, each naming its trigger level or dated proof point. This skill produces analysis and stops short of personalised investment advice and execution instructions.

**Done when** the answer sits in the first two sentences, posture and depth travel together in one sentence near the top, every monitoring row carries all five fields, the missing-evidence block is present, every action verb has its inputs in hand, and the artifact sits inside its depth band.
