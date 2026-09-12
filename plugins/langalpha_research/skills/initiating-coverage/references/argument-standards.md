# Argument Standards

Deferred reference for `initiating-coverage`. Load it in Task 1 when writing the thesis, the debates or the risks, and again in Task 5 when those sections go into the report.

Four structures carry the argument: pillars, debates, risks and catalysts. Each has a required shape, because each fails the same way when it is left loose, by reading as a view while committing to nothing.

## Thesis pillars

Three to five, no more. A pillar is a claim about the company that, if true, produces a specific financial outcome. Each carries four parts, and a pillar missing any one of them is a slogan:

1. **Evidence.** What is observed today, with its source and its evidence label.
2. **Consequence.** What it does to a named model line or to the valuation, quantified.
3. **Counterargument.** The strongest case against it, put as someone who believes it would put it, then answered.
4. **Monitored signal.** The metric, the threshold, the source that will show it, the window when it becomes observable, and the action each side of the threshold triggers, in the table shape set by the monitored-item standard in `.agents/skills/research-conventions/references/judgment.md`.

**Weak, and common:**

> *Pillar 2: Strong market position.* The company is a leader in its category with a differentiated product and a loyal customer base, which should support continued growth and margin expansion.

Nothing in it can be wrong. There is no number, no line item, no observation that would retire it.

**Strong:**

> *Pillar 2: Switching costs are showing up in price, not just retention.* Net revenue retention has held above 118% for six quarters while list prices rose 7% annually (10-K FY2025, page 42; quarterly supplements). We model 4% annual realised price through FY2028, worth roughly 240bp of the 380bp operating-margin expansion in our base case. **Against:** the retention figure includes seat expansion at existing accounts, so it may be measuring customer growth rather than pricing power; the FY2026 cohort disclosure would separate them. **Signal:** realised price per seat in the Q2 FY2027 supplement, due August 2027. Below 3% annual, the margin path loses about 150bp and pillar 2 is retired.

## Key debates

The debates table is where the report earns its readership: it shows what is genuinely contested rather than asserting a conclusion into silence. Three to five rows.

| Debate | Consensus view | Our view | Evidence | Why the market may be wrong | Next event that resolves it | What would change our mind |
|---|---|---|---|---|---|---|

The last column is the one that gets dropped and the one that matters. It names an observation, a threshold and a date, not a sentiment. "If the debate resolves against us" is not an entry; "if gross retention prints below 108% in the Q3 release on 2026-11-04" is.

A debate with no genuine disagreement behind it is a summary, not a debate. When the market is not actually split on a point, cut the row rather than manufacturing a bear who does not exist.

## Risks

Each risk carries five fields. The last two are what turn a risk section into something a reader uses.

| Risk | Why it matters to this thesis | Leading indicator | Downside impact | Mitigation or monitoring plan |
|---|---|---|---|---|

- **Why it matters to this thesis** names the pillar it attacks, so a reader can see which part of the argument fails.
- **Downside impact** is mechanical: through which line item, to what number, in what scenario. A narrative risk with no number attached is not a downside case.
- Every mitigation ships with at least one scenario in which the mitigation itself does not work, per `.agents/skills/research-conventions/references/judgment.md`.

Cut any risk that would appear unchanged in a report on any company in the sector. Regulation, competition and macro are risk categories, not risks: the risk is the specific rule, the specific competitor move, or the specific macro sensitivity, with the exposure quantified.

## Catalysts

Keep six groups distinct, because they carry different confidence and different tradability:

1. **Hard-dated events**: a filing, a hearing, a decision, a contract expiry, with the date and its confidence.
2. **Recurring events**: earnings, investor days, monthly or quarterly operating data.
3. **Soft catalysts**: expected but undated developments.
4. **Thesis milestones**: the operational proof points a pillar needs.
5. **Regulatory and legal**: dockets, rulings, approvals.
6. **Estimate-revision catalysts**: what makes the street move its numbers, and in which direction.

A date that came from an expectation rather than a document is labelled estimated, never printed as scheduled.

## Language

The report is written in claims that could be wrong. Paired examples, weak first:

| Weak | Strong |
|---|---|
| "well positioned for growth" | "we model 12% revenue growth through FY2028 against consensus at 9%, on the backlog conversion schedule in the FY2025 10-K" |
| "management is executing well" | "the three targets set at the 2024 investor day were met early; the fourth, on segment margin, is 180bp behind" |
| "valuation looks attractive" | "at 11x NTM EBITDA against a five-year median of 14x, the market is pricing the margin decline as permanent" |
| "risks include competition" | "a second entrant at current pricing takes roughly 300bp of gross margin, which retires pillar 3" |
| "we monitor the situation" | "gross retention below 108% in the Q3 release on 2026-11-04 retires pillar 2" |

Section headings follow the same rule: they state the finding, not the topic. "Contract structure caps downside through 2029" beats "Contract overview". A heading that would sit unchanged above another company's section is telling the reader nothing.
