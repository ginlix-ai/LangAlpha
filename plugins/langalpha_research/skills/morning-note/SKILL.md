---
name: morning-note
description: "Daily research briefing on overnight news, pre-market movers, earnings and macro events. Triggers on morning note, morning meeting, what happened overnight, morning call prep, daily note, trade idea for the open."
---

# Morning Note

One page, read in two minutes, with a view in it. A note that summarises the tape without saying what to think about it wastes the only two minutes the desk will give it.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## Workflow

### 1. Gather overnight developments, labelled as they land

**Earnings and guidance**
- `get_earnings_calendar(from_date, to_date)` on the macro MCP server for who reported and who reports next.
- `get_company_overview` for consensus, the estimate history and the rating distribution.
- Surprises against consensus on revenue, EPS and the metric the name actually trades on.
- Guidance raised, lowered or maintained, and against what the buy side was carrying.

**News and events**
- `WebSearch` and `WebFetch` for overnight headlines, deals, management changes, product and regulatory decisions, and rating changes.

**Market context**
- `get_market_overview` for index and sector performance, `get_daily_prices` for the overnight and pre-market moves, `get_economic_calendar` for what is still ahead, plus the commodity or currency move that matters to the coverage.

Label each item as it is collected rather than at write-up. A filing and a closed-period data print are `fact`. A company statement about itself is `company claim`, with the venue. A consensus figure is a `street estimate`, with its vintage. A report with no primary document behind it carries the outlet, the time, and whether the date is confirmed, scheduled, estimated or reported. A rumour reaches the note as a rumour, naming who reported it and what would confirm it, or it does not reach the note. The closed label set: `.agents/skills/research-conventions/references/evidence.md`.

### 2. Decide what is already priced

Every item that moved something splits one of two ways, and the split is the value the note adds over a headline feed. Apply the test in `.agents/skills/research-conventions/references/judgment.md` under "Priced in", which needs both the move and the revisions to carry the effect:

- **Priced in**: both halves hold; the note says which move and which revisions carry it.
- **Needs proof**: anything short of both, and the note names the print or the date that would put the claim in the numbers.

**Name the channel or hold the claim.** An impact claim runs from the event, through a named transmission channel, to a specific line item. "Tariffs are a headwind for the group" names no channel and does not ship. "A tariff on imported cells lands in module cost of goods sold, and the two names importing most of their supply carry it in gross margin from the September quarter" does. When the channel cannot be named, the item goes in as an observation without an implication, and the next step is source work.

### 3. Write the note

Two stamps at the top, because they are different facts: the note carries the time it was written, and every figure carries the as-of of the data itself. A note written pre-market is a pre-market note all day; a quote inside it is stale by the open unless it says what it is as of. The always-timestamped list is in `evidence.md`.

---

**[Date] Morning Note, [Analyst]**
**[Sector coverage] | Written [time]. Prices as of [time], [session].**

**Top Call: [the one thing the desk needs to hear]**
- Two or three sentences on the development and why it matters.
- Stock impact: rating and target reiterated or changed.

**Overnight and Pre-Market**
- [Company A]: what happened, with its source. *Our take*: the view.
- [Company B]: what happened, with its source. *Our take*: the view.
- [Sector or macro]: the development, and which names it reaches.

**Priced In / Needs Proof**
- Priced in: [name], [what the move and the revisions already reflect].
- Needs proof: [name], [the claim], [the print or date that would confirm it].

**Today**
- [Time]: [Company] earnings call, [what to listen for].
- [Time]: [data release], consensus against our read.
- [Time]: conference or investor day.

**Trade Ideas** (when there is one)
- [Long/Short] [Company]: thesis in two sentences, plus the catalyst and its date.
- Risk: the observation that would make this wrong.

---

**Fact and view stay in separate sentences.** What happened carries its source; the take carries the view and is marked as one. This is the deliverable most likely to be forwarded, and the sentence quoted back has to be one that can be defended. A view welded into the same sentence as a fact reads as a fact to everyone downstream.

### 4. Quick take on a coverage print

| Metric | Consensus | Actual | Beat/Miss |
|---|---|---|---|
| Revenue | | | |
| EPS | | | |
| [The metric the name trades on] | | | |
| Guidance | | | |

**Our take**: two or three sentences on whether this is good or bad for the stock, and whether it changes the thesis rather than the quarter.

**Action**: the verb from the gated vocabulary in `.agents/skills/research-conventions/references/judgment.md`, with a rating word (maintain, upgrade, downgrade) beside it where a published rating exists, and what would change it.

### 5. Deliver

Save to `$WORK_DIR/work/{task}/morning_note_YYYY-MM-DD.md`, as markdown for email or chat distribution. The note carries one posture from the ladder in `.agents/skills/research-conventions/SKILL.md`, stated once beside the stamps at the top.

**One page is the bar.** Over it, cut in this order: the market recap the reader already saw on a screen, the second and third bullets on one name, the calendar entries with no view attached, and any trade idea with no dated catalyst. Depth bands and the general cut order: `.agents/skills/research-conventions/references/depth.md`.

## Important notes

- Be opinionated. A note that summarises without a view is a worse version of a headline feed.
- Lead with the most important thing. The headline goes first, not third.
- "Nothing material overnight, maintaining positioning" is a complete morning note, and a better one than three paragraphs of filler.
- Separate the actionable (a print, a deal, a guidance change) from the noise (a small rating change, a non-event), and say which is which.
- When yesterday's call was wrong, say so in today's note and say what you missed. Credibility outlasts any single call.
