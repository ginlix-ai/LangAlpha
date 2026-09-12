---
name: company-profile
description: "One-page company profile slide, four quadrants of overview, financial summary, share price chart and key facts. Also the compact profile paragraph when it has to sit inside a memo, a deck or a chat reply. Triggers on one-page profile, tear sheet, company snapshot, profile slide, quick profile of [company]."
---

# Company Profile

A single slide that answers "what is this company" for a reader with thirty seconds. Four quadrants, all four full: overview, financial summary, share price or valuation chart, key facts. This is the front page of a pitch book and the first page of a diligence pack, and it is the one slide in a deck allowed to be a document, because nobody presents from it, they read it off the page.

Build it with the `pptx` skill. Its geometry, palette, build-script discipline and verification scripts all apply unchanged, and this file only describes what goes inside that frame. Reach for `html-report` instead when the answer wants two pages of prose, and `comps-analysis` when the question is how the company prices against its peers rather than what it is.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first deliverable.

## Scope

The profile is a **baseline**: what is true about this company, sourced, on one page. It is not a recommendation, and it holds no view. The moment the work turns into a thesis, a scenario set, a valuation argument or a call on the stock, it has stopped being a profile: finish the baseline, then hand off and say so. `.agents/skills/initiating-coverage/SKILL.md` owns the thesis, `.agents/skills/comps-analysis/SKILL.md` owns relative valuation, `.agents/skills/earnings-preview/SKILL.md` owns the setup into a print. Expanding the slide to carry a view is the failure this rule exists to catch, because a one-pager that argues reads as fact to everyone downstream of it.

## Workflow

### 1. Scope it before researching

Confirm the entity first, then ask what the slide is for.

**Entity identity.** Establish, before any pull: the exact issuer (a similarly named parent, subsidiary or unrelated company is the classic wrong answer), the listing line (ticker and exchange, and which share class where more than one trades), and the fiscal year end. Everything downstream carries this, and a profile of the wrong listing line is wrong in every quadrant at once.

Then run intake per `.agents/skills/research-conventions/references/intake.md`: read the mandate memory, ask only about the forks it leaves open, recommended option first, and start on the recommendation when no answer comes back, disclosing it as a default. Two forks are worth a question here:

- **Format.** One slide, recommended, or a profile deck: this slide plus two or three elaboration slides.
- **Baseline**, which decides what survives the character budget below: long-only (durability, capital allocation), hedge-fund (positioning, float, borrow, days to exit), coverage starter (what a new analyst needs first), index constituent (weight, passive ownership, flow), diligence (counterparty, contracts, liquidity). Offer the two the mandate makes plausible rather than the whole menu, and recommend the one it implies.

Whether the company is public or private is settled from the pull rather than asked: a private company has no share price, so quadrant 4 becomes holders, funding history and recent developments.

Asking after the research is doing the research twice.

### 2. Pull the data

| Need | Source |
|---|---|
| Profile, sector, market cap, consensus estimates, price targets, earnings history | `get_company_overview` |
| Revenue, EBITDA, margins, EPS, FCF across three years plus the forward year | `get_financial_statements` and `get_growth_metrics` from the fundamentals MCP server |
| EV, EV/Revenue, EV/EBITDA, P/E | `get_historical_valuation`, `get_financial_ratios` |
| Share count, float, insider and institutional holdings | `get_shares_float` |
| Named officers | `get_key_executives` |
| One year of daily closes for the price chart | `get_daily_prices` |
| Business description, segment and geographic mix | `get_sec_filing` (10-K, Item 1 Business and MD&A) |
| Recent news, private-company facts, brand colour | `WebSearch` and `WebFetch` |

Normalise before writing anything down: one currency, one scale ($mm or $bn, never mixed on one slide), one fiscal-year convention, and every figure labelled `A` for actual or `E` for estimate. Mark unaudited, preliminary, pro-forma, adjusted and company-defined figures as such in the bullet itself, since a pro-forma revenue figure sitting unlabelled beside a reported one is two different metrics in one column.

**Provenance travels with every metric**, even though the slide shows only the number. Keep period, units, source and confidence beside each figure while researching: the slide prints the number, the source line prints the tools and documents behind it, and the outline you show the user carries all four so a wrong figure is traceable in seconds rather than re-pulled.

**Flag a conflict rather than picking silently.** Two sources disagreeing on revenue, EBITDA, debt, share count or market cap; a data tool differing from the filing; the company's deck differing from its filing; a newer filing superseding the figure already written down. Resolve down the tier list and record the selection per `.agents/skills/research-conventions/references/evidence.md`, and where the conflict is material to the slide, say which figure was used in the source line.

### 3. Outline before code

Write the four quadrants out as plain text with the real numbers already in place. No placeholders: a bullet you cannot fill is a bullet the slide does not get. Show the outline and the accent colour, then build.

### 4. Build, render, look, shrink

Follow the `pptx` loop exactly:

1. Write `work/<task>/build_<name>.js` and run it with `NODE_PATH=$(npm root -g) node work/<task>/build_<name>.js`.
2. `python .agents/skills/pptx/scripts/check.py work/<task>/<name>.pptx --strict`, which reads the written file and reports anything off the slide, overlapping or overflowing.
3. `python .agents/skills/pptx/scripts/render.py work/<task>/<name>.pptx --montage`, then open the PNG and read it. Look for a bullet wrapping into the quadrant below it, a table row crossing the footer, an axis label clipped at the bottom, and a title sitting on top of the first quadrant header.
4. When anything overflows, fix it in this order: drop the body font by 1 to 2pt (12 to 11, then 11 to 10, which is the floor), then shorten the bullet, then move the boundary between the two rows. Re-render and look again.

Steps 3 and 4 are not optional and neither is the looking. `check.py` estimates overflow from font size and character count, so a pass is a reason to look, not a substitute for looking. A profile slide is the densest thing this repo produces, and density is exactly where the estimate and the renderer disagree.

## Layout

16:9, `LAYOUT_WIDE`, 13.333 x 7.5 in, with the 0.6 in side margins and the title band from the `pptx` skill left alone. The content band, y 1.75 to 6.60, splits into four equal quadrants:

```
x=0.600                          x=6.833                       x=12.733
+--------------------------------+------------------------------+  y=1.75
| 1  Company Overview            | 2  Business and Positioning  |
| w=5.90  h=2.30                 | w=5.90  h=2.30               |
+--------------------------------+------------------------------+  y=4.05
| 3  Financial Summary           | 4  Share Price / Key Facts   |
| w=5.90  h=2.35                 | w=5.90  h=2.35               |
+--------------------------------+------------------------------+  y=6.60
```

| Element | x | y | w | h | Type |
|---|---|---|---|---|---|
| Title, `Company Name (TICKER)` | 0.60 | 0.45 | 12.13 | 0.60 | 28pt bold |
| Kicker: units, periods, as-of date | 0.60 | 1.05 | 12.13 | 0.35 | 14pt |
| Rule | 0.60 | 1.42 | 12.13 | 0.02 | |
| Quadrant header, top row | col x | 1.75 | 5.90 | 0.40 | 16pt bold |
| Quadrant hairline, top row | col x | 2.17 | 5.90 | 0.01 | |
| Quadrant body, top row | col x | 2.23 | 5.90 | 1.82 | 12pt |
| Quadrant header, bottom row | col x | 4.25 | 5.90 | 0.40 | 16pt bold |
| Quadrant hairline, bottom row | col x | 4.67 | 5.90 | 0.01 | |
| Quadrant body, bottom row | col x | 4.73 | 5.90 | 1.87 | 12pt |
| Source line | 0.60 | 6.95 | 12.13 | 0.30 | 10pt |

Column x is 0.600 for quadrants 1 and 3, 6.833 for quadrants 2 and 4. The 0.333 in gutter is the only thing between the columns: white background, no fills, no shading, no boxes drawn around the quadrants.

Header boxes are 0.40 in and not a hair less. `check.py` measures a box against its text after taking off the 0.05 in inset top and bottom, so a 16pt line needs 19.2pt of clear height, and a 0.32 in box fails the overflow check even though the render looks fine.

12pt body is a deliberate exception to the deck-wide 16 to 18pt in `pptx`, and it is the only slide that gets it. `check.py` holds the hard floors either way: 10pt for body text, 14pt for table text.

## Density budget

A quadrant that looks sparse has not been researched. Fill all four, then measure each against the column width.

| Quadrant | Content | Target |
|---|---|---|
| 1 Company Overview | HQ, founded, employees, CEO and CFO, exchange and ticker, market cap, industry, one defining statistic | 6 to 8 bullets |
| 2 Business and Positioning | revenue drivers, product or segment mix with percentages, market share, the moat in one clause, customer count or concentration, geographic mix | 6 to 8 bullets |
| 3 Financial Summary | Revenue, growth, EBITDA, EBITDA margin, EPS, FCF, EV/EBITDA over three columns (FY-1A, FY0A, FY+1E) | a table of header plus 6 rows, **or** a chart, never both |
| 4 Share Price or Key Facts | one year of daily closes; for a private company, top holders with percentages, funding history, recent developments | a chart, or 5 to 7 bullets |

At 12pt across a 5.90 in column, `check.py` fits roughly 70 characters on a line (`width_in * 72 / (pt * 0.5)`), so **a bullet over 70 characters takes two lines and costs the quadrant a fact.** Counting paragraph spacing, each body box holds 7 to 8 one-line bullets. Write to that budget: pack facts, never pad them.

- Combine related facts: `HQ Austin, TX; founded 2003; 14,200 employees` is one bullet, not three.
- Always carry the number: `$4.0bn revenue` beats `large revenue`, and `+28% y/y` beats `growing fast`.
- Add the comparison that makes the number mean something: `EBITDA margin 25.4% (peer median 18%)`.
- Bold the lead term as its own run, `{ text: "Market position: ", options: { bold: true } }`, so the quadrant scans as a list of labels.
- Set `bullet: { indent: 12 }`. The pptxgenjs default leaves about a third of an inch of white between the glyph and the text, which is four characters of a 70-character line spent on nothing.

**Never silently omitted.** These are either on the slide with their as-of, or named in the evidence gaps: market cap, float, average daily volume and days to exit, index membership, passive and ETF ownership, top holders, short interest, borrow, analyst coverage count, and the consensus setup. A missing one of these changes what a reader can do with the page, so its absence is information.

**Pair the fact with the decision** wherever the decision is not obvious from the number: float speaks to capacity and squeeze risk, days to exit answers whether a position can be built or unwound at all, borrow prices the other side of the trade. `Float 62%; 14 days to exit at 20% ADV` is one bullet doing two jobs, which is how a data table becomes a decision aid inside a 70-character line.

**Evidence gaps.** One compact block, never a table of empty fields. Where there are one or two gaps, they ride at the end of the source line: `Not sourced: short interest, borrow (as of unavailable).` Where there are more, they take the last bullet of quadrant 4 as a single line starting `Not sourced:`. Each gap names what is missing and, where it is not obvious, what would supply it. A gap that costs the reader a decision is worth a bullet more than the least surprising fact on the slide.

If a quadrant still runs short, the facts usually missing are segment percentages, customer concentration, guidance against consensus, and insider ownership. If it runs long, cut the least surprising fact before you cut the font.

## Content rules

**Every adjective carries its number.** `Best-in-class margins` becomes `EBITDA margin 25.4% (peer median 18%)`; `dominant` becomes `41% share, next largest 17%`; `high quality` becomes the metric that makes it so. Where the number does not exist, the claim does not go on the slide.

**Title the valuation block by what the evidence supports.** With only historical financials and derived LTM multiples behind it, the heading is `Valuation Context` or `LTM Multiples`, and it stays that way until forward estimates, peer evidence, target-price evidence or an explicit statement of market expectations is actually sourced. A heading promising a forward debate the sourcing cannot carry is the one place a factual page can mislead without printing a single wrong number.

**Contain a live event.** A pending transaction, a rumour, an activist position or a regulatory action that is material but is not what the user asked about goes in exactly three places: one line in the read, the catalyst or risk bullet, and the evidence gap for whatever primary document is still missing. It does not get threaded through the business description, the financial summary and the valuation block as well, which is how one unresolved event takes over a page that was asked to describe a company.

**Reader-facing labels.** The slide prints the reader's word for each evidence label, one per label, per **Reader-facing labels** in `.agents/skills/research-conventions/references/evidence.md`, where a `model-derived` figure prints as `derived` in the space a slide has.

## Build script

```js
const PptxGenJS = require("pptxgenjs");

const INK = "1A1A1A", MUTED = "5A5A5A", RULE = "D8D5D0", ACCENT = "1F4E79";
const FONT = "Arial", M = 0.6, W = 13.333, CONTENT_W = W - 2 * M;
const COL_W = 5.9, COL_X = [M, 6.833];
const ROW = [{ head: 1.75, rule: 2.17, body: 2.23, h: 1.82 },
             { head: 4.25, rule: 4.67, body: 4.73, h: 1.87 }];

const pptx = new PptxGenJS();
pptx.layout = "LAYOUT_WIDE";
const slide = pptx.addSlide();
slide.background = { color: "FFFFFF" };

slide.addText("Acme Corporation (ACME)", { x: M, y: 0.45, w: CONTENT_W, h: 0.6,
  fontFace: FONT, fontSize: 28, bold: true, color: INK, valign: "middle" });
slide.addText("$ in millions unless noted; FY2026E is consensus; prices as of 2026-09-05",
  { x: M, y: 1.05, w: CONTENT_W, h: 0.35, fontFace: FONT, fontSize: 14, color: MUTED, valign: "top" });
slide.addShape("rect", { x: M, y: 1.42, w: CONTENT_W, h: 0.02, fill: { color: RULE } });

// One helper for all four quadrants, so no header can drift from its neighbour.
function quadrant(col, row, heading) {
  const x = COL_X[col], r = ROW[row];
  slide.addText(heading, { x, y: r.head, w: COL_W, h: 0.40,
    fontFace: FONT, fontSize: 16, bold: true, color: ACCENT, valign: "middle" });
  slide.addShape("rect", { x, y: r.rule, w: COL_W, h: 0.01, fill: { color: RULE } });
  return { x, y: r.body, w: COL_W, h: r.h };
}

const bullets = (lines) => lines.map((t) => ({ text: t, options: { bullet: { indent: 12 } } }));

const q1 = quadrant(0, 0, "Company Overview");
slide.addText(bullets([
  "HQ Austin, TX; founded 2003; 14,200 employees",
  "CEO J. Rivera (2019); CFO M. Osei (2022)",
  "NASDAQ: ACME; market cap $18.4bn; float 92%",
]), { ...q1, fontFace: FONT, fontSize: 12, color: INK, valign: "top", paraSpaceAfter: 3 });

const q3 = quadrant(0, 1, "Financial Summary");
const head = { fill: { color: ACCENT }, color: "FFFFFF", bold: true };
slide.addTable(
  [[{ text: "$mm", options: head }, { text: "FY2024A", options: head },
    { text: "FY2025A", options: head }, { text: "FY2026E", options: head }],
   ["Revenue", "3,410", "4,020", "4,610"],
   ["EBITDA", "742", "928", "1,105"]],
  { ...q3, colW: [2.0, 1.3, 1.3, 1.3], fontFace: FONT, fontSize: 14, color: INK,
    rowH: 0.26, valign: "middle", border: { type: "solid", pt: 1, color: RULE } }
);

const q4 = quadrant(1, 1, "Share Price, Last 12 Months");
slide.addChart(pptx.ChartType.line, [{ name: "ACME", labels: months, values: closes }],
  { ...q4, chartColors: [ACCENT], showLegend: false, lineSmooth: false, lineDataSymbol: "none",
    valAxisMinVal: 120, valAxisMaxVal: 180,   // never zero-based; see below
    catAxisLabelFontFace: FONT, catAxisLabelFontSize: 12, catAxisLabelColor: MUTED,
    valAxisLabelFontFace: FONT, valAxisLabelFontSize: 12, valAxisLabelColor: MUTED,
    valGridLine: { color: RULE, style: "solid", size: 1 }, catGridLine: { style: "none" } }
);

slide.addText("Source: company filings, fundamentals MCP server, get_daily_prices. Prices as of 2026-09-05.",
  { x: M, y: 6.95, w: CONTENT_W, h: 0.3, fontFace: FONT, fontSize: 10, color: MUTED });

pptx.writeFile({ fileName: "work/acme/acme_profile.pptx" })
  .then(() => console.log("written"));
```

A table is as tall as `rowH` times its row count whatever `h` says, so header plus 6 rows at 0.26 is 1.82 in inside the 1.87 in body box. A seventh row pushes it past the content band toward the footer, which `check.py` reports as out of bounds.

## Chart or table, never both

Quadrant 3 is a table by default: it carries nine numbers in the space a chart spends on one series. Swap in a chart only when the shape of the trend is the point, and then drop the table rather than shrinking both.

| Data | Chart |
|---|---|
| One year of daily closes | line, unsmoothed, no markers |
| Revenue or EBITDA over five periods | column |
| Segment or geographic mix | horizontal bar, sorted |
| Product mix, four slices or fewer | pie with percentages shown |

Charts are native `addChart` parts, one colour per series, axis labels at 12pt. Units go in the kicker or the axis title, not on every data label.

**A price line is never zero-based.** pptxgenjs starts the value axis at 0 by default, which pins a year of trading into the top fifth of the box and draws every stock as a flat line. Set `valAxisMinVal` and `valAxisMaxVal` around the series, roughly 10 percent outside the low and the high. This is the defect the render catches and `check.py` never will: the geometry is perfect and the chart says nothing.

## Colour

The house palette in `pptx` is the default and it is enough: ink, grey, paper, one accent. When the user asks for the company's brand colour, search for the actual hex rather than guessing, use it as the single accent (quadrant headers, table header fill, the price line) and leave the rest in the house greys. One accent, one meaning, and green and red still mean gain and loss and nothing else.

## Private companies

Quadrant 4 has no share price, so it becomes holders and history: top five holders with percentages, funding rounds with dates and amounts, and the two or three developments from the last ninety days a reader would ask about. The sources shift to the corporate site, press releases and news search, so the source line names them and the kicker says which figures are estimates.

## Compact profile

When the profile has to sit inside a memo, a deck or a chat message rather than on its own page, compress it to one paragraph in a fixed order: what the company does and how it makes money, scale (revenue, growth, margin) with the period, the balance-sheet and positioning facts that bear on the reader's decision, the valuation context with its as-of, and the one thing that would change the picture. Two hundred words or fewer, every number carrying its period and source, and the same never-silently-omit list applies: what is not sourced is named in the last sentence.

## Profile decks

When the user asked for two or three more slides, the profile slide does not change. The rest are ordinary `pptx` content slides at 16 to 18pt body, one claim each, in the order business and market, financial detail, then leadership or ownership. Every one elaborates something the profile slide already claimed, so a reader who stops after the first page has still been told the truth.

## Checklist

- [ ] Scope settled before research started, from the mandate memory or from intake, with any default taken named in the delivery message.
- [ ] All four quadrants at their target density, none visibly lighter than its neighbour.
- [ ] Every bullet under 70 characters and on one line in the render.
- [ ] Quadrant 3 is a table or a chart, not both.
- [ ] One currency and one scale throughout, every figure labelled A or E.
- [ ] `check.py --strict` passes.
- [ ] The montage was rendered and looked at, not just generated.
- [ ] Nothing under 10pt, table text at 14pt, at most two font families.
- [ ] Source line names the tools behind the numbers and the as-of date.
- [ ] The build script sits next to the deck and reruns cleanly.

Content, before the render is looked at:

- [ ] Entity confirmed: issuer, ticker, exchange, share class, fiscal year end.
- [ ] Every metric traceable to period, units, source and confidence.
- [ ] Every adjective carries its number, or is gone.
- [ ] Valuation heading matches the evidence actually sourced.
- [ ] Conflicts flagged with the selected figure, and preliminary or pro-forma figures labelled.
- [ ] Nothing from the never-silently-omit list is missing without appearing in the evidence gaps.
- [ ] Evidence gaps present, in the source line or as one bullet, never as a table of blanks.
- [ ] One posture, stated once near the top, read from the ladder in `.agents/skills/research-conventions/SKILL.md`.
- [ ] No thesis, no recommendation, no scenario: the page states what is, and the handoff names who owns the rest.
- [ ] The delivery message names the next analytical step this profile enables.
