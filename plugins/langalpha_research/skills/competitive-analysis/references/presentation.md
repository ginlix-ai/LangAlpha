# Presentation and Format

Deferred reference for `competitive-analysis`. Load it when the deliverable is a deck or a formatted document, before the build, and whenever the request specifies exact titles, sections, chart types, competitor lists or figure formats.

Geometry, margins, typography, palette, the build script and the render-and-look loop belong to the deliverable skill: `.agents/skills/pptx/SKILL.md` for a deck, `.agents/skills/docx/SKILL.md` for a memo. This file carries what is specific to a competitive deck on top of those.

## Prompt fidelity

When the request specifies something explicitly, reproduce it verbatim. It is a requirement, not a starting point.

**Titles and section names.** A request for an "Overview and Competitive Scope" slide gets that title, character for character, not a livelier paraphrase. A request for content "within the Segment Mix section" gets a section header reading `Segment Mix`.

**Chart against table.** A request for an embedded chart gets a real chart object built through the deliverable skill, not a table and not an ASCII drawing. Data labels requested on a chart go on the chart elements (bars, slices, lines), not into table cells. The two are not interchangeable.

**Complete series.** Seven competitors named in the request means seven in the table. Years 2015 to 2025 means every year. Six series specified for a chart means six series.

**Exact values and phrasing.** A figure given as `Revenue: $43.98B (+18% YoY)` appears in that format. Ratios given as "4:1 and 8:1" stay those ratios rather than becoming "7.6x". Percentages given per company are used as given.

When something looks ambiguous, re-read the request before inventing a resolution.

## Design and formatting

- **Slide titles are insights**: "Scale leaders pulling away from niche players", not "Competitive Analysis".
- **Titles fit**: one or two lines, no overflow. Shorten the wording first, and drop a size only as far as the deliverable skill allows.
- **Signposts are quantified**: "margin below 40%", not "margins decline".
- **Ratings carry their actuals**: "●●● $160B", not "●●●" alone.
- **Every slide carries a page number.**

## Competitive deck specifics

- **Charts are real chart objects.** Pie, bar and line charts are built as chart objects through the deliverable skill, never as text or ASCII representations.
- **Competitor tables.** A comprehensive analysis gives each competitor a metrics table and a qualitative table. A rapid assessment may combine them into one.
- **Segment financials.** Show revenue and EBITDA where both are disclosed. Revenue-only tables are correct for private competitors and thin disclosure, marked `[EBITDA not disclosed]`.
- **Match the requested structure.** Where the request lays out a slide order, follow it.

## Visual reference

**Spacing and overflow**
- Minimum 0.4" between the bottom of a slide title and the first content element.
- Minimum 0.25" between a section header and the content under it.
- Minimum 0.2" between any two elements (tables, text boxes, charts).
- Text that does not fit gets a smaller font or a second slide, never a clip or an overlap.

**Charts**
- Legends set `include_in_layout=True` so they never sit on the plot area.
- Legend at RIGHT for a pie chart of six or fewer slices, at BOTTOM for a line or bar chart of four or fewer series.
- Past six series, split the chart or switch to a table.
- Pie charts show percentages on the slices rather than relying on the legend.

**Layout**
- Tables and text blocks align to a consistent grid.
- Generous whitespace, one key message per slide, supporting detail below it.
- The most important insight is the most prominent element on the slide.

**Colour**
- Two or three colours, one of them an accent for emphasis.
- Muted tones (navy, gray, muted blue) over bright saturated ones.
- One colour keeps one meaning throughout the deck.

**Tables**
- Light gray header row, bold text.
- Subtle alternating row shading, or clean white with thin borders.
- Numbers right-aligned, text left-aligned, cells padded so text never touches a border.

**Rating visuals**
- ●●● / ●●○ / ●○○ with the actual metric alongside, in a consistent position across every comparative table.

Adapt the structure and the metrics to the industry, and hold this level of polish.

## Strict against flexible

| Strict every time | Flexible, case by case |
|---|---|
| Exact titles and sections when the request specifies them | Creative titles when it does not |
| Chart where a chart was asked for, table where a table was | Visualisation type when unspecified |
| Every data point and competitor the request lists | Number of competitors when unspecified |
| Exact values and ratios when given | Rounding when precision is unspecified |
| Titles fitting without overflow | Number of competitor categories |
| Minimum spacing between elements | Which dimensions to compare |
| Legends inside the layout | Number of competitors profiled |
| No overlapping text or elements | Visualisation form (2x2, radar, tier) |

## Presentation checklist

Run this before delivery, alongside the analysis checklist in `.agents/skills/competitive-analysis/SKILL.md`.

**Fidelity**
- Slide titles and section names match the request exactly.
- Charts where the request said chart, tables where it said table.
- Every competitor, year and series the request listed is present.
- Values, formats and phrasing match what was specified.

**Layout**
- 0.4" or more between the title and the first element.
- Nothing overlaps, and nothing sits inside the side margin the deliverable skill sets.
- All text fits its container, and titles fit the slide width.

**Charts**
- Legends set to `include_in_layout=True` and positioned per the rule above.
- Six series or fewer per chart.
- Charts are real chart objects.

**Typography**
- Every size comes from the deliverable skill's scale, set explicitly on each element and consistent by element type across the deck.

**Page furniture**
- Slide titles state insights rather than topics.
- Every slide has a page number.
- Every data slide carries one source line, placed so it survives the export.
