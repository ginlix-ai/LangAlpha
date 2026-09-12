---
name: pptx
description: "PowerPoint decks built with pptxgenjs and native charts, existing decks edited in place with python-pptx, then rendered to images and audited for overflow, overlap, bounds and typography before delivery"
---

# PPTX

Build or edit a PowerPoint deck and write it into the task directory (e.g. `work/acme_q4/acme_q4_review.pptx`). Someone will stand in front of it, or forward it to a colleague who will. That is what the format is for: **a deck is a sequence of single claims, each sized to be read from the back of a room.** A slide that needs a paragraph is not a slide, it is a page, and it belongs in a report.

This is the right output when the deck itself is the deliverable: an investment committee pack, a diligence readout, a board update, an earnings walkthrough. It is the wrong output for anything the reader will study alone at their own pace (that is `html-report`) and for anything they will change numbers in (that is `xlsx`).

> **User preferences override these defaults.** A house template, a brand palette, a required title slide, a fixed section order: those outrank every rule here. If the user hands you a deck to work in, work in it and match what is already there. The rules below are for when nothing has been specified.

## Decide: Which Output?

| Want | Use |
|---|---|
| Something to present from, or hand to someone who will present it and edit slide by slide | **pptx** (this skill) |
| A document to read alone, print, or export to PDF | `html-report` |
| A model they will change inputs in | `xlsx` |
| One chart or table inside the conversation | `inline-widget` |
| A live app with a backend | `interactive-dashboard` |

## Workflow

1. **Outline first, one message per slide.** Write the outline before any code, as `## Slide N` headings each carrying one declarative sentence: the claim the slide makes, not its topic. "Revenue grew 21 percent on flat headcount" is a slide. "Revenue" is a filing cabinet. If you cannot write the sentence, you do not have the slide yet.
2. **Write a build script**, `work/<task>/build_<name>.js`, and run it with `NODE_PATH=$(npm root -g) node work/<task>/build_<name>.js`. Never assemble a deck through ad-hoc calls. The script is the source of truth; when the user asks for a change, edit the script and rerun.
3. **Audit the file, not the script.** `python .agents/skills/pptx/scripts/check.py work/<task>/<name>.pptx --strict` reads what the file contains, including what a layout or master contributed, and reports shapes off the slide, boxes covering each other, overflowing text, fonts and placeholders. Fix every `fail`; for every `warn`, either fix it or write the one line in the delivery that says why it stands.
4. **Render and look**: `python .agents/skills/pptx/scripts/render.py work/<task>/<name>.pptx --montage`, then view `montage.png`. This is not optional. Clipped text, a legend over a bar, a title that jumps, one slide twice as dense as its neighbours: all of it is visible here and nowhere else.
5. **Fix in the build script, never in the .pptx**, then rerun steps 2 to 4 until `check.py` passes and the render looks right.
6. **Deliver the `.pptx` and the `.js` beside it**, and say in the reply what each slide claims.

## Design Rules

**Geometry.** 16:9 (`LAYOUT_WIDE`, 13.333 x 7.5 in) unless the user's screen is not. The 0.6 in margin is the left and right content margin, so content spans 12.13 in; vertically each band sits at the `y` below, from the title at 0.45 to the footer band ending at 7.25. One geometry for every content slide:

| Band | y | Height |
|---|---|---|
| Title | 0.45 | 0.60 |
| Kicker (units, period, scope) | 1.05 | 0.35 |
| Rule | 1.42 | 0.02 |
| Content | 1.75 | up to 4.85 |
| Footer and page number | 6.95 | 0.30 |

Write that as one function in the build script and call it for every slide. A title that moves by a tenth of an inch between slides reads as a flicker when the deck is clicked through, and `check.py` reports it.

**One idea per slide.** The title states the finding. The body supports it. If the body supports two findings, that is two slides. Three bullets is a good slide, six is a document, and nine is a filing.

**Type.** Two families at most, and one is usually enough. Sizes:

| Role | Size | Weight |
|---|---|---|
| Cover title | 40pt | bold |
| Slide title | 28pt | bold |
| Card or section heading | 18pt | bold |
| Body and bullets | 16 to 18pt | regular |
| Table text | 14pt | header bold |
| Chart axis and data labels | 12pt | regular |
| Footer, source line, footnote | 10pt | regular, secondary colour |

Nothing below 10pt, ever, and nothing below 14pt in a table. A number small enough to need leaning in is a number nobody checks.

**Budgets `check.py` holds you to.** Its overflow estimate assumes an average glyph 0.50 em wide (0.54 bold) and 1.2 line spacing, so a box fits about `width_in * 72 / (pt * 0.5)` characters per line: a 28pt bold title across the 12.13 in content width is one line up to about 56 characters, and the 0.60 in title band holds exactly one. Write the finding in under 56 characters or drop to 24pt. The checker takes the largest text in the top 30 percent of the slide (2.25 in) as the title when there is no title placeholder, which is every pptxgenjs deck, so a 40pt number in that band is read as a wandering title: big numbers sit below 2.25 in. Page numbers are a 10pt `addText` box in the footer band, never `slide.slideNumber`, which ignores `fontSize` and renders at 18pt in a box too short for it.

**Fonts.** Arial, Calibri, Cambria, Times New Roman and Courier New each have a metric-compatible clone installed here (Liberation Sans, Carlito, Caladea, Liberation Serif, Liberation Mono), so the render you inspect breaks its lines exactly where PowerPoint will. Georgia, Verdana, Tahoma, Trebuchet MS and anything else do not: they render substituted, so the layout you checked is not the layout the reader gets. Set `fontFace` explicitly on every text element; a run with no font takes whatever the theme hands it.

**Colour.** One accent, used for one meaning. Everything else is ink, grey, and paper.

| Role | Hex |
|---|---|
| Slide ground | `FFFFFF` |
| Title and body ink | `1A1A1A` |
| Secondary text, labels, footers | `5A5A5A` |
| Rules, borders, gridlines | `D8D5D0` |
| Panel and card fill | `F4F2EE` |
| Accent, and chart series 1 | `1F4E79` |
| Chart series 2, 3 | `4E86B8`, `8FB4D4` |
| Positive, gain | `1A7F4F` |
| Negative, loss | `B42318` |

Green and red mean gain and loss and nothing else. Never encode a category in a colour that already means a direction.

**Speaker notes** when the user will present, or when they asked. One or two sentences per slide saying what to say, not a transcript of the slide. `check.py --require-notes` enforces the presence, not the quality.

## Build a New Deck

pptxgenjs 4.0.1 is installed globally, so the script needs `NODE_PATH=$(npm root -g)`.

```js
const PptxGenJS = require("pptxgenjs");

const INK = "1A1A1A", MUTED = "5A5A5A", RULE = "D8D5D0", ACCENT = "1F4E79";
const FONT = "Arial", M = 0.6, W = 13.333, CONTENT_W = W - 2 * M;

const pptx = new PptxGenJS();
pptx.layout = "LAYOUT_WIDE";

// One geometry for every content slide, so the title never moves.
function contentSlide(title, kicker) {
  const slide = pptx.addSlide();
  slide.background = { color: "FFFFFF" };
  slide.addText(title, { x: M, y: 0.45, w: CONTENT_W, h: 0.6,
    fontFace: FONT, fontSize: 28, bold: true, color: INK, valign: "middle" });
  slide.addText(kicker, { x: M, y: 1.05, w: CONTENT_W, h: 0.35,
    fontFace: FONT, fontSize: 14, color: MUTED, valign: "top" });
  slide.addShape("rect", { x: M, y: 1.42, w: CONTENT_W, h: 0.02, fill: { color: RULE } });
  return slide;
}

const s = contentSlide("Revenue grew 21 percent on flat headcount", "Three findings drive the FY2026 view");
s.addText(
  [
    { text: "Q4 revenue of $1,240mm beat consensus by 3.4 percent.", options: { bullet: true } },
    { text: "Gross margin held at 64.2 percent on a heavier hardware mix.", options: { bullet: true } },
    { text: "Net retention of 118 percent carries half the FY2026 plan.", options: { bullet: true } },
  ],
  { x: M, y: 1.9, w: CONTENT_W, h: 2.6, fontFace: FONT, fontSize: 18, color: INK,
    lineSpacingMultiple: 1.4, valign: "top" }
);
s.addNotes("Do not read the bullets out; the room has already read them. Lead with the margin point.");

pptx.writeFile({ fileName: "work/acme_q4/acme_q4_review.pptx" })
  .then(() => console.log("written"));         // the check runs on the file: see step 3
```

Geometry is checked on the written file by `check.py`, which reads the real slide XML rather than the build script's intent. A shape crossing a slide edge, or two text boxes covering each other by more than 0.1 in in both directions, is a `fail`. Fix every one in the script and rebuild.

### Charts stay native

`addChart` writes a real chart part that the reader can click into, retheme, and read numbers off. A picture of a chart is a dead end for them and blurs at any zoom.

```js
slide.addChart(
  pptx.ChartType.bar,
  [{ name: "Revenue ($mm)", labels: ["Q1", "Q2", "Q3", "Q4"], values: [980, 1035, 1128, 1240] }],
  { x: 0.6, y: 1.75, w: 7.4, h: 4.6,
    barDir: "col", chartColors: [ACCENT], showLegend: false, showValue: true,
    dataLabelFontFace: FONT, dataLabelFontSize: 12, dataLabelColor: INK,
    catAxisLabelFontFace: FONT, catAxisLabelFontSize: 12, catAxisLabelColor: MUTED,
    valAxisLabelFontFace: FONT, valAxisLabelFontSize: 12, valAxisLabelColor: MUTED,
    valAxisMaxVal: 1400, valGridLine: { color: RULE, style: "solid", size: 1 },
    catGridLine: { style: "none" }, valAxisLineShow: false, catAxisLineShow: false }
);
```

- Series data is `[{ name, labels, values }]`, one object per series, `labels` identical across series.
- One series: pass **one** colour. `chartColors` with several entries colours each bar separately, which looks like four categories where there is one.
- Units go in the kicker or the axis title, not on every label. Turn the legend off when there is one series.
- `pptx.ChartType.bar` with `barDir: "col"` is vertical, `"bar"` is horizontal. `line`, `pie`, `doughnut`, `area`, `scatter` are the rest of the useful set.
- A combo chart (bars for one series, a line for another) is an array of `{ type, data, options }` in place of the single type, with the shared geometry and axis options in the second argument:

  ```js
  slide.addChart(
    [{ type: pptx.ChartType.bar,  data: [{ name: "Revenue ($B)", labels, values: revenue }], options: { chartColors: [ACCENT], barDir: "col" } },
     { type: pptx.ChartType.line, data: [{ name: "FCF ($B)", labels, values: fcf }], options: { chartColors: [SERIES2], lineSize: 3, lineDataSymbol: "circle" } }],
    { x: 0.6, y: 1.75, w: 7.3, h: 4.7, showLegend: true, legendPos: "b", legendFontSize: 12,
      catAxisLabelFontSize: 12, valAxisLabelFontSize: 12, valGridLine: { color: RULE, style: "solid", size: 1 } }
  );
  ```
- Reach for an image only when the chart type genuinely does not exist here (a waterfall, a slope chart, a small-multiple grid). Then say in the reply that it is an image.

Put the takeaway beside the chart in words. A chart with no sentence next to it makes the room do the work.

### Tables

```js
const head = { fill: { color: ACCENT }, color: "FFFFFF", bold: true };
slide.addTable(
  [[{ text: "Segment", options: head }, { text: "Q4 revenue", options: head }],
   ["Platform", "742"],
   [{ text: "Total", options: { bold: true } }, { text: "1,240", options: { bold: true } }]],
  { x: M, y: 1.9, w: CONTENT_W, colW: [4.6, 7.53], fontFace: FONT, fontSize: 14,
    color: INK, rowH: 0.42, valign: "middle", border: { type: "solid", pt: 1, color: RULE } }
);
```

A table is as tall as its rows: `rowH` times the row count, whatever `h` says, and that sum is what the geometry checks measure. Keep `y` plus the sum inside the content band.

Six rows and four columns is a slide. Twelve rows is a handout, so cut it to the rows that carry the argument and put the full table in an appendix slide or an xlsx. Right-align numbers, put units in the header, and state totals rather than implying them.

## Editing an Existing Deck

**Never rebuild a human's deck to change three words.** Their master, layouts, theme, and every slide you are not touching are work you would be throwing away, and rebuilding loses the parts nobody documented. Open it with python-pptx and write into it.

```python
from pptx import Presentation
from pptx.util import Inches, Pt

prs = Presentation("work/acme_q4/client_deck.pptx")

def set_text(shape, new_text):
    """Replace a shape's words and keep its formatting.

    The first run carries the font, size and colour, so writing into it and
    dropping the rest preserves the look without having to know what it is.
    `shape.text = "..."` throws all of that away.
    """
    frame = shape.text_frame
    paragraph = frame.paragraphs[0]
    if not paragraph.runs:
        paragraph.add_run()
    paragraph.runs[0].text = new_text
    for run in paragraph.runs[1:]:
        run._r.getparent().remove(run._r)
    for extra in frame.paragraphs[1:]:
        extra._p.getparent().remove(extra._p)

slide = prs.slides[3]
set_text(slide.shapes.title, "Segment detail, restated")

# A new slide comes from a layout the deck already has, so it inherits the design.
layout = next(l for l in prs.slide_layouts if l.name == "Title and Content")
new = prs.slides.add_slide(layout)
new.shapes.title.text = "FY2026 bridge"
new.placeholders[1].text_frame.text = "Volume carries 14 of the 21 points."

prs.save("work/acme_q4/client_deck_v2.pptx")
```

Read the deck first: `python .agents/skills/pptx/scripts/extract.py deck.pptx` gives every slide's text, notes, tables, charts, and the shape names and coordinates you will write into. `markitdown deck.pptx` is faster for prose alone but drops every position, so it answers "what does this say" and never "which box do I write into". A legacy `.ppt` opens with `python -c "import anydoc,sys; print(anydoc.to_markdown(sys.argv[1]))" old.ppt` for the same read-only view (never pass `ocr="hosted"`, which uploads the file to an external service); to edit one, convert it with `soffice --headless --convert-to pptx old.ppt` and start from the `.pptx`.

**What a python-pptx save preserved when it was measured**, on one six-slide deck with a chart, a table and notes: the theme part came back byte for byte, the slide parts that were not edited came back with the same elements, attributes and order (differing only in the whitespace between tags), and the native chart kept its series values. Two parts were rewritten and both are harmless: `[Content_Types].xml` is rebuilt from the parts present, and a relationship target is normalised from an absolute to a relative path. That is a measurement of one deck, not a guarantee for yours, so prove it on your own edit:

```bash
python3 -c "
import zipfile
a, b = zipfile.ZipFile('before.pptx'), zipfile.ZipFile('after.pptx')
print([n for n in sorted(set(a.namelist()) & set(b.namelist()))
       if not n.endswith('/') and a.read(n) != b.read(n)])"
```

Then render both with `render.py` and compare the slides you did not touch. That comparison is mandatory, not a formality: a part that survives the byte diff can still come out different once a renderer lays it out. Rules that follow:

- **Match what is there.** Read a neighbouring slide's fonts, sizes and colours from `extract.py` and reuse them. Do not restyle a deck the user did not ask you to restyle.
- **Placeholders inherit their position** from the layout. Reading `shape.left` gives the inherited value; writing to it pins the shape and ends the inheritance, so leave it alone unless moving it is the task.
- **A run's `font.size` of `None` means inherited**, not 18pt. Do not "fix" it by setting a size.
- **There is no copy-a-slide.** Add a slide from the same layout and re-add the content. There is no delete either; remove the `sldId` from `prs.slides._sldIdLst` when you must.
- **Chart data**: `chart.replace_data(CategoryChartData(...))` refreshes the numbers and keeps the formatting. Deleting and re-adding the chart loses it.
- Rerun `check.py` and `render.py` on the edited file. An edit is a deck change like any other.

## Verification Scripts

All three live under `.agents/skills/pptx/scripts/`, take a `.pptx` path (or `--help`), and print JSON to stdout. They read the saved file, so they judge a deck the same way whoever built it: yours, or the one the user sent you.

**`render.py <deck.pptx> [--out DIR] [--dpi N] [--montage] [--cols N] [--keep-pdf]`**: LibreOffice to PDF, pdftoppm to PNG, one image per slide, plus a numbered contact sheet with `--montage`.

```json
{"status": "success", "out": "...", "dpi": 110, "pages_rendered": 6, "slides_in_deck": 6,
 "pages": ["...slide-1.png"], "montage": "...montage.png"}
```

`status` is `success` or `error` (no PDF, no images), and only `error` exits non-zero. A `warning` field appears when the page count does not match the slide count, which means LibreOffice dropped or split a slide. **Look at the montage.** A script cannot tell you the deck is ugly.

**`check.py <deck.pptx> [--strict] [--require-notes] [--slide N]`**: the audit. `--slide` counts from 1 and errors when the deck has no such slide, rather than passing an audit of nothing.

```json
{"status": "fail", "stats": {"slides": 6, "chart_parts": 1, "images": 0, "tables": 1,
  "pictures": 0, "notes_slides": 6, "aspect": 1.778, "fonts_used": ["Arial"],
  "fonts_theme": {"major": "Calibri Light", "minor": "Calibri"}},
 "findings": [{"check": "bounds", "level": "fail", "count": 2,
   "message": "shape crosses a slide edge; part of it will never be seen",
   "examples": ["slide 2 (Shape 1: right 15.00 > 13.33)"]}]}
```

`status` is `pass` or `fail`. `fail`-level findings block delivery, `warn` is a judgement call, `info` is context. Only `--strict` makes a failing deck exit non-zero, so put `--strict` in the loop and let it stop you.

| Check | Level | What it means |
|---|---|---|
| `package`, `package_slides`, `package_media` | fail / warn | corrupt zip, slide parts that do not match the slide list, media nothing references |
| `bounds` | fail | a shape crosses a slide edge; a table is measured by the sum of its row heights |
| `overlap`, `overlap_shapes` | fail / warn | two shapes cover 15 percent of the smaller one, at least 0.1 in in both directions; containment is not reported when the outer shape holds no text (a card, a full-slide background), and is reported when it does |
| `text_overflow`, `text_tight` | fail / warn | estimated line count needs more height than the box has |
| `placeholder` | fail | `Click to add`, `Lorem`, `TODO` or `[INSERT` still in the deck |
| `font_size`, `font_size_table` | fail | body under 10pt, table text under 14pt |
| `fonts`, `fonts_metric` | fail / warn | more than two families; a family with no metric-compatible clone |
| `title_drift` | warn | a title away from the position the rest of the deck uses; the title is the placeholder or, without one, the largest text in the top 30 percent of the slide |
| `aspect` | warn | not 16:9 |
| `notes` | fail | with `--require-notes`, a content slide with none |
| `charts_as_pictures` | warn | a large bitmap, or one named like a chart, where a native chart belongs |
| `font_size_inherited`, `geometry_unknown` | info | runs and shapes whose size or position comes from the layout, so the rule could not be applied |

Overflow is an estimate from font size, box width and character count, not a measurement: it is deliberately loose and it reports lines, so read a finding as "go look at that slide in the render". Geometry checks measure top-level shapes and treat a group as one box; text inside a group is still read for the content checks.

**`extract.py <deck.pptx> [--slide N] [--text-only]`**: the deck as data, for reading before editing. Per slide: `title` and `title_from` (`placeholder`, or `largest_in_top_band` when the deck has no title placeholders, which is every pptxgenjs deck; that is the rule `check.py` uses too, and `topmost_text` is the last resort when nothing sits in the top band), `layout`, `text` (including table rows), `notes`, a `shapes` inventory with names, kinds, inches and font sizes (a table reports `height` as the extent the slide shows, with `frame_height` and `rows_height` beside it so you can see the two disagree), plus `tables` (cells) and `charts` (type, categories, series values; an XY or bubble series reports `x_values`, `y_values` and, for bubbles, `bubble_sizes` in place of `values`). `--slide` counts from 1 and errors when the deck has no such slide, rather than returning an empty `slides`.

## Pitfalls

**pptxgenjs**

- **Units are inches**, and pptxgenjs reads any number of 100 or more as EMUs instead. Keep coordinates in inches and never let a computed value cross 100.
- **Percentage strings** (`x: "50%"`) resolve against the slide at render time, so a mix of inches and percentages in one layout is a layout you cannot reason about. Pick inches.
- **`autoPage` is off by default**, so a table taller than the slide runs off the bottom silently. `check.py` takes a table's bounds from the sum of its row heights, which is what PowerPoint draws rather than the frame height stored in the file, and report it out of bounds. Cut the table, or set `autoPage: true` and accept that pptxgenjs picks the break points and the extra slides skip your title helper.
- **Never use `fit: "shrink"` or `autoFit`.** pptxgenjs writes `<a:normAutofit/>` with no `fontScale`, so LibreOffice shrinks the text to fit and PowerPoint does not: the render passes and the reader sees the overflow. Size the box to the text instead.
- **Text is vertically centred by default**, so an overfull box spills equally above and below and can cover the title. Set `valign: "top"` on every content box.
- **Rich text is an array of runs**: `[{ text, options }]`. `breakLine: true` ends a paragraph, `bullet: true` makes a bullet. A plain string is a single run and takes the box's options.
- **Bullets come from `{ bullet: true }`**, never a literal `•` character, which cannot be restyled and breaks indentation.
- **Images**: `sizing: { type: "contain" | "cover" | "crop", w, h }` preserves the aspect ratio. Setting `w` and `h` alone stretches the picture.
- **Colours are `RRGGBB` with no `#`.** `"#1F4E79"` is silently wrong.
- **`writeFile` returns a promise.** Do the work that follows inside `.then()` or the process can exit before the file lands.
- **`LAYOUT_WIDE` is 13.333 x 7.5 in, `LAYOUT_16x9` is 10 x 5.625 in.** Same ratio, different coordinate system. Choose once, at the top.

**python-pptx**

- `shape.text = "..."` and `cell.text = "..."` destroy every run's formatting. Use the `set_text` helper above.
- `shape.left` and friends are EMUs. `Inches(1.5)` to write, `shape.left.inches` to read.
- Table cells: `cell.text_frame.paragraphs[0].runs[0]`, same rules as any other text frame.
- Do not open a deck with python-pptx to *build* one from nothing. Its default template is 4:3 with placeholders you will fight; pptxgenjs is the build path.
- Charts added by python-pptx need `CategoryChartData`; the pptxgenjs data shape is not the same.

## Deliverable Checklist

- Every slide's claim is stated in its title, and the outline sentence and the title still match.
- `check.py --strict` passes.
- The montage was rendered and looked at, and every slide was worth looking at twice.
- 16:9, one geometry for titles, nothing outside the 0.6 in side margins or below the footer band.
- At most two font families, all metric-safe; nothing under 10pt, nothing under 14pt in a table.
- Numeric charts are native chart parts (`stats.chart_parts` is not zero when the deck has charts), one colour per series, units stated once.
- Speaker notes where the user will present.
- No `Click to add`, no `TODO`, no `Lorem`, no source line missing from a slide that quotes a number.
- An edited deck was diffed against the original and only the parts you meant to change moved.
- The build script sits next to the deck and reruns cleanly, and the reply names the file and what each slide claims.
