---
name: xlsx
description: "Excel workbooks with live formulas: build or edit .xlsx models with openpyxl, recalculate with IronCalc or LibreOffice, audit conventions, profile messy uploads, render for review"
---

# XLSX

Build or edit an Excel workbook and write it into the task directory (e.g. `work/dcf_acme/acme_dcf.xlsx`). The user opens it in Excel, changes an input, and every dependent number moves. That is the whole point of the format: **a workbook the agent delivers is a model, not a table of results.** A cell that could be computed from other cells is a formula. A number typed into a formula's place is a defect, even when the value is right today.

This is the right output when the user wants something they will **keep working in**: a DCF, a comps sheet, a three-statement model, a screen they will re-sort, a schedule they will extend. It is the wrong output for a one-off table (put it in the chat or an HTML report) and for a document that will not be edited (see `.agents/skills/html-report/SKILL.md`).

> **User preferences override these defaults.** A house colour code, a template workbook, a sheet layout the user has described: those outrank every rule here. Inside a workbook the font colour code below outranks the general house palette in `ui-design` (green means a cross-sheet link here, not a gain). The rules below are for when nothing has been specified.

## Decide: Which Output?

| Want | Use |
|---|---|
| A model the user will edit, extend, or plug into their own workbook | **xlsx** (this skill) |
| A polished document to read, print, or share, possibly with charts | `html-report` |
| A dataset for another program | `.csv` via pandas, no styling |
| One table in the conversation | markdown table |

## Workflow

1. **Plan the sheets** before writing code: which sheet holds inputs, which holds calculations, which holds outputs. Sketch the row labels and the period columns. A model that is laid out first is a model whose formulas can be copied across.
2. **Write a build script**, `work/<task>/build_<name>.py`, and run it. Never build a workbook cell by cell in ad-hoc calls. The script is the source of truth; when the user asks for a change, edit the script and rerun.
3. **Recalculate**: `python .agents/skills/xlsx/scripts/recalc.py work/<task>/<name>.xlsx 60`. Fix every listed error and rerun until `"status": "success"`.
4. **Audit**: `python .agents/skills/xlsx/scripts/audit.py work/<task>/<name>.xlsx --strict`. Fix every `fail`; for every `warn`, either fix it or write the one line in the delivery that says why it stands. `--strict` is the prescribed form: it exits non-zero on a `fail` and promotes `formula_number_format` from a warning.
5. **Render and look**: `python .agents/skills/xlsx/scripts/render.py work/<task>/<name>.xlsx`, then view the PNGs. Overflowing `###` columns, unformatted rates, and headers that do not line up are visible here and nowhere else.
6. **Spot-check three numbers by hand** against your own calculation before delivering. A clean recalc proves the formulas evaluate, not that they are the right formulas.
7. **Assert the sensitivity centre in the build script.** Where the model has a sensitivity grid, the script reopens the workbook after `recalc.py` with `data_only=True`, reads the centre cell of every grid and the headline output it varies, and asserts they are equal. A grid that has come unwired then fails the build instead of shipping. Name the same pair in a `Checks` row (below): that row is what lets `audit.py` re-check the equality on a workbook nobody rebuilt, and without it the audit reports `sensitivity_unverified` rather than deciding for itself which output the grid varies.

## Formulas, Not Values

Every derived number is a formula. Inputs live in their own cells and formulas reference them. The sole exception is a **solved value**: an inverse-model parameter (the growth rate a spot price implies, the rate that zeroes a residual) that needs numerical root-finding and has no direct solution the sheet can hold without a circular reference. Choosing to solve in Python or with Goal Seek does not qualify a calculation the sheet could express. A solved value is stored as a blue input with a `Solved:` comment (form under Provenance below) and a live residual row on `Checks`: the forward calculation at the solved value minus its target, with a tolerance, so the row fails when the target or any other input has moved and the value no longer fits.

```python
from openpyxl import Workbook
from openpyxl.styles import Font
from openpyxl.comments import Comment

wb = Workbook()
inputs = wb.active
inputs.title = "Inputs"
inputs["A1"], inputs["B1"] = "Revenue growth", 0.08
inputs["B1"].font = Font(color="0000FF")          # blue = hardcoded input
inputs["B1"].number_format = "0.0%"
inputs["B1"].comment = Comment("Source: FY2025 guidance, Q4 call", "LangAlpha")
inputs["A2"], inputs["B2"] = "FY2025 revenue ($mm)", 1240.0
inputs["B2"].font = Font(color="0000FF")
inputs["B2"].number_format = "#,##0"
inputs["B2"].comment = Comment("Source: 10-K FY2025, p.42", "LangAlpha")

model = wb.create_sheet("Model")
model["A1"] = "($mm)"
for i, year in enumerate(range(2026, 2031)):
    model.cell(row=1, column=2 + i, value=f"FY{year}E").font = Font(bold=True)
model["A2"] = "Revenue"
model["B2"] = "=Inputs!B2*(1+Inputs!$B$1)"            # first period reads the inputs sheet
model["B2"].font = Font(color="008000")                # green = cross-sheet link
for col in "CDEF":
    prev = chr(ord(col) - 1)
    model[f"{col}2"] = f"={prev}2*(1+Inputs!$B$1)"     # later periods chain off the previous one
wb.save("work/<task>/model.xlsx")
```

Rules that follow from this:

- **Reference, never retype.** `=B4*(1+Inputs!$B$1)`, not `=B4*1.08`. A magic number inside a formula is an input hiding where nobody will find it.
- **One formula per line item, copied across periods.** If FY2027 needs a different formula from FY2026, the model has a structural problem; fix the structure.
- **Totals sum the block directly above them.** `=SUM(B3:B7)`, not `=B3+B4+B5+B6+B7` and never a typed number.
- **Helper cells beat clever formulas.** A three-line calculation in three labelled rows is auditable; one nested formula is not.
- **Anchor with `$` deliberately.** Assumptions are `$B$1` (fixed), period references are relative, row references down a schedule are `B$1` when the column moves.
- **Guard denominators.** `=IF(B5=0,0,B6/B5)` where a zero is possible. A `#DIV/0!` in a delivered model is a defect.
- **Sensitivity tables are grids of full formulas**, not Excel Data Tables and not linear approximations. Odd dimensions (5x5, 7x7) so the base case has a centre to sit in, every cell reading the two axis headers that cross on it (`$A25` for its row, `B$24` for its column) rather than a value written into it, and the centre cell equal to the model's own output, which is the built-in check that the table is wired correctly.
- **Text that starts with `=`** is written as `"'=..."` or stored via `cell.value = "=..."` only when it is meant to be a formula.

## Formula Compatibility

The workbook must open in Excel 2016, LibreOffice, and Google Sheets without `#NAME?`.

| Do not use | Why | Use instead |
|---|---|---|
| `XLOOKUP`, `XMATCH`, `FILTER`, `SORT`, `UNIQUE`, `SEQUENCE`, `LET`, `LAMBDA` | Excel 365 only; LibreOffice and older Excel return `#NAME?` | `INDEX`/`MATCH`, `SUMIFS`, helper columns |
| `TEXTJOIN`, `CONCAT`, `IFS`, `SWITCH`, `MAXIFS`, `MINIFS`, `IFNA` without prefix | openpyxl writes the bare name and Excel shows `#NAME?` | write `_xlfn.TEXTJOIN(...)`, or avoid |
| `INDIRECT`, `OFFSET` | volatile, break on sheet renames, unauditable | direct references, `INDEX` |
| `WEBSERVICE`, `RTD`, `FILTERXML` | pull data or leak it at open time | never (`HYPERLINK` to a source page is fine) |
| `=TABLE(...)` data tables, array `{=...}` formulas | not portable through openpyxl | explicit formula grids |
| `DCF!H82:DCF!L82`, a range qualified at both ends | Excel tolerates it; IronCalc returns `#ERROR!` and `recalc.py` reports the cell | qualify once: `DCF!H82:L82` |
| `--(condition)` to coerce a boolean array | evaluates to 0 in IronCalc, so a check built on it reads FAIL or passes for the wrong reason | `(condition)*1` inside `SUMPRODUCT` |

Sheet names with spaces are quoted: `='Cash Flow'!B5`. Keep sheet names short and unquoted where you can.

## Financial Model Conventions

**Colour code** (font colour; the reader learns what to touch at a glance):

| Cell | Colour | Hex |
|---|---|---|
| Hardcoded input | blue | `0000FF` |
| Formula | black | `000000` |
| Link to another sheet | green | `008000` |
| Link to another workbook | red, and avoid entirely | `FF0000` |
| Key assumption needing attention | yellow fill | `FFFF00` fill |

**Number formats** (set `cell.number_format`; never leave a computed number as `General`):

| Kind | Format | Shows |
|---|---|---|
| Currency, millions | `#,##0;(#,##0);"-"` | `1,240` `(85)` `-` |
| Currency, one decimal | `#,##0.0;(#,##0.0);"-"` | `1,240.5` |
| Percent | `0.0%` | `8.0%` |
| Multiple | `0.0"x"` | `12.4x` |
| Share price | `$#,##0.00` | `$182.40` |
| Year header | text `FY2026E`, not a number | avoids `2,026` |
| Date | `yyyy-mm-dd` on a real date value | sorts correctly |

Negatives in parentheses, zero as a dash, units in the header (`Revenue ($mm)`), never in every cell.

**Layout**:

- Sheets: `Cover` (below), then `Inputs` (or `Assumptions`), then calculation sheets, then `Output` or `Summary`. Raw data pulled from an API goes on its own sheet, left as values, cited in a note at the top.
- Column A holds labels, column B onward holds periods. Indent sub-items with two leading spaces or `cell.alignment = Alignment(indent=1)`.
- Section headers: bold, dark fill `1F4E79` with white text, merged across the period columns. Column headers: fill `D9E1F2`. Input blocks: fill `F2F2F2`. Check rows: fill `BDD7EE`. Do not border every cell; a thin top border above totals is enough.
- `ws.column_dimensions["A"].width = 28`, period columns 12 to 14. `ws.freeze_panes = "B2"`. `ws.sheet_view.showGridLines = False` on model sheets.
- Print setup so a sheet is one page, in Excel and in `render.py`: `ws.page_setup.orientation = "landscape"`, `ws.page_setup.fitToWidth = 1`, `ws.page_setup.fitToHeight = 1`, `ws.sheet_properties.pageSetUpPr.fitToPage = True`, which is the line that makes either fit apply at all. `fitToHeight = 0` fits the width only and lets the rows run on for as many pages as they need, which is what a long schedule wants; `fitToHeight = 1` fits the whole sheet on one page, which is what a summary or a sensitivity grid needs, and the page count from `render.py` tells you which one you got. Without a fit a 12-column model prints and renders as tiles.
- Right-align numbers, left-align labels. Column labels for numeric data right-aligned.

**Cover sheet**: the first visible sheet is what a human meets when they open the file cold, so it reads as a summary of the answer. A contents-only cover is a defect. It carries, top to bottom: a status strip (the readiness posture the model skill assigned, the as-of date of the data, and whether the workbook has been recalculated), the read-through in one or two sentences, headline metric tiles, the scenario output table, the source posture in a line, the open caveats, and a map of the remaining sheets saying what each one holds. Every number on it is a live formula pointing at the cell it reports (`=Output!B12`, green), never a value typed across, or the cover starts lying the first time the reader changes an input; `audit.py` reports `cover_static` when the first visible sheet holds no formula at all. The cover is an additional first sheet, not a reason to fan the model out: a three-sheet DCF becomes `Cover`, `DCF`, `WACC`, `Checks` and the model keeps its shape.

**Provenance**: every blue input carries a cell comment naming its source (`Comment("Source: 10-K FY2025, p.42", "LangAlpha")`). A judgement call is a source too: write `Comment("Assumption: glide toward the long-run rate", "LangAlpha")`, never an empty comment, which `audit.py` counts as missing. A solved value names its target and the way back to it: `Comment("Solved: revenue CAGR at which DCF!B40 equals spot DCF!B7; re-solve with python build_dcf.py --resolve", "LangAlpha")`, and `audit.py` fails a `Solved:` cell that no `Checks` verdict depends on. External data gets a plain-text URL in an adjacent note cell or on the data sheet. The human reading the model should never have to ask where a number came from.

**Assumptions block**: every scenario knob (growth, margin, discount rate, exit multiple) is one labelled input cell. A case selector is a single input cell that formulas `CHOOSE` or `INDEX` from; never three copies of the model.

**Checks sheet**: a model carries a `Checks` sheet, one row per tie-out, and every test is a live formula so it re-evaluates when the reader changes an input. Four columns: `A: label | B: =difference formula | C: =IF(ABS(B5)<0.01,"OK","FAIL") | D: basis`, where D says in words which two cells B compares, so a failing row points at the disagreement. A direction or bound compares instead of applying a tolerance (`=IF(B17>0,"OK","FAIL")`). A check computed in Python and written in as text is not a check.

Two verdicts, because two kinds of row live here. A **hard check** is an identity that holds exactly, and its verdict is `FAIL`: the model is wrong. A **diagnostic** carries a band a defensible forecast can sit outside of, and its verdict is `WARN`: `=IF(<inside the band>,"OK","WARN")`. Giving an identity a tolerance and letting a judgement call block delivery are the same mistake in opposite directions, so the sheet keeps the two apart and rolls them up separately, in two rows at the bottom:

```
A                | B                       | C                                         | D
Overall          |                         | =IF(COUNTIF(C5:C40,"FAIL")=0,"OK","FAIL") | hard checks
Diagnostics open | =COUNTIF(C5:C40,"WARN") |                                           | bands breached; each reason sits in column D of its own row
```

`C5:C40` stands for exactly the verdict rows written, and the two roll-up rows sit outside it. After `recalc.py`, read column C and both roll-up rows back with `data_only=True`. **A `FAIL` blocks delivery**: fix the model, not the check. A `WARN` does not block, and it is not free either: every open one carries its reason in column D and one sentence in the delivery saying why the forecast is right and the band is not. The model skills (`dcf-model`, `3-statements`, `comps-analysis`) each list the rows their workbook needs and which group each row belongs to.

**Directional rows**: a model with a sensitivity grid carries one row per axis asserting the direction the economics require, comparing the two ends of the axis through its centre. A higher discount rate lowers value, so `=IF(<bottom centre>-<top centre><0,"OK","FAIL")`; a higher exit multiple raises it, so `=IF(<right centre>-<left centre>>0,"OK","FAIL")`. A grid wired to the wrong cell usually still looks plausible, and it rarely still moves the right way, which is what makes these two rows worth more than they cost. One more row ties the centre to the output the grid varies, `=<centre cell>-<output cell>` against a tolerance, and that row is also how `audit.py` learns which output to re-check the centre against. `audit.py` reports `checks_missing` when the workbook has a grid and no `Checks` sheet.

## Charts

Native Excel charts stay editable. openpyxl's `x_axis.delete` defaults leave axes hidden in current Excel builds; set them explicitly.

```python
from openpyxl.chart import BarChart, LineChart, Reference

chart = BarChart()
chart.title = "Revenue ($mm)"
chart.y_axis.title = "$mm"
data = Reference(model, min_col=2, max_col=6, min_row=2, max_row=2)   # one series across periods
cats = Reference(model, min_col=2, max_col=6, min_row=1, max_row=1)
chart.add_data(data, from_rows=True, titles_from_data=False)
chart.set_categories(cats)
chart.x_axis.delete = False
chart.y_axis.delete = False
chart.width, chart.height = 18, 8
model.add_chart(chart, "H2")                   # add_chart(chart, anchor) is the only placement that works
```

Charts are for the reader; the numbers behind them stay as cells so the chart updates when inputs change. A chart of a complex analysis belongs in an HTML report, not in the workbook.

## Editing an Existing Workbook

- **Read it twice.** `load_workbook(path)` gives formulas; `load_workbook(path, data_only=True)` gives the last cached values. One load cannot give both. If cached values are `None`, the file was written by a program that never calculated; run `recalc.py` on a copy first.
- **Quick read**: `markitdown file.xlsx` prints every sheet as markdown for orientation. It drops cell coordinates, so plan edits from openpyxl, not from that text.
- **A messy upload** (a system export, a hand-kept tracker) is not a model yet. `profile.py` reports the row its header actually sits on, the type of every column, and the issues to fix before any of it can be referenced by a formula. See *Clean a messy sheet* below.
- **Legacy formats**: `python -c "import anydoc,sys; print(anydoc.to_markdown(sys.argv[1]))" old.xls` reads `.xls`, `.xlsb` and `.csv` the same way, in milliseconds. It is read-only; to edit a legacy file, convert it first (`soffice --headless --convert-to xlsx old.xls`) and work on the `.xlsx`. Never pass `ocr="hosted"`: it uploads the document to an external service.
- **Never save a `data_only=True` workbook.** It writes values over every formula. Edit the formula workbook and let `recalc.py` restore the values.
- **openpyxl drops what it does not understand.** Images, shapes, pivot tables, slicers, sparklines and threaded comments do not survive a load and save. Check for them first (`ws._images`, `ws._charts`, `wb._pivots`, `xl/threadedComments` in the zip) and, when they exist, patch the worksheet XML with lxml instead of saving through openpyxl, or tell the user what will be lost.
- **Say what you changed.** When returning a human's workbook, list the cells and sheets you touched in the reply so they can review it, and keep their formulas in Excel 365 syntax if that is what they wrote; `recalc.py` reports `#NAME?` on those, which is a limitation of the checker, not a defect in their file.
- **Preserve what the human built.** Match existing fonts, fills, number formats and column widths for every cell you add. Do not restyle a sheet the user did not ask you to restyle. Keep their sheet order, names, and defined names.
- **Data validation lists** tell you the allowed values: `ws.data_validations.dataValidation[i].formula1` is `"Base,Bull,Bear"` or a range to dereference. Write only values that pass.
- **Merged cells**: only the top-left cell holds the value; writing to another cell in the range raises or is ignored.
- **`.xlsm`**: `load_workbook(path, keep_vba=True)` and save with the same extension, or the macros are gone.
- **Dates**: write `datetime.date` objects with a date `number_format`, not strings.
- **External links** (`[Book2.xlsx]Sheet1!A1`) cannot be resolved in the sandbox; `recalc.py` refuses them. Ask the user for the linked file or replace the link with a value and a comment.
- Read a human's cell comments before editing: `cell.comment.text` and `cell.comment.author`. They are often instructions.

### Clean a messy sheet

A sheet somebody else produced is data, not a model. Profile it before touching it:

```bash
python .agents/skills/xlsx/scripts/profile.py work/<task>/upload.xlsx
```

- **Put the proposal to the user before changing anything.** One table in the reply: column, issue, count, proposed fix. They approve it, then you edit. A sheet quietly tidied is a sheet they can no longer reconcile against the system it came out of.
- **Prefer a helper column to an overwrite.** `=TRIM(A2)`, `=VALUE(SUBSTITUTE(B2,"$",""))`, `=UPPER(C2)`, `=DATEVALUE(D2)` put the cleaned value beside the original instead of on top of it, so every transformation stays visible and reversible in the workbook itself.
- **Overwrite in place only when the user asks for it, or when no formula does the job.** Mojibake is the usual case: `Ã©` has to be rewritten, no worksheet function repairs it.
- **Keep the raw sheet untouched.** Clean onto a new sheet and leave the upload exactly as it arrived, so the two can be compared.
- **Subtotal rows are removed, not cleaned.** They double-count the moment the sheet is summed or filtered; take them out and let a formula compute the total.
- **Record what changed**: the columns cleaned, the rule applied to each, and any row dropped or flagged, both in the reply and in a note cell on the cleaned sheet.

### Inserting rows and columns

`ws.insert_rows(5)` moves cell values and nothing else. Formulas keep their old coordinates, and defined names, chart ranges, merges, conditional formatting, data validation and tables are left pointing at the cells that used to be there, so a line item added that way breaks the model silently. Use `insert.py`, which edits the package parts and repoints everything Excel would repoint.

```bash
python .agents/skills/xlsx/scripts/insert.py rows model.xlsx --sheet Model --at 6 --copy-style-from 5
python .agents/skills/xlsx/scripts/insert.py columns model.xlsx --sheet Model --at D --count 2 --copy-style-from C
python .agents/skills/xlsx/scripts/insert.py delete-rows model.xlsx --sheet Model --at 6
```

- **Insert inside the block, not at its edge.** A `=SUM(B3:B7)` grows to `=SUM(B3:B8)` when the new row lands anywhere in rows 4 to 7; inserting at row 3 or row 8 moves the range instead and the new line stays outside the total. That is Excel's own rule, and the fix is to insert one row further in.
- The new row or column arrives blank, carrying only the style of `--copy-style-from`. Write the label and the formulas into it with openpyxl, then recalculate.
- Deleting a line something references leaves `#REF!` in that formula. Every one is listed in `warnings` with the sheet and cell that now reads it, so run the delete with `--dry-run` first and read that list before anything is written.
- Anchored charts and images ride the cells they sit on, as they do in Excel. Pivot cache source ranges do not move; the report warns when the workbook has one.
- Run `recalc.py` afterwards. Values below the change are stale until something calculates them.
- `insert.py` edits the workbook in place, so the build script no longer reproduces the file. Fold the edit back into the build script, or save the exact commands in a `reproduce.sh` next to the workbook, so the deliverable can still be rebuilt from source.

## Verification Scripts

All five live under `.agents/skills/xlsx/scripts/` and print to stdout; `--help` on any of them prints the full usage with every subcommand and flag.

**`recalc.py <file> [timeout] [--check-only] [--force]`**: recalculates every formula, writes the workbook back with cached values and formulas intact, then reports:

```json
{"status": "errors_found", "total_formulas": 35, "total_errors": 1,
 "error_summary": {"#DIV/0!": 1},
 "errors": [{"sheet": "DCF", "cell": "B15", "error": "#DIV/0!", "formula": "=B11/0"}]}
```

`status` is `success`, `errors_found`, `incomplete` (the engine produced no value for `unmatched` formula cells, so their cached values are stale; treat it as a failure until the cells are named and explained), or `error` (file problem, engine failure, timeout, external links). Only `error` exits non-zero. The calculation runs on a disposable copy and only the computed values are written back, so a human's workbook keeps every style, chart, comment and formula exactly as it was; `values_written` equals `total_formulas` on a `success`. Two engines sit behind it and `engine` names the one used: IronCalc answers in well under a second and knows the 365 functions (`XLOOKUP`, `FILTER`, `LET`, `XNPV`), so it runs first; when it cannot load the file or meets a function it lacks, the script reruns in headless LibreOffice, which takes a few seconds and is why the timeout exists. A `#NAME?` that survives both engines is a misspelt or unsupported function. Give a large model 120 seconds.

**`audit.py <file> [--strict] [--sheet NAME]`**: checks the conventions above; `--help` names every check it runs, the threshold each applies, and what it counts as a structural constant or a grid. Returns `findings` with counts and example cells; `fail` items block delivery, `warn` items are judgement calls. Two findings catch what recalc cannot, because the formulas compute without error: `frozen_row` is a formula row whose text is identical across three or more period columns, which means a formula was written once and repeated instead of copied, so every period reads the first one; `totals` and `totals_formula` read the row label, and a blue cell on a row called `Total debt` is an input, so the label alone never fails it. Three more catch what a clean recalc hides, because a constant and a wrong colour both evaluate fine: `formula_literal` is a formula whose whole body is a typed number, the `=0` left behind when a placeholder was never replaced; `formula_font_color` is a formula whose font leaves the colour code, red included, since red means a link to another workbook and a red formula without one is miscoloured; `formula_number_format` is a formula that evaluates to a number and still carries `General`, a warning on its own and a `fail` under `--strict`, which is the form to run.

Four more read the workbook rather than any single value. `formula_hardcode` is a number typed inside a formula body, the `0.42` in `=B4*0.42` that belongs in a labelled input cell where the reader can find and change it. `formula_family` is the inverse of `frozen_row`: a cell whose formula does not have the shape the rest of its row shares, because a line item is one formula copied across, so an odd cell in the middle of a row was edited by hand. `hidden_sheet` is a sheet the reader never sees: unhide it or delete it. `iterative_calc` is iterative calculation left switched on, which almost always means a circular reference: name the loop that is intended in a note, or clear `wb.calculation.iterate`.

The last five ask whether the file is a model at all. `formula_density` is the share of a calculation sheet's numeric cells that are formulas, reported per sheet with the total formula count in `stats.formula_density`; a low share means what is on screen is a table of results wearing a model's layout, and the fix is to write the formula that produced each typed number. `cover_static` is a workbook of three or more sheets whose first visible sheet holds no formula, a cover typed across instead of linked. `sensitivity_centre` re-checks after recalc what the build script asserts: the centre of a grid is the base case, so it has to reproduce the output the grid varies, and the audit reads which output that is from the `Checks` row naming the pair, else a defined name, else a cover tile. `sensitivity_unverified` is the answer when it could not check at all, which means either that nothing names the output or that the workbook was never recalculated; both leave a grid that reads as a valid table whether or not it is wired to anything. `checks_missing` warns when a grid exists and no `Checks` sheet does.

**`profile.py <file.xlsx|.csv> [--sheet NAME] [--max-rows N]`**: reads a sheet somebody else produced and reports what would have to be cleaned before it can carry a formula. Per sheet it prints the header row it detected, then for every column `name`, `type` (`text`, `number`, `percent`, `currency`, `date`, `boolean`, `identifier`, `mixed`, `empty`), `confidence`, `missing_pct`, `unique` and up to five `samples`, then `issues` in the same `check`/`level`/`count`/`examples` shape `audit.py` returns: `blank_header`, `duplicate_header`, `empty_rows`, `empty_columns`, `duplicate_rows`, `subtotal_rows`, `number_as_text`, `mixed_dates`, `whitespace`, `casing`, `mojibake`, `error_cells`. The header row is scored across the first 30 rows rather than assumed to be the first, so a title and a blank line above the real header do not shift every column and every type by two rows. It reads `.csv` as well, needs nothing but openpyxl, and never writes to the file; `status` is `success` or `error`, and a `--sheet` the workbook does not have is an `error` rather than a success that profiled nothing. `--max-rows` caps the scan at 5,000 rows a sheet, and a sheet cut short reports `truncated: true`, so `data_rows`, the column types and every issue count on it describe the rows that were read rather than the whole file; raise the cap and re-run before proposing a clean-up of a longer upload, or the duplicates and subtotals further down go unreported.

**`insert.py rows|columns|delete-rows|delete-columns <file> --sheet NAME --at N|LETTER [--count K] [--copy-style-from N|LETTER] [--out PATH] [--dry-run]`**: structural edits that keep every reference pointing at the same data. Writes in place unless `--out` is given, computes without writing under `--dry-run`, and reports what moved:

```json
{"status": "success", "sheet": "Model", "operation": "insert_rows", "at": "6", "count": 1,
 "cells_shifted": 32, "formulas_rewritten": 25, "names_rewritten": 1, "charts_rewritten": 1,
 "parts_changed": ["xl/charts/chart1.xml", "xl/workbook.xml", "xl/worksheets/sheet2.xml"],
 "warnings": [{"type": "ref_error", "sheet": "Checks", "cell": "B7", "formula": "Model!#REF!"}],
 "self_check": {"ok": true}}
```

Only `error` (bad arguments, missing file, unknown sheet, a failed self-check) exits non-zero. Each warning is a record with a `type` and the sheet and cell it points at: `ref_error` for a formula that became `#REF!`, `external_reference` for a link into another workbook that was left alone, and records for rules dropped because their whole range was deleted, pivot caches that do not move, and table columns given a placeholder header. `self_check` reopens the written file and confirms every formula still tokenizes, every range attribute still parses, and the `#REF!` count matches the warnings; when it fails nothing is written and the input is untouched. Run with `--dry-run` first on a workbook you did not build, read the warnings, then run for real. Anchors move with the cells they point at, threaded comments included, and an autofilter keeps its filtered columns where the data went. Parts the edit does not need to touch are written back byte for byte, so a diff of the package shows exactly what changed.

**`render.py <file> [--out DIR] [--dpi N]`**: sheets to PNG pages. Look at every page.

## openpyxl Pitfalls

- A formula is a string starting with `=`; openpyxl stores it and computes nothing. Values appear only after `recalc.py`.
- `ws.cell(row, column)` is 1-based. `ws["B2"]` and `ws.cell(row=2, column=2)` are the same cell.
- Column width is in characters, not pixels; 12 fits `(1,240.5)`, 28 fits a label.
- Writing a number as a string (`"1240"`) makes Excel show a green triangle and breaks `SUM`. Convert first.
- `Font`, `PatternFill`, `Border` objects are immutable; build a new one per style rather than mutating.
- Colour strings are `RRGGBB` or `AARRGGBB`; `"#0000FF"` is not accepted.
- `ws.append(row)` writes below the current `max_row`, which includes styled empty rows; prefer explicit coordinates in a model.
- pandas `to_excel` is fine for a raw data sheet, then reopen with openpyxl to style and add formulas. Do not round-trip a styled workbook through pandas.
- Sheet titles are at most 31 characters and cannot contain `[]:*?/\`.

## Deliverable Checklist

- `recalc.py` reports `success`; `audit.py <file> --strict` reports no `fail`.
- Every input blue with a source comment; every computed number a formula with a number format.
- Inputs on their own sheet or block; one formula per line item across periods; totals are `SUM`s.
- The first visible sheet is a cover that reads as a summary, and every number on it is a formula.
- `stats.formula_density` puts every calculation sheet near 1.00; anything lower names numbers that were typed where a formula belonged.
- The `Checks` roll-up reads `OK` with no `FAIL`, and every open `WARN` carries its reason in column D and a sentence in the delivery.
- Rendered pages show no `###`, no raw fractions, no misaligned headers.
- Three numbers spot-checked by hand, and every sensitivity centre equal to the output it varies, asserted in the build script, named in a `Checks` row, and re-checked by `audit.py`.
- The build script is saved next to the workbook and rerunnable.
- The file is at `work/<task>/<descriptive_name>.xlsx` and the reply names it, lists the sheets, and states the key outputs.
