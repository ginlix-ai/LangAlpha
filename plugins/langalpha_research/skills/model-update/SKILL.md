---
name: model-update
description: "Refresh an existing financial model after a print, a guidance change, a consensus move, a filing, a KPI release or a capital-structure change. Triggers on update the model, roll it forward, the new quarter is out, revise estimates, refresh the price target."
---

# Model Update

Updating is not building. You know what changed in the world and you do not know what the file will do when you write into it, so the order below maps every data point to a treatment before a single cell moves, keeps the file the user gave you byte-identical, and leaves a reader able to see what changed and on whose authority.

Evidence labels, source tiers, staleness, the readiness posture and the intake limits: `.agents/skills/research-conventions/SKILL.md`, read before the first write.

Routing before you start:

- The workbook's own `Checks` sheet fails, or the model looks wrong before you touch it: audit it first with `.agents/skills/check-model/SKILL.md`.
- The refresh has to move operating metrics rather than the three statements: `.agents/skills/dcf-model/references/sector-drivers.md` lists what drives each sector.

## Step 1: Name the trigger and list the fields it touches

One update has one trigger. Name it, hold the document or the tool call that carries it, and write down the fields it reaches before opening the workbook.

| Trigger | Arrives as | Touches |
|---|---|---|
| Results | a reported quarter or year | historical actuals, the LTM roll, and the forward periods the print reprices |
| Guidance change | a range, a raise, a cut, a withdrawal | the driver rows for the guided periods, and the basis note beside them |
| Consensus move | a revised mean estimate with its vintage | the comparison column, never the model's own drivers |
| Transcript disclosure | a number said on a call | the operating metrics it re-anchors, and any driver built on them |
| Filing | 10-K, 10-Q, 8-K, proxy | restatements, segment re-cuts, share count, the debt schedule, contingencies |
| KPI release | a monthly or quarterly operating metric | the sector KPI rows, and the revenue build above them |
| Capital structure change | a buyback, an issuance, a raise, a repayment | diluted share count, net debt, the equity bridge |
| Market data move | price, rates, FX | the spot anchor, the WACC inputs, and the implied return |

A trigger that is really two, a print that also restates a prior year, is two rows in Step 3 rather than one blurred update.

**Done when** the trigger is named, its source is in hand with an as-of date, and the list of fields it touches exists in writing.

## Step 2: Take a versioned copy

Every edit lands on a copy. The file the user gave you is the thing they can reconcile against, and it stays exactly as it arrived.

```bash
shasum -a 256 "$SRC"                                   # before anything
cp "$SRC" "$WORK_DIR/work/{task}/<name>_v2_$(date +%F).xlsx"
```

Increment the version rather than overwriting a previous update, so a reader can diff two vintages of the same model.

**Done when** the copy exists under the task directory and the source file's hash matches the one taken before any edit.

## Step 3: Map every data point before you write

Read the workbook twice, once for formulas and once with `data_only=True`, per *Editing an Existing Workbook* in `.agents/skills/xlsx/SKILL.md`. Then build the mapping table. It is the reviewable artifact of this skill: the table, not the edit, is what you present first.

| Data point | New value | As-of | Source | Target sheet!cell | What is there now | Treatment |
|---|---|---|---|---|---|---|

`What is there now` is one of declared input, formula, or absent, read from the workbook rather than assumed. `Treatment` comes from this closed set:

| Treatment | Holds when | What happens |
|---|---|---|
| `safe to write` | the target is a declared input cell and the new value is the same quantity on the same basis | overwrite it in the copy and log the write |
| `reference only` | the target holds a formula, or writing would cut a linkage the model depends on | record the figure beside the model and leave the cell computing |
| `no place in the model` | the model carries no line for the item | record it in the unwritten block of the change log and raise it on delivery |
| `needs an assumption` | the model's cell needs something the source does not supply, an allocation, a split, a period conversion | write the chosen value into the declared input cell, label it `assumption` per the evidence rules, and log it as an assumption |
| `requires rebuild` | the change is structural: a segment re-cut, an accounting-basis change, a driver the model does not have | write nothing, and route it per *When to rebuild instead* |

Two rules the table carries with it:

- **A restatement keeps both figures.** When the company restates a prior period, the row records the original and the restated value and the model shows the restated one, so the estimate history stays readable rather than quietly rewritten.
- **Market-sensitive inputs carry an as-of every time.** Price, diluted share count, net debt, consensus, FX and rates each get the date they were captured, in the mapping row and in the cell's provenance comment.

**Done when** every data point from Step 1 has a row, every row carries exactly one of the five treatments, and the table has been presented to the user before the first cell is written. That stop is an intake exception and yields under `.agents/skills/research-conventions/references/intake.md`.

## Step 4: Write the resolved rows and log every one

Write the rows tagged `safe to write` and the rows tagged `needs an assumption`, and only into declared input cells. An assumption row carries its basis in the cell's provenance comment and is logged as an assumption, so the change log tells a sourced write from a chosen one.

Structural edits, a new period column or a new line item, go through `python .agents/skills/xlsx/scripts/insert.py` rather than openpyxl's own row insert, which moves values and leaves formulas, defined names, chart ranges and merges pointing at the cells that used to be there. Its `--help` prints the subcommands and flags. Read the `warnings` it returns for `#REF!` results before continuing.

The workbook gains a `Change Log` sheet, written by the edit script, one row per write:

```
date | sheet!cell | line item | old value | new value | source | as-of | treatment | note
```

Below the written rows, the same sheet carries the unwritten block: every mapping row tagged `reference only`, `no place in the model` or `requires rebuild`, with the reason. A reader who opens only this sheet learns what moved, what did not, and why.

Each written input keeps a provenance comment naming the source and the as-of, replacing the one that was there.

**Done when** every written row has a `Change Log` row saying whether it was sourced or assumed, every logged cell holds the new value, and the unwritten block accounts for every mapping row that was not written.

## Step 5: Rebase the estimates

**Reported to adjusted.** The reported figures and the basis the model runs on are different metrics. Bridge them, one line per adjustment, and say which basis the model uses:

```
Reported operating income | + stock compensation | + restructuring | + acquisition amortisation | = adjusted operating income
```

When the company changes its own definition of the adjusted measure, that change is a finding: name it, show both definitions on the affected period, and treat the periods either side as not comparable until the bridge is restated.

**Estimate change by driver.** Show the walk, not just the new numbers. Each step is a driver, and the steps close on the new estimate:

```
Prior FY26 EPS | volume | price/mix | gross margin | operating expense | share count | tax | = new FY26 EPS
```

Then compare the revised estimates against consensus, with the consensus vintage stated.

**Done when** the bridge's closing figure equals the model's new estimate cell for every period shown, and every adjustment line and driver step names its source or its reason.

## Step 6: Restate the valuation as a delta

A new price target on its own tells the reader nothing about what moved it. Report the change and its drivers:

| | Prior | Updated | Change |
|---|---|---|---|
| Fair value per share | | | |
| Spot price, as of | | | |
| Implied return | | | |

Then decompose the change into estimate revision, discount rate or multiple change, capital structure, and the roll forward of one period, so the components sum to the total change.

The stance the number implies, and the vocabulary available for it, come from `.agents/skills/research-conventions/references/judgment.md`.

**Done when** the decomposition sums to the change in fair value, and the spot price and implied return both carry the same as-of.

## Step 7: Recalculate, audit, and say which

```bash
python .agents/skills/xlsx/scripts/recalc.py <file> 60      # until status is "success"
python .agents/skills/xlsx/scripts/audit.py  <file> --strict # fix every fail
```

Then reopen with `data_only=True` and read the `Checks` roll-up, if the workbook carries one. A cached value shows a passing check on a broken model, which is why the delivery message states plainly whether the file was recalculated or whether displayed values are the ones that were already there.

**Done when** `recalc.py` reports success, `audit.py --strict` reports no `fail`, the `Checks` roll-up reads OK, and the delivery message says the workbook was recalculated.

## Step 8: Deliver

Everything under `$WORK_DIR/work/{task}/`:

- the updated workbook, carrying the `Change Log` sheet,
- the estimate-change summary: what changed, why, whether it is thesis-changing or noise,
- the valuation delta from Step 6.

The message that delivers them carries three things: the readiness posture, read from the table in `.agents/skills/research-conventions/SKILL.md` against the state the update leaves the model in, where an assumption row means a load-bearing input rests on a chosen number and a `requires rebuild` row means the model cannot represent the change at all; the rows that were not written and what they would take; and the monitoring items the update creates, in the table shape `.agents/skills/research-conventions/references/judgment.md` gives.

**Done when** the posture is stated with the specific row responsible for it, and every unwritten mapping row appears in the message.

## When the model cannot be edited safely

Some workbooks cannot take a write without breaking. The tells: the target cells hold formulas rather than inputs, the file drives external links or macros, it carries pivot caches or objects openpyxl drops on save, or it is the user's system of record and they have not asked for it to be changed.

Deliver a control pack instead: the original untouched, plus a companion workbook whose sheets say what they hold, `New Data`, `Bridge`, `Implied Impact`, `Change Log`. The pack stands alone, since a cross-workbook link cannot be resolved: values taken from the original are typed in with a provenance comment naming the sheet and cell they came from, and everything computed inside the pack is a live formula under the conventions in `.agents/skills/xlsx/SKILL.md`.

**Done when** the original's hash is unchanged, and every sheet in the pack names the cells of the original it corresponds to.

## When to rebuild instead

An update assumes the model's shape still fits the company. These changes break that assumption, and updating through them produces a model that ties and misstates:

- the company re-cut its segments, or changed what a segment contains,
- the accounting basis changed: a new standard adopted, a revenue-recognition change, a reporting-currency change,
- a merger, divestiture or spin changed the entity the model describes,
- the driver the update needs does not exist in the model at all.

Say so rather than forcing the write, and route it: `.agents/skills/3-statements/SKILL.md` for the operating model, `.agents/skills/dcf-model/SKILL.md` for the valuation. The mapping table is what carries into the rebuild, since it already says what the new shape has to hold.
