/**
 * The block a selected range hands to the composer: displayed values as TSV,
 * then the formulas behind them, so the agent reads both what the model says
 * and how it got there. Agent-facing text, so it stays English.
 */
import {
  boxCells,
  columnName,
  formatLocator,
  formatRange,
  isWholeColumns,
  type CellBox,
} from '@/pages/ChatAgent/utils/a1';
import type { GridCell, SheetData } from './parse';

/** A composer pill is context, not a payload: past this the range is a file. */
const MAX_SNIPPET_CELLS = 200;

const NO_CELL: Pick<GridCell, 'text' | 'formula'> = { text: '' };

function cellAt(sheet: SheetData, row: number, col: number): Pick<GridCell, 'text' | 'formula'> {
  return sheet.rows[row - 1]?.[col - 1] ?? NO_CELL;
}

/** Whole rows are kept whole; a half row of TSV has no readable shape. */
function clampBox(box: CellBox): CellBox {
  if (boxCells(box) <= MAX_SNIPPET_CELLS) return box;
  const width = Math.min(box.right - box.left + 1, MAX_SNIPPET_CELLS);
  const rows = Math.max(1, Math.floor(MAX_SNIPPET_CELLS / width));
  return {
    top: box.top,
    left: box.left,
    bottom: Math.min(box.bottom, box.top + rows - 1),
    right: box.left + width - 1,
  };
}

/**
 * The part of a box the preview parsed, or null when none of it was. Past the
 * parsed extent every cell reads blank, so a snippet there would be empty rows
 * under real addresses.
 */
export function withinSheet(box: CellBox, sheet: SheetData): CellBox | null {
  const rows = sheet.rows.length;
  const cols = sheet.colCount;
  if (box.top > rows || box.left > cols) return null;
  return { top: box.top, left: box.left, bottom: Math.min(box.bottom, rows), right: Math.min(box.right, cols) };
}

/** A field is one cell's worth of text, not a document pasted into one. */
const MAX_FIELD_CHARS = 300;
/** The whole block, in characters. Cells are small on average and huge in the
 *  tail, so the cell cap alone does not bound what reaches the message. */
const MAX_SNIPPET_CHARS = 32 * 1024;

/** A TSV field holds no tab and no newline, or the grid it describes collapses. */
function field(text: string): string {
  const flat = text.replace(/[\t\r\n]+/g, ' ').trim();
  return flat.length > MAX_FIELD_CHARS ? `${flat.slice(0, MAX_FIELD_CHARS)}…` : flat;
}

function extentNote(sheet: SheetData): string {
  return `rows 1-${sheet.rows.length}, columns A-${columnName(sheet.colCount)}`;
}

export function buildRangeSnippet(sheet: SheetData, box: CellBox): string {
  const full = formatLocator(sheet.name, box);
  const inside = withinSheet(box, sheet);
  if (!inside) return `${full} · no cells inside the parsed sheet (${extentNote(sheet)})`;
  const clipped = boxCells(inside) < boxCells(box);
  const kept = clampBox(inside);

  // Rows and columns come off the bottom and the right until the block fits;
  // the head names what is shown either way.
  const assemble = (rows: number, cols: number): string => {
    const shown: CellBox = { top: kept.top, left: kept.left, bottom: kept.top + rows - 1, right: kept.left + cols - 1 };
    const values: string[] = [];
    const formulas: string[] = [];
    for (let row = shown.top; row <= shown.bottom; row++) {
      const line: string[] = [];
      for (let col = shown.left; col <= shown.right; col++) {
        const cell = cellAt(sheet, row, col);
        line.push(field(cell.text));
        if (cell.formula) formulas.push(`${columnName(col)}${row}\t${field(cell.formula)}`);
      }
      values.push(line.join('\t'));
    }
    let head = `${full} · values (TSV)`;
    if (clipped) head += `, ${formatRange(inside)} is inside the parsed sheet (${extentNote(sheet)})`;
    if (boxCells(shown) < boxCells(inside)) head += `, first ${boxCells(shown)} of ${boxCells(inside)} cells (${formatRange(shown)})`;
    const blocks = [head, values.join('\n')];
    if (formulas.length) blocks.push('', `${formatLocator(sheet.name, shown)} · formulas`, formulas.join('\n'));
    return blocks.join('\n');
  };

  let rows = kept.bottom - kept.top + 1;
  let cols = kept.right - kept.left + 1;
  let out = assemble(rows, cols);
  while (out.length > MAX_SNIPPET_CHARS && (rows > 1 || cols > 1)) {
    if (rows > 1) rows--;
    else cols--;
    out = assemble(rows, cols);
  }
  return out;
}

/** Headers are quoted in the hint, and the hint is one line, so this is the cap. */
const MAX_HEADERS = 6;

/**
 * The line a whole row or column hands to the composer. A column is a
 * thousand cells the agent can read itself; what it cannot know is which one
 * the user meant, so the hint names the axis, its header label, and how much
 * of it is filled, and nothing of what fills it. `box` is the locator's box:
 * full-height for columns, full-width for rows.
 */
export function buildWholeSnippet(sheet: SheetData, box: CellBox): string {
  const cols = isWholeColumns(box);
  const extentRows = sheet.rows.length;
  const extentCols = sheet.colCount;
  const lines = cols
    ? range(box.left, Math.min(box.right, extentCols))
    : range(box.top, Math.min(box.bottom, extentRows));
  const across = cols ? range(1, extentRows) : range(1, extentCols);

  let values = 0;
  let formulas = 0;
  for (const line of lines) {
    for (const k of across) {
      const cell = cols ? cellAt(sheet, k, line) : cellAt(sheet, line, k);
      if (cell.formula) formulas++;
      else if (cell.text) values++;
    }
  }

  // The label a reader would use: row 1 of a column, column A of a row.
  const headers = lines
    .map((line) => field(cols ? cellAt(sheet, 1, line).text : cellAt(sheet, line, 1).text))
    .filter(Boolean);
  const shown = headers.slice(0, MAX_HEADERS).map((h) => `"${h}"`);
  if (headers.length > MAX_HEADERS) shown.push('…');

  const noun = cols ? 'column' : 'row';
  const name = lines.length === 1
    ? `${noun} ${cols ? columnName(lines[0]) : lines[0]}`
    : `${noun}s ${formatRange(box)}`;
  const span = cols
    ? `rows 1-${extentRows}`
    : `columns A-${columnName(extentCols)}`;
  const parts = [`${formatLocator(sheet.name, box)} · ${name}`];
  if (shown.length) parts.push(`header ${shown.join(', ')}`);
  parts.push(`${values} value${values !== 1 ? 's' : ''}, ${formulas} formula${formulas !== 1 ? 's' : ''} in ${span}`);
  return parts.join(' · ');
}

function range(from: number, to: number): number[] {
  return Array.from({ length: Math.max(0, to - from + 1) }, (_, i) => from + i);
}
