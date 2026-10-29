/**
 * A1 addressing, the vocabulary the grid, the formula bar, the composer and
 * the `#Model!B4:D9` locator all speak, so a range the user selects and a link
 * the agent writes back are the same string read twice. The grammar is written
 * here once: precedent scanning and fragment reading build on these sources.
 */

/** 1-based on both axes, the way Excel addresses its own cells. */
export interface CellRef {
  row: number;
  col: number;
}

/** An inclusive rectangle of cells. */
export interface CellBox {
  top: number;
  left: number;
  bottom: number;
  right: number;
}

export interface Locator {
  /** Absent when the reference named no sheet, which reads as "the open one". */
  sheet?: string;
  box: CellBox;
}

/** XFD, the last column a workbook can hold. */
export const MAX_COL = 16384;
export const MAX_ROW = 1048576;

const A = 'A'.charCodeAt(0);

export function columnName(col: number): string {
  let name = '';
  let n = Math.floor(col);
  while (n > 0) {
    const rem = (n - 1) % 26;
    name = String.fromCharCode(A + rem) + name;
    n = Math.floor((n - 1) / 26);
  }
  return name;
}

/** 0 for anything that is not a column between A and XFD. */
export function columnIndex(name: string): number {
  if (!/^[A-Za-z]{1,3}$/.test(name)) return 0;
  let n = 0;
  for (const ch of name.toUpperCase()) n = n * 26 + (ch.charCodeAt(0) - A + 1);
  return n <= MAX_COL ? n : 0;
}

/** One cell address, absolute markers allowed, as a regex source with no groups. */
export const CELL_SOURCE = '\\$?[A-Za-z]{1,3}\\$?[1-9]\\d{0,6}';
/** A sheet qualifier as Excel writes it: bare identifier, or quoted with doubled apostrophes. */
export const SHEET_SOURCE = "'(?:[^']|'')+'|[A-Za-z_][A-Za-z0-9_.]*";

const CELL_RE = new RegExp(`^${CELL_SOURCE}$`);
const RANGE_RE = new RegExp(`^(${CELL_SOURCE})(?::(${CELL_SOURCE}))?$`);
// Excel's own spelling for whole columns and whole rows: `B:D`, `4:9`.
const COLS_RE = /^\$?([A-Za-z]{1,3}):\$?([A-Za-z]{1,3})$/;
const ROWS_RE = /^\$?([1-9]\d{0,6}):\$?([1-9]\d{0,6})$/;

/** `'Bob''s'` back to `Bob's`; a bare name comes back as it was. */
export function unquoteSheet(sheet: string): string {
  return sheet.startsWith("'") ? sheet.slice(1, -1).replace(/''/g, "'") : sheet;
}

/** A box that runs the full height of the sheet is a column selection. */
export function isWholeColumns(box: CellBox): boolean {
  return box.top === 1 && box.bottom === MAX_ROW;
}

/** A box that runs the full width of the sheet is a row selection. */
export function isWholeRows(box: CellBox): boolean {
  return box.left === 1 && box.right === MAX_COL;
}

/** A cell the grammar already accepted, split into its coordinates. */
function cellOf(text: string): CellRef | null {
  const split = /^([A-Za-z]+)(\d+)$/.exec(text.replace(/\$/g, ''));
  if (!split) return null;
  const col = columnIndex(split[1]);
  const row = Number(split[2]);
  return col && row <= MAX_ROW ? { row, col } : null;
}

/** `B7`, `$B$7`; absolute markers are dropped, a viewer has nothing to fill down. */
export function parseCellRef(text: string): CellRef | null {
  const trimmed = text.trim();
  return CELL_RE.test(trimmed) ? cellOf(trimmed) : null;
}

export function formatCellRef(ref: CellRef): string {
  return columnName(ref.col) + ref.row;
}

export function boxOf(a: CellRef, b: CellRef): CellBox {
  return {
    top: Math.min(a.row, b.row),
    left: Math.min(a.col, b.col),
    bottom: Math.max(a.row, b.row),
    right: Math.max(a.col, b.col),
  };
}

export function boxCells(box: CellBox): number {
  return (box.bottom - box.top + 1) * (box.right - box.left + 1);
}

export function inBox(box: CellBox, row: number, col: number): boolean {
  return row >= box.top && row <= box.bottom && col >= box.left && col <= box.right;
}

/** `B7`, `B4:D9`, `B:D` or `4:9`, in either corner order. */
export function parseRange(text: string): CellBox | null {
  const trimmed = text.trim();
  const cols = COLS_RE.exec(trimmed);
  if (cols) {
    const a = columnIndex(cols[1]);
    const b = columnIndex(cols[2]);
    if (!a || !b) return null;
    return { top: 1, bottom: MAX_ROW, left: Math.min(a, b), right: Math.max(a, b) };
  }
  const rows = ROWS_RE.exec(trimmed);
  if (rows) {
    const a = Number(rows[1]);
    const b = Number(rows[2]);
    if (a > MAX_ROW || b > MAX_ROW) return null;
    return { top: Math.min(a, b), bottom: Math.max(a, b), left: 1, right: MAX_COL };
  }
  const m = RANGE_RE.exec(trimmed);
  if (!m) return null;
  const a = cellOf(m[1]);
  const b = m[2] ? cellOf(m[2]) : a;
  return a && b ? boxOf(a, b) : null;
}

/** A single cell collapses to one address, the way the name box writes it;
 *  a whole column or row is written the way Excel's name box writes those. */
export function formatRange(box: CellBox): string {
  if (isWholeColumns(box)) return `${columnName(box.left)}:${columnName(box.right)}`;
  if (isWholeRows(box)) return `${box.top}:${box.bottom}`;
  const tl = formatCellRef({ row: box.top, col: box.left });
  if (box.top === box.bottom && box.left === box.right) return tl;
  return `${tl}:${formatCellRef({ row: box.bottom, col: box.right })}`;
}

// The qualifier is read loosely here (any run without `!`, `[` or `]`), since
// a link may name a sheet Excel itself would have quoted; `parseRange` is the
// strict half.
const LOCATOR_RE = /^(?:('(?:[^']|'')+'|[^'![\]]+)!)?([^!]+)$/;

/** `Model!B4:D9`, `'My Sheet'!B7`, `B7`. */
export function parseLocator(text: string): Locator | null {
  const m = LOCATOR_RE.exec(text.trim());
  if (!m) return null;
  const box = parseRange(m[2]);
  if (!box) return null;
  if (!m[1]) return { box };
  const sheet = unquoteSheet(m[1]);
  return sheet ? { sheet, box } : { box };
}

/** Bare only when Excel itself would leave it bare: a name that could be read
 *  as a cell address (a sheet called `A1`) has to be quoted or it is one. */
function needsQuotes(sheet: string): boolean {
  return !/^[A-Za-z_][A-Za-z0-9_.]*$/.test(sheet) || CELL_RE.test(sheet);
}

export function formatLocator(sheet: string, box: CellBox): string {
  const name = needsQuotes(sheet) ? `'${sheet.replace(/'/g, "''")}'` : sheet;
  return `${name}!${formatRange(box)}`;
}

/**
 * The size a summary line prints after a locator: `18 cells` for a range, but
 * `column B` or `rows 4:9` for a whole-axis selection, whose cell count is the
 * sheet's capacity and says nothing. Empty when the locator is unreadable.
 */
export function describeLocatorSize(locator: string): string {
  const parsed = parseLocator(locator);
  if (!parsed) return '';
  const { box } = parsed;
  if (isWholeColumns(box)) {
    return box.left === box.right
      ? `column ${columnName(box.left)}`
      : `columns ${columnName(box.left)}:${columnName(box.right)}`;
  }
  if (isWholeRows(box)) {
    return box.top === box.bottom ? `row ${box.top}` : `rows ${box.top}:${box.bottom}`;
  }
  const cells = boxCells(box);
  return `${cells} cell${cells !== 1 ? 's' : ''}`;
}
