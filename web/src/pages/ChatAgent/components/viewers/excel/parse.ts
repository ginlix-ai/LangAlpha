/**
 * The workbook, flattened into what the grid draws. Everything expensive,
 * number formatting, merge geometry, style translation, happens once here,
 * off the render path, so selection and arrow keys stay cheap.
 */
import ExcelJS from 'exceljs';
import type { CSSProperties } from 'react';
import { inBox, parseRange, type CellBox } from '@/pages/ChatAgent/utils/a1';
import { countDrawings, type DrawingCount } from './drawings';
import { formatCellValue, type DateSystem } from './numFmt';

/** The window the viewer draws. A workbook past it is still reported in full. */
export const MAX_PREVIEW_ROWS = 500;
export const MAX_PREVIEW_COLS = 200;

// Office's default theme palette, for a colour given as a theme index with no
// resolved ARGB beside it.
const DEFAULT_THEME_COLORS = [
  'FFFFFF', '000000', 'E7E6E6', '44546A',
  '4472C4', 'ED7D31', 'A5A5A5', 'FFC000', '5B9BD5', '70AD47',
];

interface ExcelColor {
  argb?: string;
  theme?: number;
  tint?: number;
}

export interface GridCell {
  /** What the grid draws. Empty string for an empty cell. */
  text: string;
  /** The cell's own formula, shared formulas already resolved to this address. */
  formula?: string;
  /**
   * False on a formula the writer never calculated, which is how openpyxl
   * leaves every formula it writes. Those cells show their formula instead of
   * a value, because there is no value to show and none can be computed here.
   */
  calculated: boolean;
  /** `#REF!` and friends, when the cached result is an error. */
  error?: string;
  numFmt?: string;
  kind: 'empty' | 'value' | 'formula';
  /** Text rather than a number: it decides the default alignment. */
  isText: boolean;
  hyperlink?: string;
  style: CSSProperties;
  /** Set on the top-left cell of a merge. */
  colSpan?: number;
  rowSpan?: number;
  /** Set on a cell a merge covers; the grid skips it and selects the master. */
  master?: { row: number; col: number };
}

export interface SheetData {
  name: string;
  /** `rows[row - 1][col - 1]`, dense over the preview window. */
  rows: GridCell[][];
  colCount: number;
  totalRows: number;
  totalCols: number;
  /** Formulas carrying no cached value, across the whole preview window. */
  uncalculated: number;
  /** On the sheet but not in the grid, which draws cells only. */
  charts: number;
  pictures: number;
}

const EMPTY_CELL: GridCell = { text: '', calculated: true, kind: 'empty', isText: false, style: {} };
const NO_DRAWINGS: DrawingCount = { charts: 0, pictures: 0 };

/** A formula as the bar and the grid print it; ExcelJS stores it without the `=`. */
export function withEquals(formula: string): string {
  return `=${formula.replace(/^=/, '')}`;
}

function applyTint(hex: string, tint: number): string {
  if (!tint) return hex;
  const channel = (slice: string) => {
    const v = parseInt(slice, 16);
    const mixed = tint > 0 ? v + (255 - v) * tint : v * (1 + tint);
    return Math.max(0, Math.min(255, Math.round(mixed))).toString(16).padStart(2, '0');
  };
  return channel(hex.slice(0, 2)) + channel(hex.slice(2, 4)) + channel(hex.slice(4, 6));
}

function resolveColor(color: ExcelColor | undefined | null): string | null {
  if (!color) return null;
  if (color.argb) return '#' + (color.argb.length === 8 ? color.argb.slice(2) : color.argb);
  if (color.theme != null) {
    const base = DEFAULT_THEME_COLORS[color.theme] || '000000';
    return '#' + (color.tint ? applyTint(base, color.tint) : base);
  }
  return null;
}

function getCellStyle(cell: ExcelJS.Cell): CSSProperties {
  const style: CSSProperties = {};

  const fill = cell.fill as ExcelJS.FillPattern | undefined;
  if (fill?.type === 'pattern' && fill.pattern !== 'none' && fill.fgColor) {
    const bg = resolveColor(fill.fgColor as ExcelColor);
    if (bg) style.backgroundColor = bg;
  }

  const font = cell.font;
  if (font) {
    if (font.bold) style.fontWeight = 'bold';
    if (font.italic) style.fontStyle = 'italic';
    const decorations: string[] = [];
    if (font.underline) decorations.push('underline');
    if (font.strike) decorations.push('line-through');
    if (decorations.length) style.textDecoration = decorations.join(' ');
    const fontColor = resolveColor(font.color as ExcelColor | undefined);
    if (fontColor) style.color = fontColor;
    if (font.size) style.fontSize = `${font.size}pt`;
  }

  const align = cell.alignment;
  if (align) {
    if (align.horizontal) style.textAlign = align.horizontal as CSSProperties['textAlign'];
    if (align.vertical === 'middle') style.verticalAlign = 'middle';
    else if (align.vertical === 'top') style.verticalAlign = 'top';
    if (align.wrapText) style.whiteSpace = 'pre-wrap';
  }

  return style;
}

type FormulaValue = ExcelJS.CellFormulaValue | ExcelJS.CellSharedFormulaValue;

/** The boxed half of `CellValue`: every member that is an object but not a Date. */
type CellObject = Exclude<ExcelJS.CellValue, string | number | boolean | Date | null | undefined>;

function asObject(v: ExcelJS.CellValue): CellObject | null {
  return typeof v === 'object' && v !== null && !(v instanceof Date) ? (v as CellObject) : null;
}

function isRichText(v: ExcelJS.CellValue): v is ExcelJS.CellRichTextValue {
  const o = asObject(v);
  return !!o && Array.isArray((o as Partial<ExcelJS.CellRichTextValue>).richText);
}

function isHyperlink(v: ExcelJS.CellValue): v is ExcelJS.CellHyperlinkValue {
  const o = asObject(v);
  return !!o && typeof (o as Partial<ExcelJS.CellHyperlinkValue>).hyperlink === 'string';
}

function isError(v: ExcelJS.CellValue): v is ExcelJS.CellErrorValue {
  const o = asObject(v);
  return !!o && typeof (o as Partial<ExcelJS.CellErrorValue>).error === 'string';
}

function isFormula(v: ExcelJS.CellValue): v is FormulaValue {
  const o = asObject(v) as Partial<FormulaValue> | null;
  return !!o && (typeof o.formula === 'string' || typeof (o as Partial<ExcelJS.CellSharedFormulaValue>).sharedFormula === 'string');
}

/** The scalar a formatter can work on, or `undefined` when there is none. */
type Scalar = string | number | boolean | Date | null | undefined;

function scalarOf(v: ExcelJS.CellValue): Scalar {
  if (v === null || v === undefined) return v;
  if (v instanceof Date) return v;
  if (typeof v !== 'object') return v;
  if (isRichText(v)) return v.richText.map((r) => r.text).join('');
  if (isHyperlink(v)) return v.text ?? v.hyperlink;
  return undefined;
}

function readCell(cell: ExcelJS.Cell, system: DateSystem, locale: string): GridCell {
  const value = cell.value;
  if (value === null || value === undefined || value === '') {
    const style = getCellStyle(cell);
    return Object.keys(style).length ? { ...EMPTY_CELL, style } : EMPTY_CELL;
  }

  const numFmt = cell.numFmt || undefined;
  const style = getCellStyle(cell);

  if (isFormula(value)) {
    // `cell.formula` is the accessor that slides a shared formula onto this
    // address, so B3 reads `A3*2` rather than the master's `A2*2`.
    const formula = cell.formula || (value as ExcelJS.CellFormulaValue).formula || '';
    // Read through the cell. The value object copies only truthy fields, so a
    // formula that came to 0, FALSE or "" would read as never calculated.
    const result = cell.result as FormulaValue['result'];
    const calculated = result !== undefined;
    const error = isError(result) ? result.error : undefined;
    const text = error ?? (calculated ? formatCellValue(scalarOf(result), numFmt, system, locale) : withEquals(formula));
    return {
      text,
      formula,
      calculated,
      error,
      numFmt,
      kind: 'formula',
      isText: typeof result === 'string' || !calculated,
      style,
    };
  }

  if (isError(value)) {
    return { text: value.error, calculated: true, error: value.error, numFmt, kind: 'value', isText: false, style };
  }

  const scalar = scalarOf(value);
  return {
    text: formatCellValue(scalar, numFmt, system, locale),
    calculated: true,
    numFmt,
    kind: 'value',
    isText: typeof scalar === 'string',
    hyperlink: isHyperlink(value) ? value.hyperlink : undefined,
    style,
  };
}

// ExcelJS 4.4 exposes merges only privately; the public `ws.model` getter
// rebuilds the whole sheet to answer, which on a large workbook costs as much
// as the parse did.
function mergeRanges(ws: ExcelJS.Worksheet): string[] {
  const own = (ws as unknown as { _merges?: Record<string, { range?: string } | undefined> })._merges;
  if (!own || typeof own !== 'object') return [];
  return Object.values(own).map((m) => m?.range).filter((r): r is string => !!r);
}

function readSheet(ws: ExcelJS.Worksheet, system: DateSystem, locale: string, drawings: DrawingCount): SheetData {
  const totalCols = ws.columnCount;
  const totalRows = ws.rowCount;
  const colCount = Math.min(totalCols, MAX_PREVIEW_COLS);
  const rowCount = Math.min(totalRows, MAX_PREVIEW_ROWS);
  const boxes = mergeRanges(ws)
    .map(parseRange)
    .filter((b): b is CellBox => !!b);

  const rows: GridCell[][] = [];
  let uncalculated = 0;

  for (let r = 1; r <= rowCount; r++) {
    const row = ws.getRow(r);
    const cells: GridCell[] = [];
    // Almost every row meets no merge at all, so the per-cell test starts from
    // an empty list rather than walking the whole sheet's merges.
    const rowBoxes = boxes.filter((b) => r >= b.top && r <= b.bottom);
    for (let c = 1; c <= colCount; c++) {
      const box = rowBoxes.find((b) => inBox(b, r, c));
      if (box && (box.top !== r || box.left !== c)) {
        cells.push({ ...EMPTY_CELL, master: { row: box.top, col: box.left } });
        continue;
      }
      const cell = readCell(row.getCell(c), system, locale);
      if (cell.kind === 'formula' && !cell.calculated) uncalculated++;
      if (box) {
        // A merge reaching past the preview window is drawn only as far as the
        // window goes, or the table grows rows that are not there.
        const rowSpan = Math.min(box.bottom, rowCount) - box.top + 1;
        const colSpan = Math.min(box.right, colCount) - box.left + 1;
        cells.push({
          ...cell,
          rowSpan: rowSpan > 1 ? rowSpan : undefined,
          colSpan: colSpan > 1 ? colSpan : undefined,
        });
        continue;
      }
      cells.push(cell);
    }
    rows.push(cells);
  }

  return { name: ws.name, rows, colCount, totalRows, totalCols, uncalculated, ...drawings };
}

export async function parseWorkbook(buffer: ArrayBuffer, locale: string): Promise<SheetData[]> {
  const wb = new ExcelJS.Workbook();
  // The grid draws cells, never charts or pictures, so drawings are not read at
  // all. exceljs matches the `xdr:` prefix literally, and openpyxl, which writes
  // the agent's workbooks, declares that namespace as the default instead: a
  // sheet with any chart or picture parses its drawing to nothing and the load
  // throws for the whole file.
  // Skipping the part and the sheet's pointer to it keeps both halves away.
  const reader = wb.xlsx as unknown as { _processDrawingEntry: () => Promise<void> };
  reader._processDrawingEntry = async () => {};
  const [, drawings] = await Promise.all([
    wb.xlsx.load(buffer, { ignoreNodes: ['drawing'] }),
    countDrawings(buffer),
  ]);
  const system: DateSystem = { date1904: wb.properties?.date1904 === true };
  return wb.worksheets.map((ws) => readSheet(ws, system, locale, drawings.get(ws.id) ?? NO_DRAWINGS));
}
