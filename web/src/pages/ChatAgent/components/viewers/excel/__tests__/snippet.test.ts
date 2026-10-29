// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { buildRangeSnippet, buildWholeSnippet } from '../snippet';
import { columnName, MAX_COL, MAX_ROW, type CellBox } from '@/pages/ChatAgent/utils/a1';
import type { GridCell, SheetData } from '../parse';

const box = (top: number, left: number, bottom: number, right: number): CellBox => ({ top, left, bottom, right });

const BLANK: GridCell = { text: '', calculated: true, kind: 'empty', isText: false, style: {} };

/** A sheet from a sparse map of `A1` addresses, dense over `rows` x `cols`. */
function sheetOf(name: string, grid: Record<string, { text: string; formula?: string }>, rows: number, cols: number): SheetData {
  const data: GridCell[][] = [];
  for (let r = 1; r <= rows; r++) {
    const line: GridCell[] = [];
    for (let c = 1; c <= cols; c++) {
      const cell = grid[`${columnName(c)}${r}`];
      line.push(cell ? { ...BLANK, ...cell, kind: cell.formula ? 'formula' : 'value' } : BLANK);
    }
    data.push(line);
  }
  return { name, rows: data, colCount: cols, totalRows: rows, totalCols: cols, uncalculated: 0, charts: 0, pictures: 0 };
}

describe('buildRangeSnippet', () => {
  const sheet = sheetOf('Model', {
    B4: { text: '12.4' },
    C4: { text: '13.1' },
    B5: { text: '1,624', formula: 'B4*Inputs!C3' },
    C5: { text: '1,755', formula: 'C4*Inputs!C3' },
  }, 5, 3);

  it('writes the displayed values as TSV, then the formulas behind them', () => {
    expect(buildRangeSnippet(sheet, box(4, 2, 5, 3))).toBe(
      [
        'Model!B4:C5 · values (TSV)',
        '12.4\t13.1',
        '1,624\t1,755',
        '',
        'Model!B4:C5 · formulas',
        'B5\tB4*Inputs!C3',
        'C5\tC4*Inputs!C3',
      ].join('\n'),
    );
  });

  it('leaves the formula block out when nothing in the range is computed', () => {
    expect(buildRangeSnippet(sheet, box(4, 2, 4, 3))).toBe('Model!B4:C4 · values (TSV)\n12.4\t13.1');
  });

  it('quotes a sheet name that needs it', () => {
    const out = buildRangeSnippet({ ...sheet, name: 'My Sheet' }, box(4, 2, 4, 2));
    expect(out.startsWith("'My Sheet'!B4 · values (TSV)")).toBe(true);
  });

  it('caps the block at whole rows and says so in its first line', () => {
    // 4 columns x 100 rows = 400 cells; 50 whole rows is what the cap allows.
    const tall = sheetOf('Model', {}, 100, 4);
    const out = buildRangeSnippet(tall, box(1, 1, 100, 4));
    expect(out.split('\n')[0]).toBe('Model!A1:D100 · values (TSV), first 200 of 400 cells (A1:D50)');
    expect(out.split('\n')).toHaveLength(51);
  });

  it('cuts columns too when one row alone is wider than the cap', () => {
    const wide = sheetOf('Model', {}, 9, 500);
    const out = buildRangeSnippet(wide, box(1, 1, 9, 500));
    expect(out.split('\n')[0]).toBe('Model!A1:SF9 · values (TSV), first 200 of 4500 cells (A1:GR1)');
  });

  it('keeps a TSV row on one line whatever the cell held', () => {
    const messy = sheetOf('Model', { A1: { text: 'line one\nline two' }, B1: { text: 'a\tb' } }, 1, 2);
    expect(buildRangeSnippet(messy, box(1, 1, 1, 2)).split('\n')[1]).toBe('line one line two\ta b');
  });

  it('clips a range that reaches past the sheet to the cells it parsed, and says so', () => {
    expect(buildRangeSnippet(sheet, box(5, 3, 6, 4))).toBe(
      'Model!C5:D6 · values (TSV), C5 is inside the parsed sheet (rows 1-5, columns A-C)\n1,755\n\nModel!C5 · formulas\nC5\tC4*Inputs!C3',
    );
  });

  it('has nothing to show for a range wholly past the sheet', () => {
    expect(buildRangeSnippet(sheet, box(900, 2, 902, 3))).toBe('Model!B900:C902 · no cells inside the parsed sheet (rows 1-5, columns A-C)');
  });

  it('cuts a long cell and a long formula to a field, not a document', () => {
    const long = 'x'.repeat(1000);
    const wordy = sheetOf('Model', { A1: { text: long, formula: `CONCAT(${long})` } }, 1, 1);
    const lines = buildRangeSnippet(wordy, box(1, 1, 1, 1)).split('\n');
    expect(lines[1]).toBe(`${'x'.repeat(300)}…`);
    expect(lines[4]).toBe(`A1\t${`CONCAT(${long})`.slice(0, 300)}…`);
  });

  it('drops rows until the block fits its size cap, and reports what is shown', () => {
    // 200 rows of one 300-char cell is ~60 KB; the cell cap alone would keep it all.
    const grid: Record<string, { text: string }> = {};
    for (let r = 1; r <= 200; r++) grid[`A${r}`] = { text: 'y'.repeat(300) };
    const heavy = sheetOf('Model', grid, 200, 1);
    const out = buildRangeSnippet(heavy, box(1, 1, 200, 1));
    expect(out.length).toBeLessThanOrEqual(32 * 1024);
    const shown = out.split('\n').length - 1;
    expect(shown).toBeLessThan(200);
    expect(out.split('\n')[0]).toBe(`Model!A1:A200 · values (TSV), first ${shown} of 200 cells (A1:A${shown})`);
  });
});

describe('buildWholeSnippet', () => {
  const sheet = sheetOf('Model', {
    A1: { text: 'Line item' }, B1: { text: 'FY2025E' }, C1: { text: 'FY2026E' },
    A4: { text: 'Revenue' }, B4: { text: '12.4' }, C4: { text: '13.1' },
    A5: { text: 'Cost' }, B5: { text: '1,624', formula: 'B4*Inputs!C3' }, C5: { text: '1,755', formula: 'C4*Inputs!C3' },
  }, 5, 3);

  it('names a column by its header and says how full it is, never what fills it', () => {
    const out = buildWholeSnippet(sheet, box(1, 2, MAX_ROW, 2));
    expect(out).toBe('Model!B:B · column B · header "FY2025E" · 2 values, 1 formula in rows 1-5');
    expect(out).not.toContain('12.4');
    expect(out.split('\n')).toHaveLength(1);
  });

  it('names a row by its first cell', () => {
    expect(buildWholeSnippet(sheet, box(5, 1, 5, MAX_COL))).toBe(
      'Model!5:5 · row 5 · header "Cost" · 1 value, 2 formulas in columns A-C',
    );
  });

  it('lists every header of a multi-column selection', () => {
    expect(buildWholeSnippet(sheet, box(1, 2, MAX_ROW, 3))).toContain('columns B:C · header "FY2025E", "FY2026E"');
  });

  it('leaves the header out of a column that has none', () => {
    const wider = sheetOf('Model', {}, 5, 4);
    expect(buildWholeSnippet(wider, box(1, 4, MAX_ROW, 4))).toBe('Model!D:D · column D · 0 values, 0 formulas in rows 1-5');
  });
});
