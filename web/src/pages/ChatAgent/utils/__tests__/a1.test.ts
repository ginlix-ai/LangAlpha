// @vitest-environment node
import { describe, expect, it } from 'vitest';
import {
  boxCells,
  boxOf,
  columnIndex,
  columnName,
  describeLocatorSize,
  formatLocator,
  formatRange,
  inBox,
  isWholeColumns,
  isWholeRows,
  MAX_COL,
  MAX_ROW,
  parseCellRef,
  parseLocator,
  parseRange,
} from '../a1';

describe('column names', () => {
  it('rolls over at Z the way Excel does', () => {
    expect(columnName(1)).toBe('A');
    expect(columnName(26)).toBe('Z');
    expect(columnName(27)).toBe('AA');
    expect(columnName(702)).toBe('ZZ');
    expect(columnName(16384)).toBe('XFD');
  });

  it('round-trips, and refuses a column past the last one', () => {
    expect(columnIndex('A')).toBe(1);
    expect(columnIndex('aa')).toBe(27);
    expect(columnIndex('XFD')).toBe(16384);
    expect(columnIndex('XFE')).toBe(0);
    expect(columnIndex('ZZZZ')).toBe(0);
    expect(columnIndex('')).toBe(0);
  });
});

describe('parseCellRef', () => {
  it('drops the absolute markers a viewer has no use for', () => {
    expect(parseCellRef('B7')).toEqual({ row: 7, col: 2 });
    expect(parseCellRef('$B$7')).toEqual({ row: 7, col: 2 });
    expect(parseCellRef(' d9 ')).toEqual({ row: 9, col: 4 });
  });

  it('rejects anything that is not one address', () => {
    expect(parseCellRef('B0')).toBeNull();
    expect(parseCellRef('B4:D9')).toBeNull();
    expect(parseCellRef('Model!B7')).toBeNull();
    expect(parseCellRef('7B')).toBeNull();
  });
});

describe('ranges', () => {
  it('normalises whichever corners were given', () => {
    expect(parseRange('B4:D9')).toEqual({ top: 4, left: 2, bottom: 9, right: 4 });
    expect(parseRange('D9:B4')).toEqual({ top: 4, left: 2, bottom: 9, right: 4 });
    expect(parseRange('B7')).toEqual({ top: 7, left: 2, bottom: 7, right: 2 });
  });

  it('reads whole columns and rows the way Excel spells them', () => {
    expect(parseRange('B:B')).toEqual({ top: 1, left: 2, bottom: MAX_ROW, right: 2 });
    expect(parseRange('D:B')).toEqual({ top: 1, left: 2, bottom: MAX_ROW, right: 4 });
    expect(parseRange('4:9')).toEqual({ top: 4, left: 1, bottom: 9, right: MAX_COL });
    expect(parseRange('XFE:XFE')).toBeNull();
    expect(parseRange('B:4')).toBeNull();
    expect(isWholeColumns(parseRange('B:D')!)).toBe(true);
    expect(isWholeRows(parseRange('B:D')!)).toBe(false);
    expect(isWholeRows(parseRange('7:7')!)).toBe(true);
  });

  it('writes a whole axis back in the same spelling', () => {
    expect(formatRange(parseRange('B:D')!)).toBe('B:D');
    expect(formatRange(parseRange('4:4')!)).toBe('4:4');
    expect(formatLocator('Model', parseRange('B:B')!)).toBe('Model!B:B');
  });

  it('writes a single cell as one address', () => {
    expect(formatRange({ top: 7, left: 2, bottom: 7, right: 2 })).toBe('B7');
    expect(formatRange({ top: 4, left: 2, bottom: 9, right: 4 })).toBe('B4:D9');
  });

  it('counts and tests membership', () => {
    const box = boxOf({ row: 9, col: 4 }, { row: 4, col: 2 });
    expect(box).toEqual({ top: 4, left: 2, bottom: 9, right: 4 });
    expect(boxCells(box)).toBe(18);
    expect(inBox(box, 4, 2)).toBe(true);
    expect(inBox(box, 9, 4)).toBe(true);
    expect(inBox(box, 3, 2)).toBe(false);
    expect(inBox(box, 5, 5)).toBe(false);
  });
});

describe('locators', () => {
  it('reads a sheet qualifier, quoted or bare', () => {
    expect(parseLocator('Model!B7')).toEqual({ sheet: 'Model', box: { top: 7, left: 2, bottom: 7, right: 2 } });
    expect(parseLocator('Model!B4:D9')?.sheet).toBe('Model');
    expect(parseLocator("'My Sheet'!B7")?.sheet).toBe('My Sheet');
    expect(parseLocator("'Bob''s'!B7")?.sheet).toBe("Bob's");
    expect(parseLocator('B7')).toEqual({ box: { top: 7, left: 2, bottom: 7, right: 2 } });
    expect(parseLocator('Model!nonsense')).toBeNull();
  });

  it('quotes a name Excel would have to quote, including one shaped like an address', () => {
    const box = { top: 4, left: 2, bottom: 9, right: 4 };
    expect(formatLocator('Model', box)).toBe('Model!B4:D9');
    expect(formatLocator('My Sheet', box)).toBe("'My Sheet'!B4:D9");
    expect(formatLocator("Bob's", box)).toBe("'Bob''s'!B4:D9");
    expect(formatLocator('A1', box)).toBe("'A1'!B4:D9");
  });

  it('round-trips a quoted name', () => {
    const box = { top: 7, left: 2, bottom: 7, right: 2 };
    expect(parseLocator(formatLocator("Bob's Model", box))).toEqual({ sheet: "Bob's Model", box });
  });

  it('describes a whole axis by name, since its cell count is the sheet capacity', () => {
    expect(describeLocatorSize('Model!B4:D9')).toBe('18 cells');
    expect(describeLocatorSize('B7')).toBe('1 cell');
    expect(describeLocatorSize('Model!B:B')).toBe('column B');
    expect(describeLocatorSize('Model!B:D')).toBe('columns B:D');
    expect(describeLocatorSize('7:7')).toBe('row 7');
    expect(describeLocatorSize('Model!4:9')).toBe('rows 4:9');
    expect(describeLocatorSize('valuation')).toBe('');
  });
});
