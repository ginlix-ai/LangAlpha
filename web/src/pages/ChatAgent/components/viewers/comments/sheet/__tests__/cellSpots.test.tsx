import { describe, expect, it } from 'vitest';
import { render } from '@testing-library/react';
import CsvViewer from '../../../CsvViewer';
import { joined, partModel } from '../../draftParts';
import { cellSpots, rangeLabel, rangeValues, type SheetSource, type SheetView } from '../cellSpots';

const CSV = 'Metric,Q1,Q2,Q3\nRevenue,312,325,338\nCOGS,214,220,231\nOpex,180,188,196\n';

function viewOf(rows: string[][], sheet: string | null = null, sheets: string[] = []): SheetView {
  const text = (r: number, c: number) => rows[r - 1]?.[c - 1] ?? '';
  return {
    sheet,
    sheets,
    rows: rows.length,
    cols: rows[0].length,
    text,
    isText: (r, c) => text(r, c) !== '' && Number.isNaN(Number(text(r, c))),
  };
}

const ROWS = CSV.trim().split('\n').map((line) => line.split(','));

function setup(view: SheetView = viewOf(ROWS), tabs: Record<string, Element> = {}) {
  const { container } = render(<CsvViewer content={CSV} />);
  const grid = container.querySelector('.excel-table-wrapper')!;
  const holder = document.createElement('div');
  container.append(holder);
  const revealed: [string, unknown][] = [];
  const source: SheetSource = {
    view: () => view,
    tabOf: (name) => tabs[name] ?? null,
    reveal: (name, box) => revealed.push([name, box]),
  };
  const cell = (r: number, c: number) => grid.querySelector(`[data-r="${r}"][data-c="${c}"]`)!;
  const strategy = cellSpots(grid, holder, source);
  return { strategy, spots: strategy.spots!, cell, revealed };
}

describe('a range of cells, as a comment names it', () => {
  it('quotes what it showed row by row, empty cells left out, cut where the anchor would cut it', () => {
    const view = viewOf([['a', '', 'b'], ['', 'c', '']]);
    expect(rangeValues(view, { top: 1, left: 1, bottom: 2, right: 3 })).toBe('a, b, c');
    expect(rangeValues(viewOf([['123456789', '123456789', '123456789']]), { top: 1, left: 1, bottom: 1, right: 3 }, 25)).toBe('123456789, 123456789…');
    // A blank stretch adds nothing to the quote, so the cells read stop too.
    let reads = 0;
    const blank: SheetView = { ...viewOf([['']]), rows: 100_000, cols: 100, text: () => { reads += 1; return ''; } };
    expect(rangeValues(blank, { top: 1, left: 1, bottom: 100_000, right: 100 }, 100)).toBe('');
    expect(reads).toBe(2000);
  });

  it('labels a column by its header and a row by its first words; a block has no one label', () => {
    const view = viewOf(ROWS);
    expect(rangeLabel(view, { top: 2, left: 4, bottom: 4, right: 4 })).toBe('Q3');
    expect(rangeLabel(view, { top: 3, left: 2, bottom: 3, right: 4 })).toBe('COGS');
    expect(rangeLabel(view, { top: 3, left: 4, bottom: 3, right: 4 })).toBe('COGS, Q3');
    expect(rangeLabel(view, { top: 2, left: 2, bottom: 3, right: 3 })).toBeNull();
  });
});

describe('cellSpots', () => {
  it('saves a clicked cell of a CSV as a cells anchor with no sheet, by its A1 name and value', () => {
    const { spots, cell } = setup();
    const spot = spots.spotAt(cell(3, 4))!;
    expect(spots.anchorOf([spot])).toEqual({ kind: 'cells', ranges: [{ range: 'D3', values: '231' }] });
    expect(spots.labelOf(spot)).toBe('D3 · COGS, Q3');
  });

  it('keeps one spot per range and lists several in reading order', () => {
    const { spots, cell } = setup();
    const d4 = spots.spotAt(cell(4, 4))!;
    const b2 = spots.spotAt(cell(2, 2))!;
    expect(spots.spotAt(cell(4, 4))).toBe(d4);
    const parts = joined([d4], [b2], partModel(spots));
    expect(spots.anchorOf(parts)).toEqual({
      kind: 'cells',
      ranges: [{ range: 'B2', values: '312' }, { range: 'D4', values: '196' }],
    });
  });

  it('takes a range as holding the cells and ranges inside it, so a part around another replaces it', () => {
    const { strategy, spots, cell } = setup({ ...viewOf(ROWS), sheet: 'Model', sheets: ['Model'] });
    const column = strategy.find({ kind: 'cells', sheet: 'Model', ranges: [{ range: 'D2:D4' }] });
    const range = column && 'parts' in column ? column.parts[0]![0] : null;
    const inside = spots.spotAt(cell(3, 4))!;
    expect(range).not.toBeNull();
    expect(spots.holds!(range!, inside)).toBe(true);
    expect(spots.holds!(inside, range!)).toBe(false);
    expect(joined([inside], [range!], partModel(spots))).toEqual([range]);
  });

  it('finds a range again by its name on its sheet, cut to what is drawn', () => {
    const { strategy, spots } = setup({ ...viewOf(ROWS), sheet: 'Model', sheets: ['Model'] });
    const found = strategy.find({ kind: 'cells', sheet: 'Model', ranges: [{ range: 'B2:C9' }, { range: 'H1' }] });
    expect(found && 'parts' in found ? found.parts.map((p) => p && spots.anchorOf(p)) : null).toEqual([
      { kind: 'cells', sheet: 'Model', ranges: [{ range: 'B2:C4', values: '312, 325, 214, 220, 180, 188' }] },
      null,
    ]);
  });

  it('parks a comment on another sheet at its tab, and draws that sheet for a scroll to it', () => {
    const tab = document.createElement('button');
    const { strategy, revealed } = setup({ ...viewOf(ROWS), sheet: 'Model', sheets: ['Model', 'Assumptions'] }, { Assumptions: tab });
    const elsewhere = { kind: 'cells' as const, sheet: 'Assumptions', ranges: [{ range: 'B3' }] };
    const found = strategy.find(elsewhere);
    expect(found && 'parked' in found ? found.parked : null).toBe(tab);
    expect(strategy.find({ ...elsewhere, sheet: 'Gone' })).toBeNull();
    if (found && 'parked' in found) found.show?.();
    expect(revealed).toEqual([['Assumptions', { top: 3, left: 2, bottom: 3, right: 2 }]]);
  });
});

describe('a CSV file, as its cells are addressed', () => {
  it('numbers each row as a workbook opening the file would, blank records counted', () => {
    const { container } = render(<CsvViewer content={'Metric,Q1\nRevenue,312\n,\nCOGS,214\n'} />);
    const rowOf = (word: string) => [...container.querySelectorAll('td')].find((td) => td.textContent === word)?.getAttribute('data-r');
    expect(rowOf('Revenue')).toBe('2');
    expect(rowOf('COGS')).toBe('4');
    expect([...container.querySelectorAll('.excel-row-num')].map((n) => n.textContent)).toEqual(['1', '2', '4']);
  });

  it('keeps the grid for an empty file, so the comment layer still covers it', () => {
    const { container } = render(<CsvViewer content={'\n,\n'} />);
    expect(container.querySelector('.excel-table-wrapper.is-empty')?.textContent).toBe('No data');
  });
});
