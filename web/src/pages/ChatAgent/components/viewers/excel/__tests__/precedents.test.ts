// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { extractRefs, precedentsOf } from '../precedents';

const refs = (formula: string) => extractRefs(formula).map((r) => r.text);

describe('extractRefs', () => {
  it('reads plain references and ranges', () => {
    expect(refs('=B5*(1+B6)')).toEqual(['B5', 'B6']);
    expect(refs('=SUM(B4:D4)')).toEqual(['B4:D4']);
    expect(refs('=$B$5+B$6')).toEqual(['$B$5', 'B$6']);
  });

  it('gives each cited cell one entry', () => {
    expect(refs('=B5/B5-1')).toEqual(['B5']);
  });

  it('is not fooled by a function name that ends in digits', () => {
    expect(refs('=LOG10(B2)')).toEqual(['B2']);
    expect(refs('=SUM(A1:A9)/COUNT(A1:A9)')).toEqual(['A1:A9']);
  });

  it('ignores an address inside quoted text', () => {
    expect(refs('="see A1"&B2')).toEqual(['B2']);
  });

  it('ignores a reference into another workbook, which is not open to resolve', () => {
    expect(refs('=[1]Model!A1+B2')).toEqual(['B2']);
  });

  it('keeps the sheet a reference named', () => {
    expect(extractRefs('=B4*Inputs!C3')).toEqual([
      { sheet: undefined, box: { top: 4, left: 2, bottom: 4, right: 2 }, text: 'B4' },
      { sheet: 'Inputs', box: { top: 3, left: 3, bottom: 3, right: 3 }, text: 'Inputs!C3' },
    ]);
    expect(extractRefs("='My Sheet'!A1")[0].sheet).toBe('My Sheet');
  });
});

describe('precedentsOf', () => {
  it('tints what is on this sheet and names what is not', () => {
    const p = precedentsOf('=B5*(1+B6)-Inputs!C4', 'Model');
    expect(p.local).toEqual([
      { top: 5, left: 2, bottom: 5, right: 2 },
      { top: 6, left: 2, bottom: 6, right: 2 },
    ]);
    expect(p.external).toEqual(['Inputs!C4']);
  });

  it('counts a reference that names this sheet as local', () => {
    expect(precedentsOf('=Model!B6', 'model').external).toEqual([]);
    expect(precedentsOf('=Model!B6', 'model').local).toHaveLength(1);
  });

  it('has nothing to say about a cell with no formula', () => {
    expect(precedentsOf(undefined, 'Model')).toEqual({ local: [], external: [] });
    expect(precedentsOf('', 'Model')).toEqual({ local: [], external: [] });
  });
});
