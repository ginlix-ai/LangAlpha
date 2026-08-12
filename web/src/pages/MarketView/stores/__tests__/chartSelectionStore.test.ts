import { act, renderHook } from '@testing-library/react';
import { afterEach, describe, expect, it, vi } from 'vitest';

import {
  chartSelectionStore,
  isConfirmedFor,
  useChartSelections,
  toSelectionSnapshot,
  promoteSelectionComment,
  type ChartSelection,
  type DraftSelectionInput,
} from '../chartSelectionStore';

function makeRegionInput(overrides: Partial<DraftSelectionInput> = {}): DraftSelectionInput {
  return {
    symbol: 'NVDA',
    timeframe: '1day',
    selectionType: 'region',
    timeStart: '2024-01-03T00:00:00.000Z',
    timeEnd: '2024-02-15T00:00:00.000Z',
    priceLow: 180,
    priceHigh: 195,
    bars: [],
    barsTruncated: false,
    ...overrides,
  };
}

afterEach(() => {
  chartSelectionStore._resetForTesting();
});

describe('chartSelectionStore', () => {
  it('beginDraft adds a pending selection, opens its editor, and returns the id', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput());
    const all = chartSelectionStore.getAll();
    expect(all).toHaveLength(1);
    expect(all[0].id).toBe(id);
    expect(all[0].status).toBe('pending');
    expect(all[0].comment).toBe('');
    expect(chartSelectionStore.getActiveId()).toBe(id);
  });

  it('a second beginDraft discards the prior un-confirmed draft but keeps confirmed ones', () => {
    const first = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(first, 'keep me');
    const draft = chartSelectionStore.beginDraft(makeRegionInput({ priceLow: 1, priceHigh: 2 }));
    chartSelectionStore.beginDraft(makeRegionInput({ priceLow: 3, priceHigh: 4 }));
    const all = chartSelectionStore.getAll();
    // confirmed first + the newest draft; the middle un-confirmed draft was dropped.
    expect(all).toHaveLength(2);
    expect(all.some((s) => s.id === first && s.status === 'confirmed')).toBe(true);
    expect(all.some((s) => s.id === draft)).toBe(false);
  });

  it('confirm saves the note, marks it confirmed, and closes the editor', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(id, '  resistance retest  ');
    const sel = chartSelectionStore.getAll()[0];
    expect(sel.status).toBe('confirmed');
    expect(sel.comment).toBe('  resistance retest  '); // store keeps verbatim; mapper trims
    expect(chartSelectionStore.getActiveId()).toBeNull();
  });

  it('setComment updates a confirmed note', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(id, 'first');
    chartSelectionStore.setComment(id, 'second');
    expect(chartSelectionStore.getAll()[0].comment).toBe('second');
  });

  it('remove drops by id and clears activeId when it was the active one', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput());
    expect(chartSelectionStore.getActiveId()).toBe(id);
    chartSelectionStore.remove(id);
    expect(chartSelectionStore.getAll()).toHaveLength(0);
    expect(chartSelectionStore.getActiveId()).toBeNull();
  });

  it('openEditor / closeEditor toggle the active selection', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(id, '');
    expect(chartSelectionStore.getActiveId()).toBeNull();
    chartSelectionStore.openEditor(id);
    expect(chartSelectionStore.getActiveId()).toBe(id);
    chartSelectionStore.closeEditor();
    expect(chartSelectionStore.getActiveId()).toBeNull();
    chartSelectionStore.openEditor('missing'); // no-op for unknown id
    expect(chartSelectionStore.getActiveId()).toBeNull();
  });

  it('getConfirmedFor returns only confirmed selections matching the chart (case-insensitive symbol)', () => {
    const pending = chartSelectionStore.beginDraft(makeRegionInput());
    const confirmed = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(confirmed, 'note');
    // Another confirmed one on a different instance.
    const other = chartSelectionStore.beginDraft(makeRegionInput({ symbol: 'AAPL' }));
    chartSelectionStore.confirm(other, 'x');

    const got = chartSelectionStore.getConfirmedFor('nvda', '1day');
    expect(got.map((s) => s.id)).toEqual([confirmed]);
    expect(got.some((s) => s.id === pending)).toBe(false);
    expect(chartSelectionStore.getConfirmedFor('NVDA', '1hour')).toHaveLength(0);
  });

  it('clearAll empties selections and the active editor', () => {
    chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.clearAll();
    expect(chartSelectionStore.getAll()).toHaveLength(0);
    expect(chartSelectionStore.getActiveId()).toBeNull();
  });

  it('notifies subscribers on mutation', () => {
    const listener = vi.fn();
    const unsub = chartSelectionStore.subscribe(listener);
    chartSelectionStore.beginDraft(makeRegionInput());
    expect(listener).toHaveBeenCalledTimes(1);
    chartSelectionStore.clearAll();
    expect(listener).toHaveBeenCalledTimes(2);
    unsub();
    chartSelectionStore.beginDraft(makeRegionInput());
    expect(listener).toHaveBeenCalledTimes(2);
  });

  it('toSelectionSnapshot carries the card fields, time bounds, bars, and a trimmed note', () => {
    const bars = [
      { time: '2024-01-03T00:00:00.000Z', open: 181, high: 186, low: 180, close: 185, volume: 1000 },
    ];
    const id = chartSelectionStore.beginDraft(makeRegionInput({ bars, barsTruncated: true }));
    chartSelectionStore.confirm(id, '  watch this break  ');
    const sel = chartSelectionStore.getConfirmedFor('NVDA', '1day')[0];
    expect(toSelectionSnapshot(sel)).toEqual({
      selectionType: 'region',
      symbol: 'NVDA',
      timeframe: '1day',
      priceLow: 180,
      priceHigh: 195,
      comment: 'watch this break',
      timeStart: '2024-01-03T00:00:00.000Z',
      timeEnd: '2024-02-15T00:00:00.000Z',
      bars,
      barsTruncated: true,
    });
    // the id never rides along on the snapshot.
    expect(toSelectionSnapshot(sel)).not.toHaveProperty('id');
  });

  it('toSelectionSnapshot omits comment + time bounds for a blank-note price level', () => {
    const blank: ChartSelection = {
      id: 'sel-x', symbol: 'AAPL', timeframe: '1hour', selectionType: 'price_level',
      priceLow: 205, priceHigh: 205, bars: [], barsTruncated: false, comment: '   ',
      status: 'confirmed',
    };
    const snap = toSelectionSnapshot(blank);
    expect(snap).not.toHaveProperty('comment');
    expect(snap).not.toHaveProperty('timeStart');
    expect(snap).not.toHaveProperty('timeEnd');
    expect(snap.selectionType).toBe('price_level');
    expect(snap.priceLow).toBe(205);
    expect(snap.bars).toEqual([]);
    expect(snap.barsTruncated).toBe(false);
  });

  it('promoteSelectionComment promotes only a single selection note', () => {
    const sel = (comment: string): ChartSelection => ({
      id: 'x', symbol: 'NVDA', timeframe: '1day', selectionType: 'region',
      priceLow: 1, priceHigh: 2, bars: [], barsTruncated: false, comment, status: 'confirmed',
    });
    // A non-empty typed message always wins.
    expect(promoteSelectionComment('hello', [sel('note')])).toBe('hello');
    // Blank message + one selection → the trimmed note becomes the message.
    expect(promoteSelectionComment('   ', [sel('  how about this part?  ')])).toBe('how about this part?');
    // Multiple selections → promote nothing (joining notes would be ambiguous);
    // each note still rides as its selection's label.
    expect(promoteSelectionComment('', [sel('A'), sel('B')])).toBe('');
    // Blank message + a single note-less selection → stays blank (cards only).
    expect(promoteSelectionComment('', [sel('   ')])).toBe('');
  });

  it('useChartSelections reflects store changes', () => {
    const { result } = renderHook(() => useChartSelections());
    expect(result.current.selections).toHaveLength(0);
    let id = '';
    act(() => { id = chartSelectionStore.beginDraft(makeRegionInput()); });
    expect(result.current.selections).toHaveLength(1);
    expect(result.current.activeId).toBe(id);
    act(() => chartSelectionStore.confirm(id, 'hi'));
    expect(result.current.selections[0].status).toBe('confirmed');
    expect(result.current.activeId).toBeNull();
    act(() => chartSelectionStore.clearAll());
    expect(result.current.selections).toHaveLength(0);
  });
});

describe('isConfirmedFor', () => {
  const sel = (overrides: Partial<ChartSelection> = {}): ChartSelection => ({
    id: 'x', symbol: 'NVDA', timeframe: '1day', selectionType: 'region',
    priceLow: 1, priceHigh: 2, bars: [], barsTruncated: false, comment: '',
    status: 'confirmed', ...overrides,
  });

  it('matches a confirmed selection on the same chart', () => {
    expect(isConfirmedFor(sel(), 'NVDA', '1day')).toBe(true);
  });

  it('rejects a pending (un-added) selection', () => {
    expect(isConfirmedFor(sel({ status: 'pending' }), 'NVDA', '1day')).toBe(false);
  });

  it('matches symbol case-insensitively (selections store uppercased)', () => {
    expect(isConfirmedFor(sel(), 'nvda', '1day')).toBe(true);
  });

  it('rejects on a different timeframe or symbol', () => {
    expect(isConfirmedFor(sel(), 'NVDA', '1hour')).toBe(false);
    expect(isConfirmedFor(sel(), 'AAPL', '1day')).toBe(false);
  });

  it('rejects a null/undefined symbol (no empty-string match)', () => {
    expect(isConfirmedFor(sel(), null, '1day')).toBe(false);
    expect(isConfirmedFor(sel(), undefined, '1day')).toBe(false);
  });
});
