import { afterEach, describe, expect, it } from 'vitest';

import { chartSelectionStore, type DraftSelectionInput } from '../../stores/chartSelectionStore';
import { buildChartSelectionSend } from '../selectionSend';

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

describe('buildChartSelectionSend', () => {
  it('returns nothing to send when no selection is confirmed for the chart', () => {
    chartSelectionStore.beginDraft(makeRegionInput()); // pending, never confirmed
    const { contexts, snapshots, outgoingMessage } = buildChartSelectionSend('NVDA', '1day', 'hi');
    expect(contexts).toEqual([]);
    expect(snapshots).toEqual([]);
    expect(outgoingMessage).toBe('hi');
  });

  it('builds a chart_selection context + snapshot for each confirmed selection on the chart', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(id, 'resistance retest');

    const { contexts, snapshots } = buildChartSelectionSend('NVDA', '1day', 'analyze this');
    expect(contexts).toHaveLength(1);
    expect(contexts[0]).toMatchObject({
      type: 'chart_selection',
      symbol: 'NVDA',
      timeframe: '1day',
      selection_type: 'region',
      price_low: 180,
      price_high: 195,
      label: 'resistance retest',
    });
    expect(snapshots).toHaveLength(1);
    expect(snapshots[0]).toMatchObject({ symbol: 'NVDA', timeframe: '1day', comment: 'resistance retest' });
  });

  it('includes the region screenshot as a sibling image context + display attachment when captured', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput({ croppedImage: 'data:image/jpeg;base64,ZZZZ' }));
    chartSelectionStore.confirm(id, '');

    const { contexts, attachments } = buildChartSelectionSend('NVDA', '1day', 'analyze');
    expect(contexts).toHaveLength(2);
    expect(contexts[0]).toMatchObject({ type: 'chart_selection' });
    expect(contexts[1]).toMatchObject({ type: 'image', data: 'data:image/jpeg;base64,ZZZZ' });
    // Display attachment carries the base64 as preview so the live bubble shows a thumbnail.
    expect(attachments).toHaveLength(1);
    expect(attachments[0]).toMatchObject({
      type: 'image',
      preview: 'data:image/jpeg;base64,ZZZZ',
      dataUrl: 'data:image/jpeg;base64,ZZZZ',
    });
  });

  it('emits no display attachment when the region has no cropped screenshot', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(id, '');
    expect(buildChartSelectionSend('NVDA', '1day', 'analyze').attachments).toEqual([]);
  });

  it('drops a selection drawn on a different (symbol, timeframe)', () => {
    const here = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(here, 'here');
    const elsewhere = chartSelectionStore.beginDraft(makeRegionInput({ symbol: 'AAPL', timeframe: '1hour' }));
    chartSelectionStore.confirm(elsewhere, 'there');

    const { contexts, snapshots } = buildChartSelectionSend('NVDA', '1day', '');
    expect(contexts).toHaveLength(1);
    expect(snapshots).toHaveLength(1);
    expect(snapshots[0].comment).toBe('here');
  });

  it('promotes a lone selection note as the message only when the user typed nothing', () => {
    const id = chartSelectionStore.beginDraft(makeRegionInput());
    chartSelectionStore.confirm(id, '  what about this gap?  ');

    expect(buildChartSelectionSend('NVDA', '1day', '').outgoingMessage).toBe('what about this gap?');
    expect(buildChartSelectionSend('NVDA', '1day', 'my own question').outgoingMessage).toBe('my own question');
  });
});
