import { afterEach, describe, expect, it } from 'vitest';
import { act, renderHook } from '@testing-library/react';
import { chartSelectionStore } from '@/pages/MarketView/stores/chartSelectionStore';
import { usePanelChartSelections } from '../usePanelChartSelections';

function confirmLevel(): void {
  const id = chartSelectionStore.beginDraft({
    symbol: 'AAPL',
    timeframe: '1day',
    selectionType: 'price_level',
    priceLow: 200,
    priceHigh: 200,
    bars: [],
    barsTruncated: false,
  });
  chartSelectionStore.confirm(id, 'support');
}

afterEach(() => {
  act(() => chartSelectionStore._resetForTesting());
});

describe('usePanelChartSelections', () => {
  it('drops a pick when its thread goes inactive, so the next thread does not send it', () => {
    const a = renderHook(({ active }) => usePanelChartSelections(active), { initialProps: { active: true } });
    act(() => confirmLevel());
    expect(a.result.current.chips).toHaveLength(1);

    a.rerender({ active: false });
    const b = renderHook(() => usePanelChartSelections(true));

    expect(b.result.current.chips).toHaveLength(0);
    expect(b.result.current.takeForSend('hi')).toBeNull();
  });

  it("keeps the active thread's pick when a cached inactive thread unmounts", () => {
    const cached = renderHook(() => usePanelChartSelections(false));
    const active = renderHook(() => usePanelChartSelections(true));
    act(() => confirmLevel());

    cached.unmount();

    expect(active.result.current.chips).toHaveLength(1);
  });
});
