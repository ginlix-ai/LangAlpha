import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import { act, renderHook } from '@testing-library/react';
import { chartSelectionStore } from '@/pages/MarketView/stores/chartSelectionStore';
import { usePanelChart } from '../usePanelChart';

const noInput = { current: null };

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

describe('usePanelChart', () => {
  it('drops a pick when its thread goes inactive, so the next thread does not send it', () => {
    const a = renderHook(({ active }) => usePanelChart(active, noInput), { initialProps: { active: true } });
    act(() => confirmLevel());
    expect(a.result.current.chips).toHaveLength(1);

    a.rerender({ active: false });
    const b = renderHook(() => usePanelChart(true, noInput));

    expect(b.result.current.chips).toHaveLength(0);
    expect(b.result.current.takeForSend('hi').picked).toBeNull();
  });

  it("keeps the active thread's pick when a cached inactive thread unmounts", () => {
    const cached = renderHook(() => usePanelChart(false, noInput));
    const active = renderHook(() => usePanelChart(true, noInput));
    act(() => confirmLevel());

    cached.unmount();

    expect(active.result.current.chips).toHaveLength(1);
  });

  it("hands a chart's ask to this thread's composer as a pill", () => {
    const addContext = vi.fn();
    const input = { current: { addContext } as unknown as ChatInputHandle };
    const { result } = renderHook(() => usePanelChart(true, input));
    const asked = result.current.composer.ask({ symbol: 'NVDA', timeframe: '1day', layerId: 'a', layerTitle: 'Channel' });
    expect(asked).toBe(true);
    expect(addContext).toHaveBeenCalledWith(expect.objectContaining({ source: 'chart', label: 'NVDA · 1day · Channel' }));
  });
});
