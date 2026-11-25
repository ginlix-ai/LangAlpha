import { describe, expect, it, vi } from 'vitest';
import type { ChatInputHandle } from '@/components/ui/chat-input';
import { renderHook } from '@testing-library/react';
import { usePanelChart } from '../usePanelChart';

describe('usePanelChart', () => {
  it("hands a chart's ask to this thread's composer as a pill", () => {
    const addContext = vi.fn();
    const input = { current: { addContext } as unknown as ChatInputHandle };
    const { result } = renderHook(() => usePanelChart(input));
    const asked = result.current.composer.ask({ symbol: 'NVDA', timeframe: '1day', layerId: 'a', layerTitle: 'Channel' });
    expect(asked).toBe(true);
    expect(addContext).toHaveBeenCalledWith(expect.objectContaining({ source: 'chart', label: 'NVDA · 1day · Channel' }));
  });
});
