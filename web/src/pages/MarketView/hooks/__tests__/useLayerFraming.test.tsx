import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { act, renderHook } from '@testing-library/react';
import type { ReactNode } from 'react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { requestShowLayers, resetLayerViewForTesting, useShowLayersRequest } from '../../stores/layerViewStore';
import { makeStored } from '../../stores/__tests__/layerFixture';
import { chartKeyFor } from '../../utils/chartInstanceKey';
import { chartLayersKey } from '../../utils/chartLayerCache';
import { layerFrame } from '../../utils/layerFrame';
import { useLayerFraming, type UseLayerFramingArgs } from '../useLayerFraming';

vi.mock('../../utils/layerFrame', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  layerFrame: vi.fn(() => ({ from: 0, to: 10 })),
}));

const KEY = chartKeyFor('ws-1', 'NVDA', '1day')!;
let client: QueryClient;

const wrapper = ({ children }: { children: ReactNode }) => (
  <QueryClientProvider client={client}>{children}</QueryClientProvider>
);

// No chart yet: framing cannot land, so what is left is the read it asks for.
const args = (layers: UseLayerFramingArgs['layers']): UseLayerFramingArgs => ({
  chartRef: { current: null },
  containerRef: { current: null },
  barsRef: { current: [] },
  grid: { bars: [] } as unknown as UseLayerFramingArgs['grid'],
  layers,
  chartKey: KEY,
  symbol: 'NVDA',
  interval: '1day',
  theme: 'light',
  defaultView: undefined,
  enabled: true,
  loading: false,
});

const stale = () => client.getQueryState(chartLayersKey(KEY))?.isInvalidated;

beforeEach(() => {
  client = new QueryClient();
  client.setQueryData(chartLayersKey(KEY), []);
  resetLayerViewForTesting();
});
afterEach(() => client.clear());

describe('a request to show layers this chart does not hold', () => {
  it('reads the chart’s layers again, since a background command may have published them', () => {
    renderHook(() => useLayerFraming(args([])), { wrapper });
    expect(stale()).toBeFalsy();
    act(() => requestShowLayers(KEY, ['from-a-command']));
    expect(stale()).toBe(true);
  });

  it('leaves the cache alone when it already holds every layer named', () => {
    renderHook(() => useLayerFraming(args([makeStored({ id: 'a' })])), { wrapper });
    act(() => requestShowLayers(KEY, ['a']));
    expect(stale()).toBeFalsy();
  });
});

describe('framing a group of layers the chart holds only part of', () => {
  const setRange = vi.fn();
  const chart = { timeScale: () => ({ setVisibleLogicalRange: setRange }), options: () => ({ width: 800 }) };
  const onChart = (layers: UseLayerFramingArgs['layers']): UseLayerFramingArgs => ({
    ...args(layers),
    chartRef: { current: chart } as unknown as UseLayerFramingArgs['chartRef'],
    barsRef: { current: [{}] },
    grid: { bars: [1] } as unknown as UseLayerFramingArgs['grid'],
  });
  const a = makeStored({ id: 'a' });
  const b = makeStored({ id: 'b' });
  let land: () => void;

  beforeEach(() => {
    setRange.mockClear();
    vi.mocked(layerFrame).mockClear();
    vi.spyOn(client, 'invalidateQueries').mockReturnValue(new Promise<void>((resolve) => { land = resolve; }));
  });

  it('waits for the read, then frames the whole group', () => {
    const { rerender } = renderHook((p: UseLayerFramingArgs) => useLayerFraming(p), {
      wrapper, initialProps: onChart([a]),
    });
    act(() => requestShowLayers(KEY, ['a', 'b']));
    expect(setRange).not.toHaveBeenCalled();
    rerender(onChart([a, b]));
    expect(vi.mocked(layerFrame).mock.lastCall?.[0]).toEqual([a.layer, b.layer]);
    expect(setRange).toHaveBeenCalledTimes(1);
    expect(renderHook(() => useShowLayersRequest()).result.current).toBeNull();
  });

  it('frames what the chart holds once the read finds a layer gone', async () => {
    renderHook(() => useLayerFraming(onChart([a])), { wrapper });
    act(() => requestShowLayers(KEY, ['a', 'gone']));
    expect(setRange).not.toHaveBeenCalled();
    await act(async () => land());
    expect(vi.mocked(layerFrame).mock.lastCall?.[0]).toEqual([a.layer]);
    expect(setRange).toHaveBeenCalledTimes(1);
  });
});
