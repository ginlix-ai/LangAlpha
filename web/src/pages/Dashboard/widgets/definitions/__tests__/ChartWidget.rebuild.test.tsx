/**
 * The widget's side of the live-bars rebuild contract: a reload a rebuilt
 * series asked for must report back whether it put bars on screen, or the
 * polls hold for good on the blank chart it left. The hook's own retry is
 * pinned in `lib/bars/__tests__/useLiveBars.test.ts`; this pins the report.
 */
import { act, render, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { afterEach, describe, expect, it, vi } from 'vitest';

const live = vi.hoisted(() => ({
  seedMeta: vi.fn(),
  loadFailed: vi.fn(),
  onRebuilt: null as (() => void) | null,
}));

vi.mock('@/lib/bars', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/bars')>();
  return {
    ...actual,
    fetchStockData: vi.fn(),
    useLiveBars: (_symbol: string, _interval: string, opts: { onRebuilt: () => void }) => {
      live.onRebuilt = opts.onRebuilt;
      return { seedMeta: live.seedMeta, loadFailed: live.loadFailed };
    },
  };
});

// jsdom has no canvas: every chart, series and scale call returns another
// inert stub, and `options()` an empty object, the one value the load reads.
vi.mock('lightweight-charts', async (importOriginal) => {
  const actual = await importOriginal<typeof import('lightweight-charts')>();
  const stub = (): object =>
    new Proxy({}, {
      get: (_target, key) => {
        if (typeof key !== 'string' || key === 'then') return undefined;
        return key === 'options' ? () => ({}) : () => stub();
      },
    });
  return { ...actual, createChart: () => stub() };
});

vi.mock('@/pages/MarketView/contexts/MarketDataWSContext', () => ({
  useMarketDataWSContext: () => ({
    prices: new Map(),
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
    ginlixDataEnabled: false,
  }),
}));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light' }),
}));

import { fetchStockData } from '@/lib/bars';
import ChartWidget from '../ChartWidget';

const mockFetch = vi.mocked(fetchStockData);

const BAR = { time: 1_700_000_000, open: 1, high: 2, low: 0.5, close: 1.5, volume: 100 };
const META = { watermark: 1_700_000_000_000, complete: true, marketPhase: 'open', revision: 3 };

describe('ChartWidget reload after a server-side rebuild', () => {
  afterEach(() => {
    vi.clearAllMocks();
    live.onRebuilt = null;
  });

  it('reports a reload that came back empty, so the next poll asks for it again', async () => {
    mockFetch.mockResolvedValueOnce({ data: [BAR], meta: META }).mockResolvedValue({ data: [] });
    render(
      <MemoryRouter>
        <ChartWidget
          instance={{ id: 'chart-1', type: 'markets.chart', config: { symbol: 'AAPL', interval: '1day', chartType: 'candle' } }}
          updateConfig={vi.fn()}
        />
      </MemoryRouter>,
    );
    await waitFor(() => expect(live.seedMeta).toHaveBeenCalledWith(META));
    expect(live.loadFailed).not.toHaveBeenCalled();

    // A poll found the series rebuilt; the reload is a 200 with no bars, as a
    // provider miss answers. Nothing else would ever send it again.
    act(() => live.onRebuilt?.());
    await waitFor(() => expect(live.loadFailed).toHaveBeenCalledTimes(1));
    expect(live.seedMeta).toHaveBeenCalledTimes(1);
  });
});
