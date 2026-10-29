import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';

vi.mock('@/lib/bars', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/lib/bars')>();
  return {
    ...actual,
    fetchStockData: vi.fn().mockResolvedValue({
      data: Array.from({ length: 30 }, (_, i) => ({
        timestamp: new Date(Date.UTC(2026, 0, i + 1)).toISOString(),
        open: 100 + i,
        high: 105 + i,
        low: 95 + i,
        close: 100 + i,
        volume: 1000,
      })),
    }),
  };
});

// Each tile's live quote goes through the shared quote batcher, which would
// otherwise send a real snapshot request after the batch window.
vi.mock('@/lib/quotes/snapshotApi', () => ({
  getSnapshotIndexes: vi.fn(),
  getSnapshotStocks: vi.fn(),
}));

// Stub the dashboard context — the widget only reads `watchlist.rows` to fall
// back when no symbols are configured. Mocking sidesteps Supabase / API hooks.
vi.mock('../../framework/DashboardDataContext', () => ({
  useDashboardContext: () => ({
    watchlist: { rows: [] },
  }),
}));

import '../../index'; // populate widget registry
import { fetchStockData } from '@/lib/bars';
import { getWidget } from '../../framework/WidgetRegistry';
import { WidgetFrame } from '../../framework/WidgetFrame';

const mockFetch = fetchStockData as unknown as ReturnType<typeof vi.fn>;

// In its frame, since the widget says its symbol count in the frame's edge.
function renderMiniChartGrid(config: { symbols: string[] }) {
  const def = getWidget('markets.miniChartGrid');
  if (!def) throw new Error('miniChartGrid not registered');
  const Component = def.component;
  const instance = { id: 'mcg-1', type: 'markets.miniChartGrid', config };
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <WidgetFrame
          definition={def}
          instance={instance}
          editMode={false}
          onRemove={vi.fn()}
          onDuplicate={vi.fn()}
          onOpenSettings={vi.fn()}
        >
          <Component instance={instance} updateConfig={vi.fn()} />
        </WidgetFrame>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('MiniChartGridWidget', () => {
  beforeEach(() => {
    mockFetch.mockClear();
  });
  afterEach(() => { vi.resetAllMocks(); vi.restoreAllMocks(); });

  it('caps symbols at 18 even when prefs hold more (regression)', async () => {
    // 25 symbols stored. Render path must clamp to 18 to protect the OHLC
    // backend from a corrupted prefs blob fanning out thousands of requests.
    const symbols = Array.from({ length: 25 }, (_, i) => `SYM${i}`);
    renderMiniChartGrid({ symbols });
    await waitFor(() => expect(mockFetch).toHaveBeenCalled());
    expect(mockFetch.mock.calls.length).toBeLessThanOrEqual(18);
  });

  it('renders the widget chrome with the configured symbol count', async () => {
    renderMiniChartGrid({ symbols: ['NVDA', 'AAPL', 'MSFT'] });
    // The card's edge says "{n} symbols · 30d"; assert n matches config.
    await waitFor(() => {
      expect(screen.getByText(/3\s+symbols/i)).toBeInTheDocument();
    });
  });

  it('definition exposes a Zod schema and the default round-trips', () => {
    const def = getWidget('markets.miniChartGrid')!;
    expect(def.configSchema).toBeDefined();
    const parsed = def.configSchema!.safeParse(def.defaultConfig);
    expect(parsed.success).toBe(true);
  });
});
