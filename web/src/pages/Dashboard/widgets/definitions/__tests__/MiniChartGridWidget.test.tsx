import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';

vi.mock('@/pages/MarketView/utils/api', () => ({
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
}));

// Stub the dashboard context — the widget only reads `watchlist.rows` to fall
// back when no symbols are configured. Mocking sidesteps Supabase / API hooks.
vi.mock('../../framework/DashboardDataContext', () => ({
  useDashboardContext: () => ({
    watchlist: { rows: [] },
  }),
}));

import '../../index'; // populate widget registry
import { fetchStockData } from '@/pages/MarketView/utils/api';
import { getWidget } from '../../framework/WidgetRegistry';

const mockFetch = fetchStockData as unknown as ReturnType<typeof vi.fn>;

function renderMiniChartGrid(config: { symbols: string[] }) {
  const def = getWidget('markets.miniChartGrid');
  if (!def) throw new Error('miniChartGrid not registered');
  const Component = def.component;
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <Component
          instance={{ id: 'mcg-1', type: 'markets.miniChartGrid', config }}
          updateConfig={vi.fn()}
        />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('MiniChartGridWidget', () => {
  beforeEach(() => {
    mockFetch.mockClear();
  });
  afterEach(() => vi.restoreAllMocks());

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
    // Header summary line shows "{n} symbols · 30d" — assert n matches config.
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
