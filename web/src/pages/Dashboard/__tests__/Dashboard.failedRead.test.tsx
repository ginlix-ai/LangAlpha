import { Suspense, type ReactNode } from 'react';
import { describe, it, expect, vi } from 'vitest';
import { act, render } from '@testing-library/react';
import { QueryClientProvider, QueryClient } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import { queryKeys } from '@/lib/queryKeys';

// The real usePreferences, so the observer's view of a failed read is the one
// the page sees in the app. Everything the page draws is stubbed.
const getPreferences = vi.fn(() => Promise.reject(new Error('503')));

vi.mock('@/hooks/useIsMobile', () => ({ useIsMobile: () => false }));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  PREFERENCE_MUTATION_KEY: ['user-preferences'],
  useUpdatePreferences: () => ({ mutateAsync: () => new Promise(() => {}), isPending: false }),
}));
vi.mock('../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../utils/api')>()),
  getPreferences: () => getPreferences(),
}));
vi.mock('../widgets/framework/DashboardDataContext', () => ({
  DashboardDataProvider: ({ children }: { children: ReactNode }) => children,
  useDashboardContext: () => ({
    portfolio: { editRow: null, editForm: {}, openEdit: vi.fn(), modalOpen: false, setModalOpen: vi.fn() },
    watchlist: { modalOpen: false, setModalOpen: vi.fn(), currentWatchlistId: null },
    modals: {
      selectedNewsId: null,
      selectedMarketInsightId: null,
      closeNews: vi.fn(),
      closeInsight: vi.fn(),
      deleteConfirm: { open: false, title: '', message: '' },
      runDeleteConfirm: vi.fn(),
      cancelDeleteConfirm: vi.fn(),
    },
  }),
}));
vi.mock('@/pages/MarketView/contexts/MarketDataWSContext', () => ({
  MarketDataWSProvider: ({ children }: { children: ReactNode }) => children,
}));
vi.mock('../hooks/useOnboarding', () => ({
  useOnboarding: () => ({ showPersonalizationBanner: false, setShowPersonalizationBanner: vi.fn() }),
  snoozePersonalization: vi.fn(),
}));
vi.mock('../components/DashboardHeader', () => ({ default: () => null }));
vi.mock('../components/ChatInputCard', () => ({ default: () => null }));
vi.mock('../components/NewsDetailModal', () => ({ default: () => null }));
vi.mock('../components/InsightDetailModal', () => ({ default: () => null }));
vi.mock('../components/AddWatchlistItemDialog', () => ({ default: () => null }));
vi.mock('../components/AddPortfolioHoldingDialog', () => ({ default: () => null }));
vi.mock('../widgets/framework/AddWidgetDialog', () => ({ AddWidgetDialog: () => null }));
vi.mock('../widgets/framework/PresetsDialog', () => ({ PresetsDialog: () => null }));
vi.mock('../widgets/framework/DashboardGrid', () => ({
  DashboardGrid: () => <div data-testid="dashboard-grid" />,
}));

import Dashboard from '../Dashboard';

describe('Dashboard with a failing preferences read', () => {
  it('renders the page instead of waiting on read after read', async () => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { getByTestId } = await act(async () => render(
      <QueryClientProvider client={qc}>
        <MemoryRouter>
          <Suspense fallback={<div data-testid="route-loading" />}>
            <Dashboard />
          </Suspense>
        </MemoryRouter>
      </QueryClientProvider>,
    ));
    // Long enough for a loop to show itself: each lap is one failed read.
    await act(async () => {
      await new Promise((r) => setTimeout(r, 300));
    });
    expect(getByTestId('dashboard-grid')).toBeInTheDocument();
    // The suspended read, then the page's own read on mount, and no more.
    expect(getPreferences.mock.calls.length).toBeLessThanOrEqual(2);
  });

  it('keeps the page up while a later read retries', async () => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    const { getByTestId, queryByTestId } = await act(async () => render(
      <QueryClientProvider client={qc}>
        <MemoryRouter>
          <Suspense fallback={<div data-testid="route-loading" />}>
            <Dashboard />
          </Suspense>
        </MemoryRouter>
      </QueryClientProvider>,
    ));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 50));
    });
    expect(getByTestId('dashboard-grid')).toBeInTheDocument();
    // A refetch (as on window focus) that has not landed yet.
    getPreferences.mockImplementation(() => new Promise(() => {}));
    await act(async () => {
      void qc.refetchQueries({ queryKey: queryKeys.user.preferences() });
      await new Promise((r) => setTimeout(r, 50));
    });
    expect(queryByTestId('route-loading')).toBeNull();
    expect(getByTestId('dashboard-grid')).toBeInTheDocument();
  });
});
