import { Suspense, useSyncExternalStore, type ReactNode } from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render } from '@testing-library/react';
import { QueryClientProvider, QueryClient } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router';
import { queryKeys } from '@/lib/queryKeys';
import type { DashboardPrefs } from '../widgets/types';

const prefsState: { current: { other_preference?: Record<string, unknown> | null } | null } = {
  current: { other_preference: { theme: 'dark' } },
};
const loadingState = { isLoading: false, failed: false };
const mockMutate = vi.fn();
let mockIsMobile = false;
// A subscription, as the real hook is, so a change reaches a page the
// compiler has memoized.
const mobileListeners = new Set<() => void>();
const drawn: {
  grid: DashboardPrefs | null;
  stack: DashboardPrefs | null;
  headerEdit: { editMode: boolean; onEditModeChange: (v: boolean) => void } | undefined | 'unrendered';
  addOpen: boolean;
} = { grid: null, stack: null, headerEdit: 'unrendered', addOpen: false };

vi.mock('@/hooks/useIsMobile', () => ({
  useIsMobile: () =>
    useSyncExternalStore(
      (onChange) => {
        mobileListeners.add(onChange);
        return () => mobileListeners.delete(onChange);
      },
      () => mockIsMobile,
    ),
}));
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({
    preferences: prefsState.current,
    isLoading: loadingState.isLoading,
    isLoaded: !loadingState.isLoading && !loadingState.failed,
  }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  PREFERENCE_MUTATION_KEY: ['user-preferences'],
  useUpdatePreferences: () => ({
    mutateAsync: (...args: unknown[]) => {
      mockMutate(...args);
      return new Promise(() => {});
    },
    isPending: false,
  }),
}));
// A read that never answers, for the cold-cache case.
vi.mock('../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../utils/api')>()),
  getPreferences: () => new Promise(() => {}),
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
vi.mock('../components/DashboardHeader', () => ({
  default: ({ edit }: { edit?: { editMode: boolean; onEditModeChange: (v: boolean) => void } }) => {
    drawn.headerEdit = edit;
    return <div data-testid="dashboard-header" />;
  },
}));
vi.mock('../components/ChatInputCard', () => ({ default: () => null }));
vi.mock('../components/NewsDetailModal', () => ({ default: () => null }));
vi.mock('../components/InsightDetailModal', () => ({ default: () => null }));
vi.mock('../components/AddWatchlistItemDialog', () => ({ default: () => null }));
vi.mock('../components/AddPortfolioHoldingDialog', () => ({ default: () => null }));
vi.mock('../widgets/framework/AddWidgetDialog', () => ({
  AddWidgetDialog: ({ open }: { open: boolean }) => {
    drawn.addOpen = open;
    return null;
  },
}));
vi.mock('../widgets/framework/PresetsDialog', () => ({ PresetsDialog: () => null }));
vi.mock('../widgets/framework/DashboardGrid', () => ({
  DashboardGrid: ({ prefs }: { prefs: DashboardPrefs }) => {
    drawn.grid = prefs;
    return <div data-testid="dashboard-grid" />;
  },
}));
vi.mock('../widgets/framework/DashboardStack', () => ({
  DashboardStack: ({ prefs }: { prefs: DashboardPrefs }) => {
    drawn.stack = prefs;
    return <div data-testid="dashboard-stack" />;
  },
}));

import Dashboard from '../Dashboard';
import { defaultLayout } from '../widgets/presets';
import { DASHBOARD_PREFS_VERSION } from '../widgets/types';

function renderDashboard(queryClient: QueryClient) {
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <Suspense fallback={<div data-testid="route-loading" />}>
          <Dashboard />
        </Suspense>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function makeClient({ primed = true } = {}): QueryClient {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
  if (primed) qc.setQueryData(queryKeys.user.preferences(), prefsState.current);
  return qc;
}

describe('Dashboard', () => {
  beforeEach(() => {
    mockMutate.mockReset();
    mockIsMobile = false;
    loadingState.isLoading = false;
    loadingState.failed = false;
    drawn.grid = null;
    drawn.stack = null;
    drawn.headerEdit = 'unrendered';
    drawn.addOpen = false;
    prefsState.current = { other_preference: { theme: 'dark' } };
  });
  afterEach(() => { vi.restoreAllMocks(); });

  it('draws the default layout on the grid when nothing is saved', () => {
    const { getByTestId, queryByTestId } = renderDashboard(makeClient());
    expect(getByTestId('dashboard-grid')).toBeInTheDocument();
    expect(queryByTestId('dashboard-stack')).toBeNull();
    expect(drawn.grid?.widgets.map((w) => w.id)).toEqual(defaultLayout().widgets.map((w) => w.id));
    expect(drawn.headerEdit).toEqual(expect.objectContaining({ editMode: false }));
    expect(mockMutate).not.toHaveBeenCalled();
  });

  it('draws the saved layout when there is one', () => {
    prefsState.current = {
      other_preference: {
        dashboard: {
          version: DASHBOARD_PREFS_VERSION,
          widgets: [{ id: 'saved-1', type: 'news.feed', config: {} }],
          layouts: {},
        },
      },
    };
    renderDashboard(makeClient());
    expect(drawn.grid?.widgets.map((w) => w.id)).toEqual(['saved-1']);
  });

  it('draws the stack, not the grid, on a phone, with no Edit button', () => {
    mockIsMobile = true;
    const { getByTestId, queryByTestId } = renderDashboard(makeClient());
    expect(getByTestId('dashboard-stack')).toBeInTheDocument();
    expect(queryByTestId('dashboard-grid')).toBeNull();
    expect(drawn.stack?.widgets.map((w) => w.id)).toEqual(defaultLayout().widgets.map((w) => w.id));
    expect(drawn.headerEdit).toBeUndefined();
  });

  it('closes an edit dialog left open when the window narrows to a phone', () => {
    const { getByText } = renderDashboard(makeClient());
    const edit = drawn.headerEdit;
    if (!edit || edit === 'unrendered') throw new Error('no Edit button');
    act(() => edit.onEditModeChange(true));
    fireEvent.click(getByText('Add widget'));
    expect(drawn.addOpen).toBe(true);
    act(() => {
      mockIsMobile = true;
      mobileListeners.forEach((onChange) => onChange());
    });
    expect(drawn.addOpen).toBe(false);
  });

  it('draws the default without an Edit button after a failed read, since no edit may be saved over the saved layout', () => {
    loadingState.failed = true;
    prefsState.current = null;
    const { getByTestId } = renderDashboard(makeClient({ primed: false }));
    expect(getByTestId('dashboard-grid')).toBeInTheDocument();
    expect(drawn.grid?.widgets.map((w) => w.id)).toEqual(defaultLayout().widgets.map((w) => w.id));
    expect(drawn.headerEdit).toBeUndefined();
  });

  it('cold-cache gate: holds the route loader, not a guessed layout, while preferences load', () => {
    // A guessed layout would be drawn in full and then replaced by the saved
    // one, and an edit made on it could PUT a dashboard built from nothing
    // before the GET resolved, wiping the saved layout (the server replaces
    // the dashboard key wholesale). Suspending draws no layout, so neither
    // can happen.
    loadingState.isLoading = true;
    prefsState.current = null;
    const { getByTestId, queryByTestId } = renderDashboard(makeClient({ primed: false }));
    expect(getByTestId('route-loading')).toBeInTheDocument();
    expect(queryByTestId('dashboard-grid')).toBeNull();
    expect(queryByTestId('dashboard-stack')).toBeNull();
    expect(drawn.headerEdit).toBe('unrendered');
    expect(mockMutate).not.toHaveBeenCalled();
  });
});
