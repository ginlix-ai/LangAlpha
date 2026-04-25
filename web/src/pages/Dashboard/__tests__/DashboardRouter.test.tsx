import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, act } from '@testing-library/react';
import { QueryClientProvider, QueryClient } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import { queryKeys } from '@/lib/queryKeys';

const prefsState: { current: { other_preference?: Record<string, unknown> | null } | null } = {
  current: { other_preference: { theme: 'dark', dashboard: { mode: 'classic', widgets: [], layouts: {} } } },
};
const loadingState = { isLoading: false };
const mockMutate = vi.fn();
let mockIsMobile = false;
const lastDashboardProps: { current: { mode: string; onModeChange: (n: 'classic' | 'custom') => void } | null } = { current: null };

vi.mock('@/hooks/useIsMobile', () => ({
  useIsMobile: () => mockIsMobile,
}));
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: prefsState.current, isLoading: loadingState.isLoading }),
}));
vi.mock('@/hooks/useUpdatePreferences', () => ({
  useUpdatePreferences: () => ({ mutate: mockMutate, isPending: false }),
}));
vi.mock('../Dashboard', () => ({
  __esModule: true,
  default: (props: { layoutToggle?: { mode: string; onModeChange: (n: 'classic' | 'custom') => void } }) => {
    if (props.layoutToggle) lastDashboardProps.current = props.layoutToggle;
    return <div data-testid="classic-dashboard" />;
  },
}));
vi.mock('../DashboardCustom', () => ({
  __esModule: true,
  default: (props: { mode: string; onModeChange: (n: 'classic' | 'custom') => void }) => {
    lastDashboardProps.current = { mode: props.mode, onModeChange: props.onModeChange };
    return <div data-testid="custom-dashboard" />;
  },
}));
vi.mock('../components/NetworkBanner', () => ({
  __esModule: true,
  default: () => <div data-testid="network-banner-stub" />,
}));

import DashboardRouter from '../DashboardRouter';

function renderRouter(queryClient: QueryClient) {
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <DashboardRouter />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function makeClient(): QueryClient {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
  });
  qc.setQueryData(queryKeys.user.preferences(), prefsState.current);
  return qc;
}

describe('DashboardRouter', () => {
  beforeEach(() => {
    mockMutate.mockReset();
    mockIsMobile = false;
    loadingState.isLoading = false;
    lastDashboardProps.current = null;
    prefsState.current = { other_preference: { theme: 'dark', dashboard: { mode: 'classic', widgets: [], layouts: {} } } };
  });
  afterEach(() => vi.restoreAllMocks());

  it('renders Classic dashboard when prefs.mode === classic', () => {
    const { getByTestId } = renderRouter(makeClient());
    expect(getByTestId('classic-dashboard')).toBeInTheDocument();
  });

  it('renders Custom dashboard when prefs.mode === custom', () => {
    prefsState.current = {
      other_preference: { theme: 'dark', dashboard: { mode: 'custom', widgets: [], layouts: {} } },
    };
    const { getByTestId } = renderRouter(makeClient());
    expect(getByTestId('custom-dashboard')).toBeInTheDocument();
  });

  it('forces Classic on mobile regardless of prefs.mode', () => {
    mockIsMobile = true;
    prefsState.current = {
      other_preference: { theme: 'dark', dashboard: { mode: 'custom', widgets: [], layouts: {} } },
    };
    const { getByTestId, queryByTestId } = renderRouter(makeClient());
    expect(getByTestId('classic-dashboard')).toBeInTheDocument();
    expect(queryByTestId('custom-dashboard')).toBeNull();
  });

  it('mounts NetworkBanner above Classic mode', () => {
    const { getByTestId } = renderRouter(makeClient());
    expect(getByTestId('network-banner-stub')).toBeInTheDocument();
    expect(getByTestId('classic-dashboard')).toBeInTheDocument();
  });

  it('mounts NetworkBanner above Custom mode', () => {
    prefsState.current = {
      other_preference: { theme: 'dark', dashboard: { mode: 'custom', widgets: [], layouts: {} } },
    };
    const { getByTestId } = renderRouter(makeClient());
    expect(getByTestId('network-banner-stub')).toBeInTheDocument();
    expect(getByTestId('custom-dashboard')).toBeInTheDocument();
  });

  it('onModeChange uses fresh queryClient cache snapshot, not stale render-time copy', () => {
    const queryClient = makeClient();
    renderRouter(queryClient);
    expect(lastDashboardProps.current).not.toBeNull();
    // Simulate a cross-tab edit: cache gets new theme + new dashboard mode.
    act(() => {
      queryClient.setQueryData(queryKeys.user.preferences(), {
        other_preference: {
          theme: 'light',
          dashboard: { mode: 'classic', widgets: [{ id: 'w1', type: 'native.chart', config: {} }], layouts: { lg: [] } },
        },
      });
    });
    // User clicks mode toggle. Without replay-aware fix, the write would
    // spread the stale render-time `rawOther` and clobber theme=light + the
    // remote-added widget array.
    act(() => {
      lastDashboardProps.current!.onModeChange('custom');
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: {
        theme?: string;
        dashboard?: { mode: string; widgets?: unknown[] };
      };
    };
    expect(payload.other_preference.theme).toBe('light');
    expect(payload.other_preference.dashboard?.mode).toBe('custom');
    // The remote-added widget must survive the mode-toggle write.
    expect(payload.other_preference.dashboard?.widgets?.length).toBe(1);
  });

  it('seeds the morning-brief preset on first flip from empty Classic to Custom', () => {
    const queryClient = makeClient();
    renderRouter(queryClient);
    act(() => {
      lastDashboardProps.current!.onModeChange('custom');
    });
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { dashboard?: { mode: string; widgets?: unknown[] } };
    };
    expect(payload.other_preference.dashboard?.mode).toBe('custom');
    expect((payload.other_preference.dashboard?.widgets?.length ?? 0)).toBeGreaterThan(0);
  });

  it('new-user warm cache: onModeChange writes with seeded other_preference={dashboard:{...}}', () => {
    // Regression: writer used to refuse when both freshOther and fallbackOther
    // were null, so new users' first mode flip reverted on next refetch.
    prefsState.current = { other_preference: null };
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
    });
    queryClient.setQueryData(queryKeys.user.preferences(), { other_preference: null });
    renderRouter(queryClient);
    act(() => {
      lastDashboardProps.current!.onModeChange('custom');
    });
    expect(mockMutate).toHaveBeenCalledTimes(1);
    const payload = mockMutate.mock.calls[0][0] as {
      other_preference: { dashboard?: { mode: string; widgets?: unknown[] } };
    };
    expect(payload.other_preference.dashboard?.mode).toBe('custom');
    // Morning-brief seed kicks in for the empty-widgets first flip.
    expect((payload.other_preference.dashboard?.widgets?.length ?? 0)).toBeGreaterThan(0);
  });

  it('cold-cache gate: onModeChange is a no-op while preferences are still loading', () => {
    // Regression: a fast click before the GET resolves would PUT
    // { other_preference: { dashboard: {...} } } and clobber sibling
    // server-side keys (theme, locale).
    loadingState.isLoading = true;
    prefsState.current = null;
    const queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false, gcTime: Infinity }, mutations: { retry: false } },
    });
    // Cache empty (no setQueryData) — simulates cold load.
    renderRouter(queryClient);
    expect(lastDashboardProps.current).not.toBeNull();
    act(() => {
      lastDashboardProps.current!.onModeChange('custom');
    });
    expect(mockMutate).not.toHaveBeenCalled();
  });
});
