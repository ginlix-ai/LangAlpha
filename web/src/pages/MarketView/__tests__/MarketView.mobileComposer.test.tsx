/**
 * The mobile market composer lives in a FAB that unmounts it after every
 * send and fold, so the page holds the model and Subagents picks and hands
 * them to the chat view with the send. These render the page as a phone sees
 * it, with the real composer, FAB and thread model hook, and stub only the
 * chart, header, side panels and data feeds around them.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render, screen, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type React from 'react';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';
import { requestShowLayers, resetLayerViewForTesting } from '../stores/layerViewStore';
import { chartKeyFor } from '../utils/chartInstanceKey';

const mocks = vi.hoisted(() => ({
  updateThread: vi.fn(),
  selectWorkspace: vi.fn(),
  workspaces: [{ workspace_id: 'ws-1', name: 'Workspace 1' }],
  ws: {
    prices: new Map(),
    connectionStatus: 'connected',
    dataLevel: null,
    ginlixDataEnabled: false,
    subscribe: vi.fn(),
    unsubscribe: vi.fn(),
  },
}));

vi.mock('@/hooks/useIsMobile', () => ({
  useIsMobile: () => true,
  getIsMobileSnapshot: () => true,
}));

// Exits finish at once, so a collapse unmounts the composer in the same act.
vi.mock('@/lib/framer', async (importActual) => ({
  ...(await importActual<Record<string, unknown>>()),
  AnimatePresence: ({ children }: { children?: React.ReactNode }) => <>{children}</>,
}));

// The real FAB opens on framer's tap gesture, which jsdom clicks never fire.
vi.mock('@/components/ui/langalpha-fab', () => ({
  default: ({ onClick }: { onClick: () => void }) => (
    <button type="button" aria-label="Open chat" onClick={onClick} />
  ),
}));

vi.mock('../components/StockHeader', () => ({ default: () => <div data-testid="stock-header" /> }));
// The chart stands in as its symbol, interval and an "ask about this layer" button.
vi.mock('../components/MarketChart', async () => {
  const { useChartComposer } = await import('@/components/chart/chartComposer');
  return {
    default: function Chart({ symbol, interval }: { symbol: string; interval: string }) {
      const composer = useChartComposer();
      return (
        <div data-testid="market-chart" data-symbol={symbol} data-interval={interval}>
          <button
            type="button"
            onClick={() => composer.ask({ symbol, timeframe: interval, layerId: 'a', layerTitle: 'Channel' })}
          >
            ask-layer
          </button>
        </div>
      );
    },
  };
});
vi.mock('../components/MarketChatPanel', () => ({ default: () => <div data-testid="desktop-chat-panel" /> }));
vi.mock('../components/MarketSidebarPanel', () => ({ default: () => <div data-testid="sidebar" /> }));
vi.mock('../components/CompanyOverviewPanel', () => ({ default: () => <div data-testid="overview" /> }));
vi.mock('@/components/ui/mobile-bottom-sheet', () => ({
  MobileBottomSheet: ({ open, children }: { open: boolean; children?: React.ReactNode }) => (open ? <>{children}</> : null),
}));

vi.mock('../contexts/MarketDataWSContext', () => ({
  MarketDataWSProvider: ({ children }: { children?: React.ReactNode }) => <>{children}</>,
  useMarketDataWSContext: () => mocks.ws,
}));
vi.mock('../hooks/useStockData', () => ({
  useStockData: () => ({
    stockInfo: null,
    realTimePrice: null,
    snapshotData: null,
    overviewData: null,
    overviewLoading: false,
    overlayData: null,
    marketStatus: null,
  }),
}));
vi.mock('../hooks/useStockQuoteModel', () => ({ useStockQuoteModel: () => ({}) }));
// The desktop chart's toolbar slots; the page builds them on every layout.
vi.mock('../hooks/useChartLegendSlots', () => ({ useChartLegendSlots: () => ({}) }));
vi.mock('../hooks/useRestoredWorkspace', () => ({
  useRestoredWorkspace: () => ({
    selectedWorkspaceId: 'ws-1',
    pending: false,
    select: mocks.selectWorkspace,
    workspaces: mocks.workspaces,
  }),
}));
vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({
    data: { workspaces: mocks.workspaces, total: 1 },
    isFetchedAfterMount: true,
    isSuccess: true,
  }),
}));

vi.mock('@/pages/ChatAgent/utils/api', async (importActual) => ({
  ...(await importActual<Record<string, unknown>>()),
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getHomeWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'home-ws', status: 'running', is_home: true }),
  updateThread: mocks.updateThread,
}));

// Read from the cache, so a default changed elsewhere reaches every reader
// the way a preferences write does.
vi.mock('@/hooks/usePreferences', async () => {
  const { useQuery } = await import('@tanstack/react-query');
  const { queryKeys: keys } = await import('@/lib/queryKeys');
  return {
    usePreferences: () => {
      const { data } = useQuery({
        queryKey: keys.user.preferences(),
        queryFn: () => null,
        enabled: false,
      });
      return { preferences: data ?? null, isLoading: false, isLoaded: data !== undefined };
    },
  };
});

vi.mock('@/hooks/useAllModels', () => ({
  useAllModels: () => ({
    models: {},
    slotModels: {},
    claudeCodeState: 'off',
    payers: {},
    validModelNames: new Set<string>(),
    catalogModelNames: new Set<string>(),
    metadata: { 'model-code': { provider: 'claude-code', sdk: 'claude-code' } },
    isLoading: false,
    systemDefaults: { default_model: 'model-default', background_model: '' },
    offPlanModels: new Set(),
    platform: { model_tier: 0, byok_providers: [], oauth_providers: [] },
    rawApiResponse: {},
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
  PREFERENCE_MUTATION_KEY: ['user-preferences'],
  useUpdatePreferences: () => ({ mutateAsync: vi.fn(), mutate: vi.fn(), isPending: false }),
}));

// The menu's own rendering is not under test; the buttons reach the same
// onSelectModel the dropdown items call.
vi.mock('@/components/ui/chat-input.modelMenu', () => ({
  ChatInputModelMenu: ({ onSelectModel, selectedModel }: {
    onSelectModel: (m: string) => void;
    selectedModel: string | null;
  }) => (
    <>
      <span>{`pill:${selectedModel}`}</span>
      <button type="button" onClick={() => onSelectModel('model-x')}>pick-x</button>
      <button type="button" onClick={() => onSelectModel('model-y')}>pick-y</button>
    </>
  ),
  ChatInputEffortMenu: () => null,
  ModelTriggerMeasure: () => null,
}));

import MarketView from '../MarketView';

/** The account defaults: the Default model, and the Background model that a
 *  chat never starts on. */
function prefs(defaultModel = 'model-p0', background = 'model-d') {
  return { model_preference: { preferred_model: defaultModel, preferred_background_model: background } };
}

interface ChatState {
  workspaceId?: string;
  model?: string;
  subagentsAllowed?: boolean;
}

function ChatPage() {
  const state = useLocation().state as ChatState | null;
  return (
    <div
      data-testid="chat-page"
      data-workspace={state?.workspaceId}
      data-subagents={String(state?.subagentsAllowed)}
    >
      {state?.model ?? ''}
    </div>
  );
}

let queryClient: QueryClient;

function renderPage() {
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={['/market']}>
        <Routes>
          <Route path="/market" element={<MarketView />} />
          <Route path="/chat/t/:threadId" element={<ChatPage />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function expand() {
  fireEvent.click(screen.getByRole('button', { name: 'Open chat' }));
}

/** A tap outside folds the FAB, unmounting the composer. */
function fold() {
  fireEvent.pointerDown(document.body);
  expect(screen.queryByRole('textbox')).toBeNull();
}

function send(text: string) {
  fireEvent.change(screen.getByRole('textbox'), { target: { value: text } });
  fireEvent.click(screen.getByRole('button', { name: 'Send message' }));
}

describe('the mobile market composer', () => {
  const subagentsPill = () => screen.queryByRole('button', { name: /^Subagents$/ });

  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    localStorage.clear();
    sessionStorage.clear();
    mocks.updateThread.mockReset();
    mocks.selectWorkspace.mockReset();
    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    queryClient.setQueryData(queryKeys.user.preferences(), prefs());
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it('keeps a model pick across a fold and sends it into a new chat in Home', async () => {
    renderPage();
    expand();
    expect(screen.getByRole('button', { name: 'Chief of Staff' })).toBeInTheDocument();
    expect(screen.getByText('pill:model-p0')).toBeInTheDocument();
    fireEvent.click(screen.getByText('pick-x'));
    expect(screen.getByText('pill:model-x')).toBeInTheDocument();

    fold();
    expand();
    expect(screen.getByText('pill:model-x')).toBeInTheDocument();

    send('What is AAPL doing?');
    const chat = await screen.findByTestId('chat-page');
    expect(chat).toHaveTextContent('model-x');
    expect(chat).toHaveAttribute('data-workspace', 'home-ws');
    // A pick rides the send; there is no thread yet to store it on.
    expect(mocks.updateThread).not.toHaveBeenCalled();
  });

  it('sends into the selected workspace once the scope moves to it', async () => {
    renderPage();
    expand();
    fireEvent.click(screen.getByRole('button', { name: 'Chief of Staff' }));
    fireEvent.click(within(screen.getByRole('menu')).getByRole('menuitemradio', { name: 'Workspace 1' }));
    expect(screen.getByRole('button', { name: 'Workspace 1' })).toBeInTheDocument();

    send('Build me a DCF');
    const chat = await screen.findByTestId('chat-page');
    expect(chat).toHaveAttribute('data-workspace', 'ws-1');
    // Both scopes run the Default model.
    expect(chat).toHaveTextContent('model-p0');
  });

  // The server reads a Claude Code Default it does not run for the Chief of
  // Staff as unset there, so that composer starts on the deployment's.
  it("starts a Chief of Staff send on the deployment's Default over a Claude Code one it does not run there", async () => {
    queryClient.setQueryData(queryKeys.user.preferences(), prefs('model-code'));
    queryClient.setQueryData(queryKeys.features.claudeCodeSetup(), {
      state: 'not_connected', on: true, computers: [], saved_token_runs: false, cloud: false, too_small: [],
      chief_of_staff: { kind: 'picked', computer_id: null, name: null, refusal: 'no_own_computer', default_runs: false },
    });
    renderPage();
    expand();
    expect(screen.getByText('pill:model-default')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'Chief of Staff' }));
    fireEvent.click(within(screen.getByRole('menu')).getByRole('menuitemradio', { name: 'Workspace 1' }));
    expect(screen.getByText('pill:model-code')).toBeInTheDocument();
  });

  it('shows the Subagents toggle, keeps a flip across a fold, and sends it into the new chat', async () => {
    queryClient.setQueryData(queryKeys.user.preferences(), {
      ...prefs(),
      other_preference: { subagents_default: true },
    });
    renderPage();
    expand();
    expect(subagentsPill()).toHaveAttribute('aria-pressed', 'true');
    fireEvent.click(subagentsPill()!);
    expect(subagentsPill()).toHaveAttribute('aria-pressed', 'false');

    fold();
    expand();
    expect(subagentsPill()).toHaveAttribute('aria-pressed', 'false');

    send('Build me a DCF');
    expect(await screen.findByTestId('chat-page')).toHaveAttribute('data-subagents', 'false');
  });
});

describe('the chart beside the mobile composer', () => {
  const chart = () => screen.getByTestId('market-chart');

  beforeEach(() => {
    localStorage.clear();
    resetLayerViewForTesting();
    queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    queryClient.setQueryData(queryKeys.user.preferences(), prefs());
  });

  it("turns a chart's ask about a layer into the composer's draft", () => {
    renderPage();
    fireEvent.click(screen.getByRole('button', { name: 'ask-layer' }));
    expect(screen.getByRole('textbox')).toHaveValue('About GOOGL · 1day · Channel: ');
  });

  it("switches to the chart of layers asked to be shown in the chat's workspace", async () => {
    renderPage();
    // The chat runs in Home under the Chief of Staff, once Home is known.
    await vi.waitFor(() => expect(queryClient.getQueryData(queryKeys.workspaces.home())).toBeTruthy());
    act(() => requestShowLayers(chartKeyFor('other-ws', 'AAPL', '1hour')!, ['x']));
    expect(chart()).toHaveAttribute('data-symbol', 'GOOGL');
    act(() => requestShowLayers(chartKeyFor('home-ws', 'NVDA', '1hour')!, ['a']));
    await vi.waitFor(() => expect(chart()).toHaveAttribute('data-symbol', 'NVDA'));
    expect(chart()).toHaveAttribute('data-interval', '1hour');
  });

  it('does not act on a request made before the page opened', async () => {
    requestShowLayers(chartKeyFor('home-ws', 'NVDA', '1hour')!, ['a']);
    await new Promise((resolve) => setTimeout(resolve, 2));
    renderPage();
    await screen.findByTestId('market-chart');
    expect(chart()).toHaveAttribute('data-symbol', 'GOOGL');
  });
});
