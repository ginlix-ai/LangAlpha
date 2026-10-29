/**
 * The mobile market composer lives in a FAB that unmounts it after every
 * send and fold, so the page holds the model and Subagents picks and hands
 * them to the chat view with the send. These render the page as a phone sees
 * it, with the real composer, FAB and thread model hook, and stub only the
 * chart, header, side panels and data feeds around them.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { fireEvent, render, screen, within } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type React from 'react';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';

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
    setPreviousClose: vi.fn(),
    setDayOpen: vi.fn(),
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
vi.mock('../components/MarketChart', () => ({ default: () => <div data-testid="market-chart" /> }));
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
vi.mock('../hooks/useChartAnnotationSync', () => ({ useChartAnnotationSync: () => {} }));
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
    payers: {},
    validModelNames: new Set<string>(),
    catalogModelNames: new Set<string>(),
    metadata: {},
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
