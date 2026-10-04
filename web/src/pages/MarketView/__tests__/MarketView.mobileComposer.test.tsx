/**
 * The mobile market composer lives in a FAB that unmounts it after every
 * send. A composer that held its own model came back from the next expand on
 * the account default, and the follow-up stored that over the model the Fast
 * thread was started on. These render the page as a phone sees it, with the
 * real composer, FAB and thread model hook, and stub only the chart, header,
 * side panels and data feeds around them.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { act, fireEvent, render, screen } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type React from 'react';
import { ChatInputRegistry, ContextBus } from '@/lib/contextBus';
import { queryKeys } from '@/lib/queryKeys';
import type { Thread } from '@/types/api';

const mocks = vi.hoisted(() => ({
  sendFlashChatMessage: vi.fn(),
  getThread: vi.fn(),
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
vi.mock('../utils/flashWorkspace', () => ({
  getOrFetchFlashWorkspaceId: () => Promise.resolve('flash-ws'),
}));

vi.mock('../utils/api', async (importActual) => ({
  ...(await importActual<Record<string, unknown>>()),
  sendFlashChatMessage: (...args: unknown[]) => mocks.sendFlashChatMessage(...args),
}));
vi.mock('@/pages/ChatAgent/utils/api', async (importActual) => ({
  ...(await importActual<Record<string, unknown>>()),
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
  getThread: mocks.getThread,
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
    modelAccessMap: undefined,
    validModelNames: new Set<string>(),
    catalogModelNames: new Set<string>(),
    metadata: {},
    isLoading: false,
    systemDefaults: { default_model: 'model-default', flash_model: '' },
  }),
}));

vi.mock('@/hooks/useUpdatePreferences', () => ({
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
  ModelTriggerMeasure: () => null,
}));

import MarketView from '../MarketView';

type Emit = (event: Record<string, unknown>) => void;

/** The arg positions `useMarketChat` passes the thread, event callback and model in. */
const THREAD_ID = 1;
const ON_EVENT = 2;
const MODEL = 6;

/** The account defaults: Flash on `flash`, PTC on `ptc`. */
function prefs(flash: string, ptc = 'model-p0') {
  return { model_preference: { preferred_model: ptc, preferred_flash_model: flash } };
}

function ChatPage() {
  const { state } = useLocation();
  return <div data-testid="chat-page">{(state as { model?: string } | null)?.model ?? ''}</div>;
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

async function send(text: string) {
  fireEvent.change(screen.getByRole('textbox'), { target: { value: text } });
  await act(async () => {
    fireEvent.click(screen.getByRole('button', { name: 'Send message' }));
  });
}

/** Starts a Fast thread on model-x, picked over the model-d default. The
 *  stream settles inside the send's act, and the send folds the FAB. */
async function startFlashThreadOnX() {
  expand();
  expect(screen.getByText('pill:model-d')).toBeInTheDocument();
  fireEvent.click(screen.getByText('pick-x'));
  expect(screen.getByText('pill:model-x')).toBeInTheDocument();

  await send('What is AAPL doing?');
  expect(mocks.sendFlashChatMessage).toHaveBeenCalledTimes(1);
  expect(mocks.sendFlashChatMessage.mock.calls[0][THREAD_ID]).toBe('__default__');
  expect(mocks.sendFlashChatMessage.mock.calls[0][MODEL]).toBe('model-x');
  expect(screen.queryByRole('textbox')).toBeNull();
}

describe('the mobile market composer on a Fast thread', () => {
  beforeEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
    Element.prototype.scrollIntoView = vi.fn();
    localStorage.clear();
    sessionStorage.clear();
    mocks.sendFlashChatMessage.mockReset();
    mocks.getThread.mockReset();
    mocks.updateThread.mockReset();

    // The first send's stream names the thread it created; later sends go to it.
    mocks.sendFlashChatMessage.mockImplementation(async (...args: unknown[]) => {
      const emit = args[ON_EVENT] as Emit;
      emit({ event: 'metadata', thread_id: 't-1', run_id: `run-${mocks.sendFlashChatMessage.mock.calls.length}` });
      emit({ event: 'message_chunk', content_type: 'text', content: 'Up 1.2%.' });
    });
    // The server stored the model the first send named.
    mocks.getThread.mockImplementation(async (id: string) => ({ thread_id: id, llm_model: 'model-x' }) as unknown as Thread);

    queryClient = new QueryClient({
      defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
    });
    queryClient.setQueryData(queryKeys.user.preferences(), prefs('model-d'));
  });
  afterEach(() => {
    ContextBus.__resetForTests();
    ChatInputRegistry.__resetForTests();
  });

  it("keeps the thread's model across a fold when the default moves, and sends it", async () => {
    renderPage();
    await startFlashThreadOnX();

    // The Flash default changes elsewhere, for new threads only.
    act(() => {
      queryClient.setQueryData(queryKeys.user.preferences(), prefs('model-e'));
    });

    expand();
    expect(await screen.findByText('pill:model-x')).toBeInTheDocument();
    expect(screen.getByText('This thread uses Model X. New threads start on Model E.')).toBeInTheDocument();

    await send('And MSFT?');
    expect(mocks.sendFlashChatMessage).toHaveBeenCalledTimes(2);
    expect(mocks.sendFlashChatMessage.mock.calls[1][THREAD_ID]).toBe('t-1');
    expect(mocks.sendFlashChatMessage.mock.calls[1][MODEL]).toBe('model-x');
  });

  it("keeps a PTC pick off the Fast thread, and sends it into the new chat", async () => {
    renderPage();
    await startFlashThreadOnX();

    expand();
    expect(await screen.findByText('pill:model-x')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Flash' }));
    // A PTC send opens a new thread, so it starts on the PTC default.
    expect(screen.getByText('pill:model-p0')).toBeInTheDocument();
    fireEvent.click(screen.getByText('pick-y'));
    expect(screen.getByText('pill:model-y')).toBeInTheDocument();

    // Back on Flash, the thread's own model, not the PTC pick or the default.
    fireEvent.click(screen.getByRole('button', { name: 'PTC' }));
    expect(await screen.findByText('pill:model-x')).toBeInTheDocument();

    // And back on PTC, the pick made there.
    fireEvent.click(screen.getByRole('button', { name: 'Flash' }));
    expect(screen.getByText('pill:model-y')).toBeInTheDocument();
    await send('Build me a DCF');

    expect(await screen.findByTestId('chat-page')).toHaveTextContent('model-y');
    expect(mocks.sendFlashChatMessage).toHaveBeenCalledTimes(1);
    expect(mocks.updateThread).not.toHaveBeenCalled();
  });

  it('keeps a PTC pick off the Fast thread when the PTC default is the thread model', async () => {
    // Both modes would seed model-x, so nothing but the mode tells the PTC
    // pick from the thread's model.
    queryClient.setQueryData(queryKeys.user.preferences(), prefs('model-d', 'model-x'));
    renderPage();
    await startFlashThreadOnX();

    expand();
    expect(await screen.findByText('pill:model-x')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Flash' }));
    expect(screen.getByText('pill:model-x')).toBeInTheDocument();
    fireEvent.click(screen.getByText('pick-y'));
    expect(screen.getByText('pill:model-y')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: 'PTC' }));
    expect(await screen.findByText('pill:model-x')).toBeInTheDocument();
    await send('And MSFT?');

    expect(mocks.sendFlashChatMessage).toHaveBeenCalledTimes(2);
    expect(mocks.sendFlashChatMessage.mock.calls[1][THREAD_ID]).toBe('t-1');
    expect(mocks.sendFlashChatMessage.mock.calls[1][MODEL]).toBe('model-x');
    expect(mocks.updateThread).not.toHaveBeenCalled();
  });
});
