import { render, screen, fireEvent, act } from '@testing-library/react';
import { MemoryRouter, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// --- Hoisted fixtures referenced inside vi.mock factories ---

// One spy per chat-engine handler so we can assert the panel forwards the SAME
// function the hook returns. The original bug was that the HITL handlers weren't
// forwarded at all (dead Accept/Decline buttons); the parity pass also wires the
// message-action + stop + action-command handlers.
const h = vi.hoisted(() => ({
  handleSendMessage: vi.fn(),
  handleApproveInterrupt: vi.fn(),
  handleRejectInterrupt: vi.fn(),
  handleAnswerQuestion: vi.fn(),
  handleSkipQuestion: vi.fn(),
  handleApproveCreateWorkspace: vi.fn(),
  handleRejectCreateWorkspace: vi.fn(),
  handleApproveStartQuestion: vi.fn(),
  handleRejectStartQuestion: vi.fn(),
  handleApprovePTCAgent: vi.fn(),
  handleRejectPTCAgent: vi.fn(),
  handleApproveSecretaryAction: vi.fn(),
  handleRejectSecretaryAction: vi.fn(),
  handleEditMessage: vi.fn(),
  handleRegenerate: vi.fn(),
  handleRetry: vi.fn(),
  handleThumbUp: vi.fn(),
  handleThumbDown: vi.fn(),
  feedbackByTurn: { 0: { rating: 'thumbs_up' } } as Record<number, unknown>,
  insertNotification: vi.fn(),
  setIsCompacting: vi.fn(),
  stopWorkflow: vi.fn(),
  threadId: 'thread-xyz', // mutated per-test to exercise the new-chat case
  pendingInterrupt: null as unknown, // mutated per-test to exercise input gating
}));

// API spies — compaction calls straight into the ChatAgent api module. (Stop is
// owned by the hook's stopWorkflow, mocked via `h`, since #273 retired the
// soft-interrupt endpoint in favor of a client-side hard cancel.)
const api = vi.hoisted(() => ({
  summarizeThread: vi.fn().mockResolvedValue({ original_message_count: 3 }),
  offloadThread: vi.fn().mockResolvedValue({ offloaded_args: 1, offloaded_reads: 2 }),
}));

// Capture the props MarketChatPanel hands to MessageList + ChatInput.
const ml = vi.hoisted(() => ({
  props: null as Record<string, unknown> | null,
  actions: null as Record<string, unknown> | null,
}));
const ci = vi.hoisted(() => ({ props: null as Record<string, unknown> | null }));

vi.mock('@/pages/ChatAgent/hooks/useChatMessages', () => ({
  useChatMessages: () => ({
    messages: [{ id: 'm1', role: 'assistant' }], // non-empty → MessageList renders
    isLoading: false,
    isLoadingHistory: false,
    messageError: null,
    threadId: h.threadId,
    threadModels: {},
    lastThreadModel: null,
    handleSendMessage: h.handleSendMessage,
    stopWorkflow: h.stopWorkflow,
    getSubagentHistory: vi.fn(),
    handleApproveInterrupt: h.handleApproveInterrupt,
    handleRejectInterrupt: h.handleRejectInterrupt,
    handleAnswerQuestion: h.handleAnswerQuestion,
    handleSkipQuestion: h.handleSkipQuestion,
    handleApproveCreateWorkspace: h.handleApproveCreateWorkspace,
    handleRejectCreateWorkspace: h.handleRejectCreateWorkspace,
    handleApproveStartQuestion: h.handleApproveStartQuestion,
    handleRejectStartQuestion: h.handleRejectStartQuestion,
    handleApprovePTCAgent: h.handleApprovePTCAgent,
    handleRejectPTCAgent: h.handleRejectPTCAgent,
    handleApproveSecretaryAction: h.handleApproveSecretaryAction,
    handleRejectSecretaryAction: h.handleRejectSecretaryAction,
    pendingInterrupt: h.pendingInterrupt,
    pendingRejection: null,
    hasActiveSubagents: false,
    workspaceStarting: false,
    isCompacting: false,
    setIsCompacting: h.setIsCompacting,
    tokenUsage: null,
    insertNotification: h.insertNotification,
    handleEditMessage: h.handleEditMessage,
    handleRegenerate: h.handleRegenerate,
    handleRetry: h.handleRetry,
    handleThumbUp: h.handleThumbUp,
    handleThumbDown: h.handleThumbDown,
    feedbackByTurn: h.feedbackByTurn,
  }),
}));

// The transcript action surface arrives through MessageActionsContext now, so
// the stand-in reads the provided value from inside the provider.
vi.mock('@/pages/ChatAgent/components/MessageList', async () => {
  const { useMessageActions } = await import('@/pages/ChatAgent/components/messageList/MessageActionsContext');
  function MessageListStub(props: Record<string, unknown>) {
    ml.props = props;
    ml.actions = useMessageActions() as unknown as Record<string, unknown>;
    return <div data-testid="message-list" />;
  }
  return { default: MessageListStub };
});

vi.mock('@/components/ui/chat-input', () => ({
  default: (props: Record<string, unknown>) => {
    ci.props = props;
    return <div data-testid="chat-input" />;
  },
}));

vi.mock('@/pages/MarketView/components/MarketChatHistoryButton', () => ({
  default: () => <div data-testid="history-btn" />,
}));

vi.mock('@/pages/ChatAgent/utils/api', async (importActual) => ({
  ...(await importActual<Record<string, unknown>>()),
  getFlashWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'flash-ws' }),
  getPreviewUrl: vi.fn().mockResolvedValue({ url: 'https://signed.example/' }),
  summarizeThread: api.summarizeThread,
  offloadThread: api.offloadThread,
}));

import MarketChatPanel from '../MarketChatPanel';
import { chartSelectionStore } from '../../stores/chartSelectionStore';

type PanelProps = React.ComponentProps<typeof MarketChatPanel>;

const baseProps: PanelProps = {
  symbol: 'AAPL',
  interval: '1day',
  mode: 'ptc',
  onModeChange: vi.fn(),
  workspaces: [{ workspace_id: 'ws-1' }],
  selectedWorkspaceId: 'ws-1',
  onWorkspaceChange: vi.fn(),
  chartImage: null,
  chartImageDesc: null,
  onCaptureChart: vi.fn(),
  onClearChartImage: vi.fn(),
  prefillMessage: '',
  onClearPrefill: vi.fn(),
  quickQueries: [],
  onQuickQuery: vi.fn(),
  onShuffleQueries: vi.fn(),
};

function renderPanel(override: Partial<PanelProps> = {}) {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={['/market']}>
        <Routes>
          <Route path="/market" element={<MarketChatPanel {...baseProps} {...override} />} />
          <Route path="/chat/t/:threadId" element={<div data-testid="chat-page" />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

describe('MarketChatPanel', () => {
  beforeEach(() => {
    h.threadId = 'thread-xyz';
    h.pendingInterrupt = null;
    ml.props = null;
    ml.actions = null;
    ci.props = null;
    localStorage.clear();
  });
  afterEach(() => vi.clearAllMocks());

  it('provides every HITL handler through MessageActionsContext so plan/question cards work', () => {
    renderPanel();
    const a = ml.actions!;
    // The context members are useStableHandler'd (identity must survive every
    // streamed chunk), so assert delegation, not identity.
    const wiring: Array<[string, ReturnType<typeof vi.fn>]> = [
      ['onApprovePlan', h.handleApproveInterrupt],
      ['onRejectPlan', h.handleRejectInterrupt],
      ['onAnswerQuestion', h.handleAnswerQuestion],
      ['onSkipQuestion', h.handleSkipQuestion],
      ['onApproveCreateWorkspace', h.handleApproveCreateWorkspace],
      ['onRejectCreateWorkspace', h.handleRejectCreateWorkspace],
      ['onApproveStartQuestion', h.handleApproveStartQuestion],
      ['onRejectStartQuestion', h.handleRejectStartQuestion],
      ['onApprovePTCAgent', h.handleApprovePTCAgent],
      ['onRejectPTCAgent', h.handleRejectPTCAgent],
      ['onApproveSecretaryAction', h.handleApproveSecretaryAction],
      ['onRejectSecretaryAction', h.handleRejectSecretaryAction],
    ];
    for (const [key, spy] of wiring) {
      expect(typeof a[key]).toBe('function');
      (a[key] as (arg: unknown) => void)('i1');
      expect(spy).toHaveBeenCalledWith('i1');
    }
  });

  it('provides message-action handlers and passes stored feedback as data', () => {
    renderPanel();
    const a = ml.actions!;
    // Thumbs address a backend turn, not a bubble id.
    (a.onThumbUp as (turnIndex: number) => void)(2);
    expect(h.handleThumbUp).toHaveBeenCalledWith(2);
    (a.onThumbDown as (t: number, c: string[], m: string | null, k: boolean) => void)(2, ['wrong'], 'note', true);
    expect(h.handleThumbDown).toHaveBeenCalledWith(2, ['wrong'], 'note', true);
    // Stored ratings ride as a turn-keyed map on MessageList, not a lookup callback.
    expect(ml.props!.feedbackByTurn).toBe(h.feedbackByTurn);
    // Edit/regenerate/retry are thin wrappers (they thread the model picker), so
    // assert they're wired and delegate to the hook.
    expect(typeof a.onEditMessage).toBe('function');
    (a.onEditMessage as (id: string, c: string) => void)('m1', 'edited');
    expect(h.handleEditMessage).toHaveBeenCalledWith('m1', 'edited', undefined);
    (a.onRegenerate as (id: string) => void)('m1');
    expect(h.handleRegenerate).toHaveBeenCalledWith('m1', undefined);
    (a.onRetry as () => void)();
    expect(h.handleRetry).toHaveBeenCalled();
    // PTC mode → no flash deep-link context.
    expect(ml.props!.flashContext).toBeNull();
  });

  it('wires the stop button to the hook hard-cancel (stopWorkflow)', async () => {
    renderPanel();
    expect(typeof ci.props!.onStop).toBe('function');
    // onStop flips `wasStopped` synchronously then fires stopWorkflow — wrap in act.
    await act(async () => { (ci.props!.onStop as () => void)(); });
    expect(h.stopWorkflow).toHaveBeenCalledTimes(1);
  });

  it('disables the input while a plan approval is pending', () => {
    renderPanel();
    expect(ci.props!.disabled).toBe(false);

    h.pendingInterrupt = { interruptId: 'i1' };
    ci.props = null;
    renderPanel();
    expect(ci.props!.disabled).toBe(true);
  });

  it('routes /compact and /offload action commands to the thread', () => {
    renderPanel();
    const onAction = ci.props!.onAction as (cmd: { name: string }) => void;
    onAction({ name: 'compact' });
    expect(api.summarizeThread).toHaveBeenCalledWith('thread-xyz');
    onAction({ name: 'offload' });
    expect(api.offloadThread).toHaveBeenCalledWith('thread-xyz');
  });

  it('forwards typed slash commands as skill + subagent contexts on send', () => {
    renderPanel();
    const onSend = ci.props!.onSend as (
      m: string, plan: boolean, att: unknown[], cmds: unknown[], opts: unknown,
    ) => void;
    onSend('draw a trend line', false, [], [
      { type: 'skill', name: 'deep-research', skillName: 'deep-research' },
      { type: 'subagent', name: 'subagent' },
    ], {});

    expect(h.handleSendMessage).toHaveBeenCalledTimes(1);
    const contexts = h.handleSendMessage.mock.calls[0][2] as Array<Record<string, unknown>>;
    // Chart-annotation skill is always injected; the typed skill rides alongside.
    expect(contexts).toEqual(expect.arrayContaining([
      expect.objectContaining({ type: 'skills', name: 'chart-annotation' }),
      expect.objectContaining({ type: 'skills', name: 'deep-research' }),
      expect.objectContaining({ type: 'directive' }),
    ]));
  });

  it('forwards a confirmed region crop as a display attachment (arg 3) so the bubble shows a thumbnail', () => {
    // baseProps is AAPL/1day; stage a confirmed region with a crop on that chart.
    const id = chartSelectionStore.beginDraft({
      symbol: 'AAPL',
      timeframe: '1day',
      selectionType: 'region',
      timeStart: '2024-01-03T00:00:00.000Z',
      timeEnd: '2024-02-15T00:00:00.000Z',
      priceLow: 180,
      priceHigh: 195,
      bars: [],
      barsTruncated: false,
      croppedImage: 'data:image/jpeg;base64,WIRED',
    });
    chartSelectionStore.confirm(id, '');

    renderPanel();
    const onSend = ci.props!.onSend as (
      m: string, plan: boolean, att: unknown[], cmds: unknown[], opts: unknown,
    ) => void;
    // clearAll() after send notifies the chip subscriber → wrap to flush in act.
    act(() => onSend('analyze', false, [], [], {}));

    expect(h.handleSendMessage).toHaveBeenCalledTimes(1);
    const attachmentMeta = h.handleSendMessage.mock.calls[0][3] as Array<Record<string, unknown>>;
    expect(attachmentMeta).toEqual(expect.arrayContaining([
      expect.objectContaining({
        type: 'image',
        preview: 'data:image/jpeg;base64,WIRED',
        dataUrl: 'data:image/jpeg;base64,WIRED',
      }),
    ]));
    chartSelectionStore._resetForTesting();
  });

  it('does not double-inject chart-annotation when typed explicitly', () => {
    renderPanel();
    const onSend = ci.props!.onSend as (
      m: string, plan: boolean, att: unknown[], cmds: unknown[], opts: unknown,
    ) => void;
    onSend('annotate', false, [], [
      { type: 'skill', name: 'chart-annotation', skillName: 'chart-annotation' },
    ], {});

    const contexts = h.handleSendMessage.mock.calls[0][2] as Array<Record<string, unknown>>;
    const chartCtx = contexts.filter((c) => c.name === 'chart-annotation');
    expect(chartCtx).toHaveLength(1);
  });

  it('shows "Open in Chat" for an active thread and deep-links to /chat/t/{id}', () => {
    renderPanel();
    const btn = screen.getByText('Open in Chat');
    fireEvent.click(btn);
    expect(screen.getByTestId('chat-page')).toBeInTheDocument();
  });

  it('falls back to "Return to Chat" before a thread exists, when arrived from chat', () => {
    h.threadId = '__default__';
    const onReturnToChat = vi.fn();
    renderPanel({ onReturnToChat });

    expect(screen.queryByText('Open in Chat')).not.toBeInTheDocument();
    fireEvent.click(screen.getByText('Return to Chat'));
    expect(onReturnToChat).toHaveBeenCalledTimes(1);
  });

  it('shows no continue button on a fresh chat with no return path', () => {
    h.threadId = '__default__';
    renderPanel();
    expect(screen.queryByText('Open in Chat')).not.toBeInTheDocument();
    expect(screen.queryByText('Return to Chat')).not.toBeInTheDocument();
  });
});
