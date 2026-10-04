import { render, screen, fireEvent, act } from '@testing-library/react';
import { MemoryRouter, Routes, Route, useLocation } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

// --- Hoisted fixtures referenced inside vi.mock factories ---

// One spy per chat-engine handler so we can assert the panel forwards the SAME
// function the hook returns. The original bug was that the HITL handlers weren't
// forwarded at all (dead Accept/Decline buttons); the parity pass also wires the
// message-action + stop + action-command handlers.
const h = vi.hoisted(() => ({
  messages: [{ id: 'm1', role: 'assistant' }],
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
  handleResumeCreditPause: vi.fn(),
  handleEditMessage: vi.fn(),
  handleRegenerate: vi.fn(),
  handleRetry: vi.fn(),
  handleThumbUp: vi.fn(),
  handleThumbDown: vi.fn(),
  feedbackByTurn: { 0: { rating: 'thumbs_up' } } as Record<number, unknown>,
  insertNotification: vi.fn(),
  setIsCompacting: vi.fn(),
  stopWorkflow: vi.fn(),
  isLoading: false, // set per-test while a turn streams
  threadId: 'thread-xyz', // mutated per-test to exercise the new-chat case
  pendingInterrupt: null as unknown, // mutated per-test to exercise input gating
  preferences: null as unknown, // the user's preferences, set per-test
}));

// API spies — compaction calls straight into the ChatAgent api module. (Stop is
// owned by the hook's stopWorkflow, mocked via `h`, since #273 retired the
// soft-interrupt endpoint in favor of a client-side hard cancel.)
const api = vi.hoisted(() => ({
  summarizeThread: vi.fn().mockResolvedValue({ original_message_count: 3 }),
  offloadThread: vi.fn().mockResolvedValue({ offloaded_args: 1, offloaded_reads: 2 }),
  getWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'ws-1', name: 'Workspace 1' }),
}));

// Capture the props MarketChatPanel hands to MessageList + ChatInput.
const ml = vi.hoisted(() => ({
  props: null as Record<string, unknown> | null,
  actions: null as Record<string, unknown> | null,
}));
const ci = vi.hoisted(() => ({ props: null as Record<string, unknown> | null }));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: h.preferences, isLoading: false, isLoaded: true }),
}));

vi.mock('@/pages/ChatAgent/hooks/useChatMessages', () => ({
  useChatMessages: () => ({
    messages: h.messages, // non-empty → MessageList renders
    liveMessages: { get: () => h.messages, set: () => {}, subscribe: () => () => {} },
    isLoading: h.isLoading,
    isLoadingHistory: false,
    messageError: null,
    threadId: h.threadId,
    threadModels: {},
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
    handleResumeCreditPause: h.handleResumeCreditPause,
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
    // Bubbles carry the markers a turn-end landing looks for.
    const messages = (props.messages ?? []) as Array<{ id: string; role: string }>;
    return (
      <div data-testid="message-list">
        {messages.map((m) => (
          <div key={m.id} data-message-id={m.id}>
            {m.role === 'assistant' && <p data-reply-start="" />}
          </div>
        ))}
      </div>
    );
  }
  function LiveMessageListStub({ store, ...props }: { store: { get: () => unknown } } & Record<string, unknown>) {
    return <MessageListStub messages={store.get()} {...props} />;
  }
  return { default: MessageListStub, LiveMessageList: LiveMessageListStub };
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
  getWorkspace: api.getWorkspace,
  getPreviewUrl: vi.fn().mockResolvedValue({ url: 'https://signed.example/' }),
  summarizeThread: api.summarizeThread,
  offloadThread: api.offloadThread,
}));

import MarketChatPanel from '../MarketChatPanel';
import { chartSelectionStore } from '../../stores/chartSelectionStore';
import { userLocalStorage } from '@/lib/userStorage';

type PanelProps = React.ComponentProps<typeof MarketChatPanel>;

const baseProps: PanelProps = {
  symbol: 'AAPL',
  interval: '1day',
  mode: 'ptc',
  onModeChange: vi.fn(),
  workspaces: [{ workspace_id: 'ws-1', name: 'Workspace 1' }],
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

function Probe() {
  return <div data-testid="search">{useLocation().search}</div>;
}

function renderPanel(override: Partial<PanelProps> = {}, entry = '/market') {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const page = (props: Partial<PanelProps>) => (
    <>
      <MarketChatPanel {...baseProps} {...props} />
      <Probe />
    </>
  );
  const view = render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[entry]}>
        <Routes>
          <Route path="/market" element={page(override)} />
          <Route path="/chat/t/:threadId" element={<div data-testid="chat-page" />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
  const rerender = (next: Partial<PanelProps>) => view.rerender(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[entry]}>
        <Routes>
          <Route path="/market" element={page(next)} />
          <Route path="/chat/t/:threadId" element={<div data-testid="chat-page" />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { ...view, rerender };
}

// The transcript's scroll geometry. jsdom has no layout, so the panel's
// scroller reads a 400px viewport over `layout`, and the content's
// ResizeObserver fires when a test grows it.
const observers: { cb: ResizeObserverCallback; targets: Element[] }[] = [];
const scrollTo = vi.fn();

class CapturingResizeObserver {
  targets: Element[] = [];
  constructor(cb: ResizeObserverCallback) {
    observers.push({ cb, targets: this.targets });
  }
  observe(el: Element) {
    this.targets.push(el);
  }
  unobserve() {}
  disconnect() {}
}

function renderTranscript(override: Partial<PanelProps> = {}) {
  const view = renderPanel(override);
  const transcript = screen.getByTestId('message-list').parentElement!;
  const container = transcript.parentElement!;
  const observer = observers.find((o) => o.targets.includes(transcript))!;
  const layout = { top: 0, height: 0 };
  Object.defineProperties(container, {
    scrollTop: { get: () => layout.top, configurable: true },
    scrollHeight: { get: () => layout.height, configurable: true },
    clientHeight: { get: () => 400, configurable: true },
  });
  // A follow moves at once; its scroll event comes with the next frame.
  scrollTo.mockImplementation(({ top: to }: ScrollToOptions) => {
    layout.top = to!;
  });
  const grow = (to: number) => {
    layout.height = to;
    observer.cb([{ contentRect: { height: to } } as ResizeObserverEntry], {} as ResizeObserver);
  };
  const nextFrame = () => fireEvent.scroll(container);
  const userScroll = (to: number) => {
    layout.top = to;
    fireEvent.scroll(container);
  };
  return { ...view, container, layout, grow, nextFrame, userScroll };
}

/** A settled thread opened at its end, which the reader scrolls up to reread. */
function scrolledUpReader() {
  const kit = renderTranscript();
  kit.grow(1000);
  kit.nextFrame();
  kit.userScroll(200);
  scrollTo.mockClear();
  return kit;
}

describe('MarketChatPanel', () => {
  beforeEach(() => {
    h.threadId = 'thread-xyz';
    h.pendingInterrupt = null;
    h.isLoading = false;
    h.messages = [{ id: 'm1', role: 'assistant' }];
    h.preferences = null;
    ml.props = null;
    ml.actions = null;
    ci.props = null;
    localStorage.clear();
  });
  afterEach(() => vi.clearAllMocks());

  describe('transcript scrolling', () => {
    const originalScrollTo = HTMLElement.prototype.scrollTo;
    beforeEach(() => {
      observers.length = 0;
      vi.stubGlobal('ResizeObserver', CapturingResizeObserver);
      HTMLElement.prototype.scrollTo = scrollTo as HTMLElement['scrollTo'];
    });
    afterEach(() => {
      HTMLElement.prototype.scrollTo = originalScrollTo;
      vi.unstubAllGlobals();
    });

    it('follows a streaming reply, lets a reader scroll away from it, and stops when the turn ends', () => {
      h.isLoading = true;
      const { rerender, container, layout, grow, nextFrame, userScroll } = renderTranscript();

      grow(1000);
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 600 });
      nextFrame();

      // A chart lands between a follow and its scroll event.
      grow(1100);
      layout.height = 1400;
      nextFrame();
      grow(1400);
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 1000 });
      nextFrame();

      // A notch up, still inside the band a downward scroll rejoins at.
      scrollTo.mockClear();
      userScroll(990);
      grow(1500);
      expect(scrollTo).not.toHaveBeenCalled();

      userScroll(1050);
      grow(1600);
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 1200 });

      // A notch up before that follow's scroll event arrives.
      scrollTo.mockClear();
      userScroll(1190);
      grow(1700);
      expect(scrollTo).not.toHaveBeenCalled();

      // Back at the end, the turn settles with one more line, followed in
      // the commit that ends it.
      userScroll(1300);
      layout.height = 1750;
      h.isLoading = false;
      rerender({ quickQueries: [] });
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 1350 });
      nextFrame();

      // The reply's actions and the text the typewriter still held land
      // after the turn has ended, and are followed too.
      grow(1750);
      grow(1800);
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 1400 });
      nextFrame();

      // A row the reader opens at the end of the settled turn stays where it
      // opened.
      scrollTo.mockClear();
      fireEvent.pointerDown(container);
      grow(2000);
      expect(scrollTo).not.toHaveBeenCalled();
    });

    it('stops following the end of a turn once the transcript has been quiet', () => {
      vi.useFakeTimers();
      try {
        h.isLoading = true;
        const { rerender, layout, grow, nextFrame } = renderTranscript();
        grow(1000);
        nextFrame();
        layout.height = 1100;
        h.isLoading = false;
        rerender({ quickQueries: [] });
        nextFrame();

        scrollTo.mockClear();
        act(() => { vi.advanceTimersByTime(1600); });
        grow(1100);
        grow(1300);
        expect(scrollTo).not.toHaveBeenCalled();
      } finally {
        vi.useRealTimers();
      }
    });

    it('takes a reader who scrolled up to the end when they send, and follows the reply', () => {
      const { rerender, grow, nextFrame, userScroll } = scrolledUpReader();

      h.handleSendMessage.mockImplementationOnce(() => {
        h.isLoading = true;
      });
      const onSend = ci.props!.onSend as (
        m: string, plan: boolean, att: unknown[], cmds: unknown[], opts: unknown,
      ) => void;
      act(() => onSend('and the next quarter?', false, [], [], {}));
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 600 });
      nextFrame();

      // Their message lands, then the reply grows under it.
      rerender({});
      grow(1150);
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 750 });
      nextFrame();
      grow(1400);
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 1000 });
      nextFrame();

      // The follow keeps its rules: a scroll up leaves it again.
      scrollTo.mockClear();
      userScroll(800);
      grow(1500);
      expect(scrollTo).not.toHaveBeenCalled();
    });

    it.each([
      ['an edit', 'onEditMessage', ['m1', 'edited']],
      ['a regenerate', 'onRegenerate', ['m1']],
      ['a retry', 'onRetry', []],
    ])('takes a reader who scrolled up to the end on %s', (_name, action, args) => {
      scrolledUpReader();
      act(() => (ml.actions![action] as (...a: unknown[]) => void)(...args));
      expect(scrollTo).toHaveBeenLastCalledWith({ top: 600 });
    });

    describe('when a reply finishes', () => {
      const REPLY_START = { other_preference: { turn_end_scroll: 'reply_start' } };

      /** A reader at the end of a thread while a turn streams a reply under
       *  them. The viewport is 400px; the reply's first line sits `replyAt` px
       *  down the transcript. */
      function streamTurn() {
        h.messages = [{ id: 'u1', role: 'user' }, { id: 'a1', role: 'assistant' }];
        const { rerender, container, grow, nextFrame, userScroll, ...kit } = renderTranscript();
        const layout = Object.assign(kit.layout, { replyAt: 0 });
        const setLoading = (loading: boolean) => {
          h.isLoading = loading;
          rerender({});
        };

        grow(1000);
        nextFrame();
        h.messages = [...h.messages, { id: 'u2', role: 'user' }, { id: 'a2', role: 'assistant' }];
        setLoading(true);
        // jsdom has no layout: the scroller's own top is 0.
        container.querySelector<HTMLElement>('[data-message-id="a2"] [data-reply-start]')!.getBoundingClientRect =
          () => ({ top: layout.replyAt - layout.top }) as DOMRect;
        grow(1600);
        expect(scrollTo).toHaveBeenLastCalledWith({ top: 1200 });
        nextFrame();
        scrollTo.mockClear();
        return { container, layout, grow, nextFrame, userScroll, setLoading };
      }

      it('brings the start of the reply under the viewport top and stops following', () => {
        h.preferences = REPLY_START;
        const { container, layout, grow, nextFrame, setLoading } = streamTurn();
        layout.replyAt = 900;
        setLoading(false);
        expect(scrollTo).toHaveBeenLastCalledWith({ top: 884, behavior: 'smooth' });
        nextFrame();

        // The settled turn folds its work away above the reply: the line is held
        // under the viewport top as it moves.
        layout.replyAt = 700;
        grow(1400);
        expect(scrollTo).toHaveBeenLastCalledWith({ top: 684 });
        nextFrame();

        // Once the reader takes over, a turn they did not start here (an
        // answered approval resuming, a reconnect) grows under them without
        // carrying them off the reply.
        fireEvent.wheel(container);
        scrollTo.mockClear();
        setLoading(true);
        grow(1500);
        expect(scrollTo).not.toHaveBeenCalled();
      });

      it('follows the next turn when the reader sends while the reply is held', () => {
        h.preferences = REPLY_START;
        const { layout, grow, nextFrame, setLoading } = streamTurn();
        layout.replyAt = 900;
        setLoading(false);
        nextFrame();
        h.handleSendMessage.mockImplementationOnce(() => {
          h.isLoading = true;
        });
        const onSend = ci.props!.onSend as (
          m: string, plan: boolean, att: unknown[], cmds: unknown[], opts: unknown,
        ) => void;
        act(() => onSend('and the next quarter?', false, [], [], {}));
        expect(scrollTo).toHaveBeenLastCalledWith({ top: 1200 });
        nextFrame();
        setLoading(true);
        grow(1700);
        expect(scrollTo).toHaveBeenLastCalledWith({ top: 1300 });
      });

      it('lands at once for a reader who prefers reduced motion', () => {
        vi.stubGlobal('matchMedia', (q: string) => ({ matches: q.includes('reduce'), media: q, addEventListener() {}, removeEventListener() {} }));
        h.preferences = REPLY_START;
        const { layout, setLoading } = streamTurn();
        layout.replyAt = 900;
        setLoading(false);
        expect(scrollTo).toHaveBeenLastCalledWith({ top: 884, behavior: 'auto' });
      });

      it('moves nothing when the reply fits on screen', () => {
        h.preferences = REPLY_START;
        const { layout, setLoading } = streamTurn();
        layout.replyAt = 1300;
        setLoading(false);
        expect(scrollTo).not.toHaveBeenCalled();
        expect(layout.top).toBe(1200);
      });

      it('stays at the end under the default preference', () => {
        const { layout, setLoading } = streamTurn();
        layout.replyAt = 900;
        setLoading(false);
        expect(scrollTo).not.toHaveBeenCalled();
        expect(layout.top).toBe(1200);
      });

      it('leaves a reader who scrolled up where they were', () => {
        h.preferences = REPLY_START;
        const { layout, userScroll, setLoading } = streamTurn();
        layout.replyAt = 900;
        userScroll(800);
        setLoading(false);
        expect(scrollTo).not.toHaveBeenCalled();
        expect(layout.top).toBe(800);
      });
    });
  });

  it('reads the PTC folder from the workspace detail, which a turn re-reads after a settle', async () => {
    api.getWorkspace.mockResolvedValueOnce({
      workspace_id: 'ws-1',
      name: 'New Name',
      dir_name: 'New Name',
      previous_dir_names: ['Old Name'],
    });
    renderPanel({
      workspaces: [{ workspace_id: 'ws-1', name: 'Old Name', dir_name: 'Old Name', previous_dir_names: [] }],
    });
    await vi.waitFor(() => expect(ml.props?.workspaceDirName).toBe('New Name'));
    expect(api.getWorkspace).toHaveBeenCalledWith('ws-1');
    expect(ml.props?.previousDirNames).toEqual(['Old Name']);
  });

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
      ['onResumeCreditPause', h.handleResumeCreditPause],
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

  it('drops a forwarded ?thread when the symbol switches to a fresh chat', () => {
    // The panel opens on the URL's thread, and a reload would open it again:
    // once the symbol moves to one with no saved thread, the URL has to agree
    // with the fresh chat on screen or the reload binds the old conversation
    // to the new symbol.
    h.threadId = '__default__';
    const view = renderPanel({}, '/market?thread=thread-xyz');
    expect(screen.getByTestId('search').textContent).toBe('?thread=thread-xyz');
    view.rerender({ symbol: 'MSFT' });
    expect(screen.getByTestId('search').textContent).toBe('');
    expect(userLocalStorage.getItem('marketview_thread_id_ws-1_MSFT')).toBeNull();
  });

  it('shows no continue button on a fresh chat with no return path', () => {
    h.threadId = '__default__';
    renderPanel();
    expect(screen.queryByText('Open in Chat')).not.toBeInTheDocument();
    expect(screen.queryByText('Return to Chat')).not.toBeInTheDocument();
  });

  it('opens the gone dialog for a tool row whose record the transcript no longer holds', () => {
    // The click used to be swallowed when the lookup missed, which reads as a
    // dead row; the dialog itself already says the call is gone.
    renderPanel();
    const open = ml.actions!.onToolCallDetailClick as (toolCallId: string) => void;
    act(() => open('tc-missing'));
    expect(screen.getByText(/no longer in the chat/i)).toBeInTheDocument();
  });
});
