/**
 * Two concurrent workflow runs are the designed state (max_runs_per_thread
 * defaults to 2), and switching between their tabs renders the same
 * WorkflowRunDetail element position. The detail owns its stop state locally,
 * so ChatView must key it per run — unkeyed, run B inherits run A's
 * "Stopping…" and its Stop button is dead for the rest of the run.
 *
 * Mounts the REAL ChatView (data boundary mocked, same shape as
 * ChatView.mount.test.tsx) so the assertion covers the shipped call site.
 */
import React from 'react';
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import { screen, fireEvent, waitFor } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

vi.mock('framer-motion', async () => {
  const ReactActual = await vi.importActual<typeof import('react')>('react');
  const FRAMER_ONLY_PROPS = new Set([
    'initial', 'animate', 'exit', 'transition', 'variants',
    'whileHover', 'whileTap', 'whileInView', 'layout', 'layoutId',
    'onAnimationComplete', 'onAnimationStart', 'drag', 'dragConstraints',
    'dragElastic', 'onDragEnd',
  ]);
  const createEl = ReactActual.createElement as (type: unknown, props?: unknown, ...children: unknown[]) => React.ReactElement;
  // Cached per tag: a fresh stub identity on every property access would be a
  // new component type each render, remounting ChatView's whole subtree (its
  // root is a motion.div) — which is exactly the state carryover under test.
  const cache = new Map<React.ElementType | string, React.ElementType>();
  const make = (Comp: React.ElementType | string): React.ElementType => {
    const hit = cache.get(Comp);
    if (hit) return hit;
    const Stub = function MotionStub({ children, ...props }: { children?: React.ReactNode } & Record<string, unknown>) {
      const domProps: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(props)) {
        if (!FRAMER_ONLY_PROPS.has(k)) domProps[k] = v;
      }
      return createEl(Comp, domProps, children);
    };
    cache.set(Comp, Stub);
    return Stub;
  };
  return {
    motion: new Proxy({} as Record<string, unknown>, {
      get: (_t, key: string) => (key === 'create' ? make : make(key)),
    }),
    AnimatePresence: ({ children }: { children?: React.ReactNode }) =>
      ReactActual.createElement(ReactActual.Fragment, null, children),
    animate: () => ({ stop: () => {} }),
    useMotionValue: (v: unknown) => ({ get: () => v, set: () => {}, on: () => () => {}, onChange: () => () => {} }),
    useTransform: () => ({ get: () => 0, set: () => {}, on: () => () => {} }),
    useSpring: (v: unknown) => ({ get: () => v, set: () => {}, on: () => () => {} }),
  };
});

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));
vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));

vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
}));

vi.mock('../../../../components/ui/chat-input', async () => {
  const ReactActual = await vi.importActual<typeof import('react')>('react');
  return {
    default: ReactActual.forwardRef(function ChatInputStub() {
      return <textarea data-testid="chat-input-stub" />;
    }),
  };
});

// ---------------------------------------------------------------------------
// Data boundary
// ---------------------------------------------------------------------------

const THREAD_ID = 'thread-wf-tabs';

vi.mock('../../hooks/useChatMessages', async (importOriginal) => ({
  ...(await importOriginal() as Record<string, unknown>),
  useChatMessages: () => ({
    messages: [],
    isLoading: false,
    hasActiveSubagents: true,
    awaitingReportBack: false,
    workspaceStarting: false,
    isCompacting: false,
    setIsCompacting: vi.fn(),
    queuedSend: null,
    isLoadingHistory: false,
    isReconnecting: false,
    modelStatus: null,
    fallbackSuggestion: null,
    clearFallbackSuggestion: vi.fn(),
    messageError: null,
    returnedSteering: null,
    clearReturnedSteering: vi.fn(),
    handleSendMessage: vi.fn(),
    stopWorkflow: vi.fn(),
    stopCompaction: vi.fn(),
    pendingInterrupt: null,
    pendingRejection: null,
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
    tokenUsage: null,
    threadId: THREAD_ID,
    threadModels: [],
    lastThreadModel: null,
    marketWatch: null,
    isShared: false,
    insertNotification: vi.fn(),
    handleEditMessage: vi.fn(),
    handleRegenerate: vi.fn(),
    handleRetry: vi.fn(),
    handleThumbUp: vi.fn(),
    handleThumbDown: vi.fn(),
    getFeedbackForMessage: vi.fn().mockReturnValue(null),
    reconnectIfStaleRun: vi.fn().mockResolvedValue(undefined),
    getSubagentHistory: vi.fn().mockReturnValue(null),
    resolveSubagentIdToAgentId: vi.fn((id: string) => id),
    hydrateTaskTranscript: vi.fn().mockResolvedValue(false),
  }),
}));

/** Two running workflow-run cards, the shape useCardState holds them in. */
function runCard(shortId: string, description: string) {
  return {
    title: 'Subagent',
    subagentData: {
      agentId: `task:${shortId}`,
      taskId: `task:${shortId}`,
      displayId: `Task-${shortId}`,
      description,
      prompt: '',
      type: 'workflow',
      status: 'active',
      messages: [],
      isActive: true,
    },
  };
}

vi.mock('../../hooks/useCardState', () => ({
  useCardState: () => ({
    cards: {
      'subagent-task:wfa': runCard('wfa', 'first run'),
      'subagent-task:wfb': runCard('wfb', 'second run'),
    },
    updateTodoListCard: vi.fn(),
    updateSubagentCard: vi.fn(),
    finalizePendingTodos: vi.fn(),
    clearSubagentCards: vi.fn(),
  }),
}));

vi.mock('../../hooks/useWorkspaceFiles', () => ({
  useWorkspaceFiles: () => ({ files: [], loading: false, error: null, refresh: vi.fn() }),
}));

vi.mock('../../hooks/useNavigationData', async (importOriginal) => ({
  ...(await importOriginal() as Record<string, unknown>),
  useNavigationData: () => ({
    workspaces: [],
    workspaceThreads: {},
    expandWorkspace: vi.fn(),
    hasMore: false,
    loadAll: vi.fn(),
    loadMoreThreads: vi.fn(),
    reorderWorkspace: vi.fn(),
    canReorderWorkspaces: false,
    pinWorkspace: vi.fn(),
    renameWorkspace: vi.fn(),
  }),
}));

type CancelOutcome = {
  cancelled: boolean;
  task_id: string;
  state?: string;
  message: string;
};

// Default: never settles — a real stop is cleared by the run's terminal frame,
// so the pending promise is what holds "Stopping…" on screen. Tests that need
// the endpoint's no-op answer override it per call.
const cancelSubagentTask = vi.fn(
  (_threadId: string, _taskId: string): Promise<CancelOutcome> =>
    new Promise<CancelOutcome>(() => {}),
);

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal() as Record<string, unknown>),
  getWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'ws-wf', name: 'WF WS', status: 'active' }),
  getThreadShareStatus: vi.fn().mockResolvedValue({ is_shared: false }),
  getSubagentTaskStatus: vi.fn().mockResolvedValue({}),
  cancelSubagentTask: (threadId: string, taskId: string) => cancelSubagentTask(threadId, taskId),
}));

import ChatView from '../ChatView';

function mountAtRun(initialTaskId: string) {
  return renderWithProviders(
    <ChatView
      workspaceId="ws-wf"
      threadId={THREAD_ID}
      initialTaskId={initialTaskId}
      onBack={vi.fn()}
      workspaceName="WF WS"
    />,
    { route: `/chat/t/${THREAD_ID}/${initialTaskId}` },
  );
}

const stopButton = () => screen.getByTestId('workflow-detail-stop');

beforeAll(() => {
  if (!Element.prototype.scrollTo) {
    Element.prototype.scrollTo = (() => {}) as typeof Element.prototype.scrollTo;
  }
  if (!Element.prototype.scrollIntoView) {
    Element.prototype.scrollIntoView = (() => {}) as typeof Element.prototype.scrollIntoView;
  }
});

beforeEach(() => {
  cancelSubagentTask.mockClear();
});

describe('ChatView workflow run tabs', () => {
  it('opens the run detail for the deep-linked run', () => {
    mountAtRun('wfa');
    expect(screen.getByText('first run')).toBeInTheDocument();
    expect(stopButton()).toBeEnabled();
  });

  it('gives the second run its own stop state after a tab switch', () => {
    const { rerender } = mountAtRun('wfa');

    fireEvent.click(stopButton());
    expect(cancelSubagentTask).toHaveBeenCalledWith(THREAD_ID, 'wfa');
    expect(stopButton()).toHaveTextContent('chat.workflowRun.stopping');
    expect(stopButton()).toBeDisabled();

    rerender(
      <ChatView
        workspaceId="ws-wf"
        threadId={THREAD_ID}
        initialTaskId="wfb"
        onBack={vi.fn()}
        workspaceName="WF WS"
      />,
    );

    expect(screen.getByText('second run')).toBeInTheDocument();
    expect(stopButton()).toHaveTextContent('chat.workflowRun.stop');
    expect(stopButton()).toBeEnabled();

    cancelSubagentTask.mockClear();
    fireEvent.click(stopButton());
    expect(cancelSubagentTask).toHaveBeenCalledWith(THREAD_ID, 'wfb');
  });

  it('releases the stop button when the cancel stopped nothing', async () => {
    // The endpoint answers HTTP 200 with cancelled:false for a run the ledger
    // already settled or lost, and no terminal frame follows a stop that was
    // never sent — so nothing else would ever clear the button.
    mountAtRun('wfa');
    cancelSubagentTask.mockResolvedValueOnce({
      cancelled: false,
      task_id: 'wfa',
      state: 'already_finished',
      message: 'Task already completed; nothing to cancel.',
    });

    fireEvent.click(stopButton());
    expect(stopButton()).toBeDisabled();

    await waitFor(() => expect(stopButton()).toBeEnabled());
    expect(stopButton()).toHaveTextContent('chat.workflowRun.stop');
  });

  it('does not carry a stop back to the run the user came from', () => {
    const { rerender } = mountAtRun('wfa');
    const back = (initialTaskId: string) =>
      rerender(
        <ChatView
          workspaceId="ws-wf"
          threadId={THREAD_ID}
          initialTaskId={initialTaskId}
          onBack={vi.fn()}
          workspaceName="WF WS"
        />,
      );

    back('wfb');
    fireEvent.click(stopButton());
    expect(stopButton()).toBeDisabled();

    back('wfa');
    expect(screen.getByText('first run')).toBeInTheDocument();
    expect(stopButton()).toBeEnabled();
  });
});
