/**
 * ChatView mounting harness (D2): mounts the REAL ChatView shell with the
 * data boundary mocked (useChatMessages + sibling data hooks + the api
 * barrel) so the presentation refactor of ChatView's own body has a net.
 * Child components render real except ChatInput (heavy, separately owned);
 * assertions target ChatView-owned JSX: the transcript, the reconnecting
 * status, the model-resilience pill, and the error banner.
 */
import React from 'react';
import { describe, it, expect, vi, beforeAll, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import { screen } from '@testing-library/react';
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
  const make = (Comp: React.ElementType | string) =>
    function MotionStub({ children, ...props }: { children?: React.ReactNode } & Record<string, unknown>) {
      const domProps: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(props)) {
        if (!FRAMER_ONLY_PROPS.has(k)) domProps[k] = v;
      }
      return createEl(Comp, domProps, children);
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

// Heavy leaf owned by Phase 6; the harness targets ChatView's own body.
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

/** Mutable idle-state return for the mocked useChatMessages; tests override
 * fields on `chatState` before rendering. */
const baseChatState = () => ({
  messages: [] as Record<string, unknown>[],
  isLoading: false,
  hasActiveSubagents: false,
  awaitingReportBack: false,
  workspaceStarting: false as const,
  isCompacting: false as const,
  setIsCompacting: vi.fn(),
  queuedSend: null,
  isLoadingHistory: false,
  isReconnecting: false,
  modelStatus: null as Record<string, unknown> | null,
  fallbackSuggestion: null,
  clearFallbackSuggestion: vi.fn(),
  messageError: null as string | null,
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
  threadId: 'thread-mount-1',
  threadModels: [] as string[],
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
});

let chatState: ReturnType<typeof baseChatState>;

vi.mock('../../hooks/useChatMessages', async (importOriginal) => ({
  ...(await importOriginal() as Record<string, unknown>),
  useChatMessages: () => chatState,
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

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal() as Record<string, unknown>),
  getWorkspace: vi.fn().mockResolvedValue({ workspace_id: 'ws-mount', name: 'Mount WS', status: 'active' }),
  getThreadShareStatus: vi.fn().mockResolvedValue({ is_shared: false }),
  getSubagentTaskStatus: vi.fn().mockResolvedValue({}),
}));

import ChatView from '../ChatView';

const assistant = (id: string, overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  id,
  role: 'assistant',
  content: '',
  contentType: 'text',
  timestamp: new Date(),
  isStreaming: false,
  contentSegments: [],
  reasoningProcesses: {},
  toolCallProcesses: {},
  ...overrides,
});

const userMsg = (id: string, content: string): Record<string, unknown> => ({
  id, role: 'user', content, contentType: 'text', timestamp: new Date(), isStreaming: false,
});

function mountChatView() {
  return renderWithProviders(
    <ChatView
      workspaceId="ws-mount"
      threadId="thread-mount-1"
      onBack={vi.fn()}
      workspaceName="Mount WS"
    />,
    { route: '/chat/t/thread-mount-1' },
  );
}

beforeAll(() => {
  if (!Element.prototype.scrollTo) {
    Element.prototype.scrollTo = (() => {}) as typeof Element.prototype.scrollTo;
  }
  if (!Element.prototype.scrollIntoView) {
    Element.prototype.scrollIntoView = (() => {}) as typeof Element.prototype.scrollIntoView;
  }
});

beforeEach(() => {
  chatState = baseChatState();
});

describe('ChatView mounting harness', () => {
  it('mounts idle with an empty transcript and the input area', () => {
    const { container } = mountChatView();
    expect(screen.getByTestId('chat-input-stub')).toBeInTheDocument();
    expect(container.querySelector('[data-message-id]')).toBeNull();
  });

  it('renders transcript bubbles from the hook state', () => {
    chatState.messages = [
      userMsg('u1', 'hello mount'),
      assistant('a1', {
        content: 'assistant reply text',
        contentSegments: [{ type: 'text', order: 0 }],
      }),
    ];
    const { container } = mountChatView();
    expect(screen.getByText('hello mount')).toBeInTheDocument();
    expect(container.querySelector('[data-message-id="a1"]')).not.toBeNull();
  });

  it('shows the reconnecting status row while isReconnecting', () => {
    chatState.isReconnecting = true;
    mountChatView();
    expect(screen.getByText('chat.reconnecting')).toBeInTheDocument();
  });

  it('shows the model-retry pill only while loading', () => {
    chatState.modelStatus = { kind: 'retrying', model: 'm1', attempt: 0, maxRetries: 2 };
    chatState.isLoading = true;
    mountChatView();
    expect(screen.getByText('chat.modelRetrying')).toBeInTheDocument();
  });

  it('renders the error banner for a string error when idle', () => {
    chatState.messageError = 'Something went wrong on the server';
    mountChatView();
    expect(screen.getByText('Something went wrong on the server')).toBeInTheDocument();
  });
});
