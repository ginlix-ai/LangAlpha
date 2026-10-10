/**
 * Shared harness for the suites that mount the REAL ChatView with its data
 * boundary mocked (useChatMessages, the sibling data hooks and the api
 * barrel). Each test file still declares its own `vi.mock` calls (vitest
 * hoists mocks per file) but delegates the module shapes here, so the
 * scaffold lives once. Nothing here imports ChatView or a mocked module, so a
 * mock factory can load this file while ChatView is still loading.
 */
import React from 'react';
import { vi } from 'vitest';
import { act } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

export const WORKSPACE_ID = 'ws-harness';
export const THREAD_ID = 'thread-harness-1';

const FRAMER_ONLY_PROPS = new Set([
  'initial', 'animate', 'exit', 'transition', 'variants',
  'whileHover', 'whileTap', 'whileInView', 'layout', 'layoutId',
  'onAnimationComplete', 'onAnimationStart', 'drag', 'dragConstraints',
  'dragElastic', 'onDragEnd',
]);

/** `@/lib/framer` with every motion element rendered as its plain tag. */
export function framerMock() {
  const createEl = React.createElement as (type: unknown, props?: unknown, ...children: unknown[]) => React.ReactElement;
  // Cached per tag: a fresh stub identity per access would remount ChatView's
  // subtree on every rerender, and with it the scroll controller.
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
    AnimatePresence: ({ children }: { children?: React.ReactNode }) => React.createElement(React.Fragment, null, children),
    animate: () => ({ stop: () => {} }),
    useMotionValue: (v: unknown) => ({ get: () => v, set: () => {}, on: () => () => {}, onChange: () => () => {} }),
    useTransform: () => ({ get: () => 0, set: () => {}, on: () => () => {} }),
    useSpring: (v: unknown) => ({ get: () => v, set: () => {}, on: () => () => {} }),
    useReducedMotion: () => false,
  };
}

export function markdownMock() {
  return {
    default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
  };
}

/** The props ChatView last handed the composer, which is heavy and separately owned. */
export const chatInput = { props: null as Record<string, unknown> | null };

export function chatInputMock() {
  return {
    default: (props: Record<string, unknown>) => {
      chatInput.props = props;
      return <textarea data-testid="chat-input-stub" />;
    },
  };
}

/** Idle-state return for the mocked useChatMessages. */
const baseChatState = () => ({
  messages: [] as Record<string, unknown>[],
  isLoading: false,
  hasActiveSubagents: false,
  awaitingReportBack: false,
  reportBackOwed: false,
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
  threadModels: [] as string[],
  marketWatch: null,
  isShared: false,
  insertNotification: vi.fn(),
  handleEditMessage: vi.fn(),
  handleRegenerate: vi.fn(),
  handleRetry: vi.fn(),
  handleThumbUp: vi.fn(),
  handleThumbDown: vi.fn(),
  feedbackByTurn: {} as Record<number, unknown>,
  reconnectIfStaleRun: vi.fn<() => Promise<boolean>>().mockResolvedValue(true),
  isOwnRun: () => false,
  getSubagentHistory: vi.fn().mockReturnValue(null),
  resolveSubagentIdToAgentId: vi.fn((id: string) => id),
});

export type ChatState = ReturnType<typeof baseChatState>;

/** What the mocked useChatMessages returns; tests override fields before rendering. */
export let chatState: ChatState = baseChatState();

// The live transcript the bubbles render from, notifying like the real store.
const listeners = new Set<() => void>();
const liveMessages = {
  get: () => chatState.messages,
  set: () => {},
  subscribe: (fn: () => void) => {
    listeners.add(fn);
    return () => listeners.delete(fn);
  },
};

export function resetChatState() {
  chatState = baseChatState();
  chatInput.props = null;
  listeners.clear();
}

export function chatMessagesMock(original: unknown) {
  return {
    ...(original as Record<string, unknown>),
    useChatMessages: () => ({ ...chatState, liveMessages }),
  };
}

export function workspaceFilesMock() {
  return { useWorkspaceFiles: () => ({ files: [], loading: false, error: null, refresh: vi.fn() }) };
}

export function navigationDataMock(original: unknown) {
  return {
    ...(original as Record<string, unknown>),
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
  };
}

export function apiMock(original: unknown) {
  return {
    ...(original as Record<string, unknown>),
    getWorkspace: vi.fn().mockResolvedValue({ workspace_id: WORKSPACE_ID, name: 'Harness WS', status: 'active' }),
    getThreadShareStatus: vi.fn().mockResolvedValue({ is_shared: false }),
    getSubagentTaskStatus: vi.fn().mockResolvedValue({}),
  };
}

export const userMsg = (id: string, content: string): Record<string, unknown> => ({
  id, role: 'user', content, contentType: 'text', timestamp: new Date(), isStreaming: false,
});

export const assistant = (id: string, overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
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

type ChatViewComponent = React.ComponentType<{
  workspaceId: string;
  threadId: string;
  onBack: () => void;
  workspaceName: string;
}>;

/** Mounts `ChatView` on the harness thread. `update` hands it the hook's
 *  current state, as a streamed write would. */
export function mountChatView(ChatView: ChatViewComponent) {
  const onBack = vi.fn();
  // A fresh element per render: the same one would bail out of re-rendering.
  const ui = () => <ChatView workspaceId={WORKSPACE_ID} threadId={THREAD_ID} onBack={onBack} workspaceName="Harness WS" />;
  const view = renderWithProviders(ui(), { route: `/chat/t/${THREAD_ID}` });
  const update = (patch: Partial<ChatState>) => {
    Object.assign(chatState, patch);
    act(() => {
      view.rerender(ui());
      listeners.forEach((fn) => fn());
    });
  };
  return { ...view, update };
}
