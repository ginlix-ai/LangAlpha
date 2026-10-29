/**
 * Orphan-bubble suppression: assistant messages that settled with nothing to
 * show (no segments, no text) stay in STATE — they are backend turns and
 * edit/regenerate count assistant bubbles to map UI position → turn_index —
 * but MessageList must not paint them, or the transcript shows a bare avatar +
 * action-button row (the "orphan logo"). Real sources: a HITL-resume turn whose
 * content landed on another bubble, a history turn whose only event was a
 * re-raised interrupt deduped by interrupt_id, or one whose only call was a
 * hidden tool.
 *
 * Anything renderable keeps the bubble: streaming indicator, text, a segment
 * that draws something, the Sources pill, the Stopped chip, or an error.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import MessageList, { isOrphanAssistantMessage } from '../MessageList';
import type { MessageRecord } from '../messageList/types';
import { loadConversationHistory } from '../../session/history/replayHistory';
import { buildRuntime, makeDeps, replayOf } from '../../session/history/__tests__/replayHarness';

vi.mock('@/lib/framer', async () => {
  const ReactActual = await vi.importActual<typeof import('react')>('react');
  const FRAMER_ONLY_PROPS = new Set([
    'initial', 'animate', 'exit', 'transition', 'variants',
    'whileHover', 'whileTap', 'whileInView', 'layout', 'layoutId',
    'onAnimationComplete', 'onAnimationStart',
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
  };
});

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
}));

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

type Msg = Record<string, unknown>;

const assistant = (id: string, overrides: Msg = {}): Msg => ({
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

const userMsg = (id: string, content: string): Msg => ({
  id, role: 'user', content, contentType: 'text', timestamp: new Date(), isStreaming: false,
});

const bubble = (container: HTMLElement, id: string) =>
  container.querySelector(`[data-message-id="${id}"]`);

describe('MessageList — orphan assistant bubble suppression', () => {
  it('does not paint a settled empty assistant bubble (no orphan avatar/actions)', () => {
    const messages = [
      userMsg('u-1', 'dispatch two agents'),
      assistant('history-assistant-0', {
        contentSegments: [{ type: 'text', content: 'dispatching', order: 0 }],
        content: 'dispatching',
      }),
      // The resume turn whose content landed elsewhere — stays in state, hidden.
      assistant('assistant-hitl-1'),
      assistant('history-assistant-2', {
        contentSegments: [{ type: 'text', content: 'Both agents are dispatched.', order: 0 }],
        content: 'Both agents are dispatched.',
      }),
    ];
    const { container } = renderWithProviders(<MessageList messages={messages} isLoading={false} />);

    expect(bubble(container, 'assistant-hitl-1')).toBeNull();
    // Neighbors are unaffected.
    expect(bubble(container, 'u-1')).not.toBeNull();
    expect(bubble(container, 'history-assistant-0')).not.toBeNull();
    expect(bubble(container, 'history-assistant-2')).not.toBeNull();
  });

  it('keeps empty bubbles that still communicate something', () => {
    const messages = [
      userMsg('u-1', 'hello'),
      // Mid-stream placeholder: shows the streaming indicator.
      assistant('a-streaming', { isStreaming: true }),
      // Hard-stopped turn: shows the Stopped chip.
      assistant('a-stopped', { stopped: true }),
      // Failed turn: shows the error state.
      assistant('a-error', { error: true }),
      // Provenance-only turn: shows the Sources pill.
      assistant('a-sources', {
        provenanceRecords: {
          r1: { record_id: 'r1', source_type: 'web', identifier: 'https://example.com/a', timestamp: '2026-01-01T00:00:00Z' },
        },
      }),
    ];
    const { container } = renderWithProviders(<MessageList messages={messages} isLoading={false} />);

    for (const id of ['a-streaming', 'a-stopped', 'a-error', 'a-sources']) {
      expect(bubble(container, id)).not.toBeNull();
    }
  });

  it('predicate: user and notification messages are never orphans', () => {
    expect(isOrphanAssistantMessage(userMsg('u-1', ''))).toBe(false);
    expect(isOrphanAssistantMessage({ id: 'n-1', role: 'notification', content: 'x' })).toBe(false);
    expect(isOrphanAssistantMessage(assistant('a-1'))).toBe(true);
  });
});

const user = (turn: number, content: string) => ({
  event: 'user_message',
  data: { thread_id: 'thread-1', turn_index: turn, content },
});

const text = (turn: number, content: string) => ({
  event: 'message_chunk',
  data: { thread_id: 'thread-1', turn_index: turn, role: 'assistant', content_type: 'text', content },
});

const toolCall = (turn: number, id: string, name: string, args: Record<string, unknown>) => ({
  event: 'tool_calls',
  data: { thread_id: 'thread-1', turn_index: turn, tool_calls: [{ id, name, args }] },
});

const result = (turn: number, id: string, content: string, status?: string) => ({
  event: 'tool_call_result',
  data: { thread_id: 'thread-1', turn_index: turn, tool_call_id: id, content, ...(status ? { status } : {}) },
});

/** Through the real replay, so the bubble carries the segments a reloaded
 *  thread gets (a `Task` call becomes its own segment) rather than a guess. */
async function replay(items: Array<Record<string, unknown>>) {
  const { rt, read } = buildRuntime();
  api.replayThreadHistory.mockImplementation(replayOf(items));
  await loadConversationHistory(rt, makeDeps());
  return renderWithProviders(
    <MessageList messages={read() as unknown as MessageRecord[]} isLoading={false} />,
  );
}

describe('history replay: a bubble with nothing to paint', () => {
  it('skips a bubble whose only segments are hidden tool calls', async () => {
    // TodoWrite draws the floating todo card, never anything in the bubble.
    const { container } = await replay([
      user(0, 'Track the steps'),
      toolCall(0, 'call-todo', 'TodoWrite', { todos: [{ content: 'Pull the 10-K', status: 'pending' }] }),
      result(0, 'call-todo', 'Updated todo list', 'success'),
      user(1, 'Go on'),
      text(1, 'Done.'),
    ]);

    expect(bubble(container, 'history-assistant-0')).toBeNull();
    expect(bubble(container, 'history-assistant-1')).not.toBeNull();
  });

  it('keeps a bubble whose hidden tool call draws its own card', async () => {
    const { container } = await replay([
      user(0, 'Research it in the background'),
      toolCall(0, 'call-task', 'Task', { description: 'Read the 10-K', prompt: 'Read the 10-K', subagent_type: 'research' }),
    ]);

    expect(bubble(container, 'history-assistant-0')).not.toBeNull();
  });
});
