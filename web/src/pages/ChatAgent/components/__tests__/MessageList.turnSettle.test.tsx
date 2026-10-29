/**
 * A turn whose stream ends while its reply is still typing settles only once
 * the reply has typed out.
 *
 * The end of the stream often comes in the same update as the last words. The
 * list used to decide the settle while rendering that update, before the text
 * had said it was typing again, so the turn's work folded away, and the Sources
 * pill and the deck came in, under the words still being typed.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { act } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import MessageList from '../MessageList';
import { MessageActionsProvider } from '../messageList/MessageActionsContext';
import type { MessageRecord } from '../messageList/types';

// The typewriter, held wherever the test puts it: it draws the text up to
// `typedTo` characters.
const typewriter = vi.hoisted(() => {
  let typedTo = Infinity;
  const listeners = new Set<() => void>();
  return {
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => { listeners.delete(listener); };
    },
    read: () => typedTo,
    typeTo: (chars: number) => {
      typedTo = chars;
      for (const listener of listeners) listener();
    },
  };
});

vi.mock('@/components/ui/animated-text', async () => {
  const { useSyncExternalStore } = await vi.importActual<typeof import('react')>('react');
  return {
    useAnimatedText: (text: string) => text.slice(0, useSyncExternalStore(typewriter.subscribe, typewriter.read)),
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

const ASKED = new Date('2026-09-21T15:04:00.000Z');
const FIRST_WORDS = 'Apple closed at ';
const REPLY = 'Apple closed at $229.87, up 1.2% on the day.';

function transcript(reply: string, streaming: boolean): MessageRecord[] {
  return [
    { id: 'u0', role: 'user', content: 'And Apple?', contentType: 'text', timestamp: ASKED, isStreaming: false },
    {
      id: 'a0', role: 'assistant', content: reply, contentType: 'text',
      timestamp: new Date('2026-09-21T15:04:02.000Z'), isStreaming: streaming,
      ...(streaming ? {} : { completedAt: Date.parse('2026-09-21T15:04:09.000Z') }),
      contentSegments: [
        { type: 'tool_call', order: 0, toolCallId: 'tc-0' },
        { type: 'text', content: reply, order: 1 },
      ],
      reasoningProcesses: {},
      toolCallProcesses: {
        'tc-0': { toolName: 'WebSearch', toolCall: { args: {} }, isInProgress: false, isComplete: true, isFailed: false },
      },
    },
  ] as MessageRecord[];
}

const list = (messages: MessageRecord[], isLoading: boolean) => (
  <MessageActionsProvider actions={{}}>
    <MessageList messages={messages} isLoading={isLoading} />
  </MessageActionsProvider>
);

const foldState = (container: HTMLElement) =>
  container.querySelector('[data-turn-fold]')?.getAttribute('data-turn-fold') ?? null;

describe('a turn settling', () => {
  it('waits for the last words when they arrive with the end of the stream', () => {
    // The first words are on screen, so nothing is typing when the last ones
    // and the end of the stream land together.
    const view = renderWithProviders(list(transcript(FIRST_WORDS, true), true));
    expect(foldState(view.container)).toBe('live');
    act(() => typewriter.typeTo(FIRST_WORDS.length));

    view.rerender(list(transcript(REPLY, false), false));
    expect(foldState(view.container)).toBe('live');
    expect(view.getByTestId('streaming-indicator')).toBeInTheDocument();

    act(() => typewriter.typeTo(Infinity));
    expect(foldState(view.container)).toBe('collapsed');
  });
});

describe('a history load', () => {
  const loading = (messages: MessageRecord[], isLoadingHistory: boolean) => (
    <MessageActionsProvider actions={{}}>
      <MessageList messages={messages} isLoadingHistory={isLoadingHistory} />
    </MessageActionsProvider>
  );

  // Main shown again while the thread loads: what the load has written so far
  // is a turn with its work and no answer.
  it('draws the skeleton, not the half written so far, for a list that mounts mid-load', () => {
    const half = transcript('', false);
    const view = renderWithProviders(loading(half, true));
    expect(view.queryByTestId('markdown-content')).toBeNull();
    expect(view.container.querySelector('.animate-pulse')).not.toBeNull();

    view.rerender(loading(transcript(REPLY, false), false));
    expect(view.getByText(REPLY)).toBeInTheDocument();
  });
});
