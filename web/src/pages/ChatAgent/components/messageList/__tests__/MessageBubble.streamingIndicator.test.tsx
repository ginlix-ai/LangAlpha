/**
 * The streaming indicator stays under a turn for as long as it runs, and steps
 * aside while answer text types out.
 *
 * It used to follow `message.isStreaming`, which is per model call: the flag
 * drops on a content-free finish, so the indicator left between model calls
 * while the turn still worked.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { MessageBubble } from '../MessageBubble';
import { MessageActionsProvider } from '../MessageActionsContext';
import { TranscriptDisplayContext } from '@/lib/transcriptDisplay';
import { ProseTyping, ProseTypingContext } from '../proseTyping';
import type { MessageRecord } from '../types';

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'light' }) }));
vi.mock('../../Markdown', () => ({
  default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
}));

interface Shape {
  isStreaming: boolean;
  isTurnLive: boolean;
  isTurnTail?: boolean;
  text?: string;
}

const TOOL = {
  tc1: {
    toolName: 'WebSearch', toolCall: { args: { query: 'AAPL price' } },
    isInProgress: false, isComplete: true, isFailed: false, order: 0,
  },
};

// The list's store, which the bubble learns of its prose typing from.
const typing = new ProseTyping();

function bubble({ isStreaming, isTurnLive, isTurnTail = true, text = '' }: Shape) {
  const segments: Record<string, unknown>[] = [{ type: 'tool_call', order: 0, toolCallId: 'tc1' }];
  if (text) segments.push({ type: 'text', content: text, order: 1 });
  const message = {
    id: 'a1', role: 'assistant', contentType: 'text', timestamp: new Date(), isStreaming,
    content: text, contentSegments: segments, reasoningProcesses: {}, toolCallProcesses: TOOL,
  } as MessageRecord;
  return (
    <TranscriptDisplayContext.Provider value={{ turnDisplay: 'lean', streamingMode: 'token' }}>
      <ProseTypingContext value={typing}>
        <MessageActionsProvider actions={{}}>
          <MessageBubble message={message} turnIndex={0} isTurnTail={isTurnTail} isTurnLive={isTurnLive} />
        </MessageActionsProvider>
      </ProseTypingContext>
    </TranscriptDisplayContext.Provider>
  );
}

const indicator = (c: HTMLElement) => c.querySelector<HTMLElement>('[data-testid="streaming-indicator"]');

describe('streaming indicator', () => {
  it('stays between two model calls of a running turn', () => {
    // A model call that ended on a content-free finish clears `isStreaming`
    // while the turn goes on to its next call.
    const { container } = render(bubble({ isStreaming: false, isTurnLive: true }));
    expect(indicator(container)).toBeInTheDocument();
  });

  it('fades out while reply text types, and back in once it sits still', async () => {
    const view = render(bubble({ isStreaming: true, isTurnLive: true }));
    expect(indicator(view.container)?.style.opacity).toBe('1');
    view.rerender(bubble({ isStreaming: true, isTurnLive: true, text: 'Apple closed at ' }));
    view.rerender(bubble({ isStreaming: true, isTurnLive: true, text: 'Apple closed at $229.87, up 1.2% on the day. ' }));
    await waitFor(() => expect(indicator(view.container)?.style.opacity).toBe('0'));
    // Mounted throughout, so its row never leaves the layout.
    await waitFor(() => expect(indicator(view.container)?.style.opacity).toBe('1'), { timeout: 3000 });
  });

  it('is drawn only under the last bubble of the turn', () => {
    const { container } = render(bubble({ isStreaming: true, isTurnLive: true, isTurnTail: false }));
    expect(indicator(container)).toBeNull();
  });

  it('leaves with the turn', async () => {
    const view = render(bubble({ isStreaming: true, isTurnLive: true, text: 'Apple closed at $229.87.' }));
    view.rerender(bubble({ isStreaming: false, isTurnLive: false, text: 'Apple closed at $229.87.' }));
    await waitFor(() => expect(indicator(view.container)).toBeNull());
  });
});
