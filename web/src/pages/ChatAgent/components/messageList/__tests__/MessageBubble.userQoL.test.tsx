/**
 * User-bubble QoL pins: the copy action exists for user messages (assistant
 * parity), and long user content collapses behind Show all / Show less while
 * assistant content never grows the toggle.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MessageBubble } from '../MessageBubble';
import { MessageActionsProvider, type MessageActions } from '../MessageActionsContext';
import type { MessageRecord } from '../types';

vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: null }),
}));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark' }),
}));

const writeText = vi.fn().mockResolvedValue(undefined);
beforeEach(() => {
  writeText.mockClear();
  Object.defineProperty(navigator, 'clipboard', {
    configurable: true,
    value: { writeText },
  });
});

function makeMessage(overrides: Partial<Record<string, unknown>> = {}): MessageRecord {
  return {
    id: 'm-1',
    role: 'user',
    content: 'What does the Q3 report say about margins?',
    contentType: 'text',
    timestamp: new Date('2026-08-01T00:00:00Z'),
    isStreaming: false,
    ...overrides,
  } as MessageRecord;
}

function renderBubble(message: MessageRecord, actions: MessageActions = { onEditMessage: vi.fn() }) {
  return render(
    <MessageActionsProvider actions={actions}>
      <MessageBubble message={message} turnIndex={0} isTurnTail={false} />
    </MessageActionsProvider>,
  );
}

/** jsdom has no layout — pin every element's scrollHeight for the measure effect. */
function stubScrollHeight(px: number): () => void {
  const original = Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'scrollHeight');
  Object.defineProperty(HTMLElement.prototype, 'scrollHeight', {
    configurable: true,
    get: () => px,
  });
  return () => {
    if (original) Object.defineProperty(HTMLElement.prototype, 'scrollHeight', original);
    else delete (HTMLElement.prototype as { scrollHeight?: unknown }).scrollHeight;
  };
}

describe('MessageBubble — user copy action', () => {
  it('copies the user message content and flips to the copied state', async () => {
    renderBubble(makeMessage());
    const copyButton = screen.getByTitle('Copy message');
    fireEvent.click(copyButton);
    expect(writeText).toHaveBeenCalledWith('What does the Q3 report say about margins?');
    // Copied state confirms only after the clipboard write resolves.
    expect(await screen.findByTitle('Copied!')).toBeInTheDocument();
  });

  it('keeps the edit pencil alongside copy', () => {
    renderBubble(makeMessage());
    expect(screen.getByTitle('Edit message')).toBeInTheDocument();
    expect(screen.getByTitle('Copy message')).toBeInTheDocument();
  });

  it('renders no actions at all in read-only hosts', () => {
    render(
      <MessageActionsProvider actions={{}}>
        <MessageBubble message={makeMessage()} turnIndex={0} isTurnTail={false} readOnly />
      </MessageActionsProvider>,
    );
    expect(screen.queryByTitle('Copy message')).toBeNull();
  });
});

describe('MessageBubble — long user message collapse', () => {
  let restore: () => void;
  afterEach(() => restore?.());

  it('bounds tall user content behind Show all, expandable to Show less', () => {
    restore = stubScrollHeight(999);
    renderBubble(makeMessage({ content: 'a very long message' }));
    const toggle = screen.getByTestId('overflow-collapse-toggle');
    expect(toggle).toHaveTextContent('Show all');
    fireEvent.click(toggle);
    expect(toggle).toHaveTextContent('Show less');
  });

  it('shows no toggle when the content fits', () => {
    restore = stubScrollHeight(100);
    renderBubble(makeMessage());
    expect(screen.queryByTestId('overflow-collapse-toggle')).toBeNull();
  });

  it('never collapses assistant messages', () => {
    restore = stubScrollHeight(999);
    renderBubble(makeMessage({ id: 'm-2', role: 'assistant', content: 'a very long answer' }));
    expect(screen.queryByTestId('overflow-collapse-toggle')).toBeNull();
  });
});

describe('MessageBubble — Sources pill while streaming', () => {
  const provenance = {
    r1: {
      record_id: 'r1',
      timestamp: '2026-08-01T00:00:00Z',
      source_type: 'web_fetch',
      identifier: 'https://example.com/report',
    },
  };
  const assistant = (isStreaming: boolean) =>
    makeMessage({ id: 'a-1', role: 'assistant', content: 'Margins widened.', isStreaming, provenanceRecords: provenance });

  it('keeps the faded pill out of the tab order until the turn finishes', () => {
    const { rerender } = renderBubble(assistant(true));
    const pill = screen.getByTitle('Sources');
    // Faded to opacity 0 mid-turn: a keyboard user must not land on it.
    expect(pill.closest('[inert]')).not.toBeNull();
    rerender(
      <MessageActionsProvider actions={{ onEditMessage: vi.fn() }}>
        <MessageBubble message={assistant(false)} turnIndex={0} isTurnTail={false} />
      </MessageActionsProvider>,
    );
    expect(screen.getByTitle('Sources').closest('[inert]')).toBeNull();
  });
});
