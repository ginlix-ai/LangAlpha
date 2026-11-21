/**
 * A shared workspace's thread reads as a group chat: the reader's own
 * messages at the end, another member's at the start under their name and
 * picture (once per run of theirs), the agent's replies as they always were.
 * Every other thread is drawn as before, and nothing offers a rewrite of
 * another member's turn, which the server refuses.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { screen, within } from '@testing-library/react';
import MessageList from '../../MessageList';
import { MessageActionsProvider, type MessageActions } from '../MessageActionsContext';
import { SharedThreadContext } from '../SharedThreadContext';
import { threadTurns, type SharedThreadView } from '../../../session/sharedThread';
import type { ChatMessage } from '@/types/chat';
import type { MessageRecord } from '../types';
import type { ShownPlan } from '../requestPlans';
import { renderWithProviders } from '@/test/utils';

vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: null }),
}));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark' }),
}));

const ME = { userId: 'u-lee', name: 'Lee Chen' };
const MAYA = { userId: 'u-maya', name: 'Maya Okafor' };
const SAM = { userId: 'u-sam', name: 'Sam Rivera' };
const AT = new Date('2026-11-01T12:00:00Z');

type Sender = typeof ME;

const user = (id: string, turnIndex: number, sender: Sender | null, extra: Record<string, unknown> = {}): MessageRecord => ({
  id, role: 'user', content: `${id} text`, contentType: 'text', timestamp: AT, isStreaming: false, isHistory: true, turnIndex,
  ...(sender ? { sender } : {}), ...extra,
});
const reply = (id: string, turnIndex: number, extra: Record<string, unknown> = {}): MessageRecord => ({
  id, role: 'assistant', content: `${id} reply`, contentType: 'text', timestamp: AT, isStreaming: false, isHistory: true, turnIndex,
  completedAt: AT.toISOString(), ...extra,
});

const ACTIONS: MessageActions = { onEditMessage: vi.fn(), onRegenerate: vi.fn(), onRetry: vi.fn() };

function renderThread(messages: MessageRecord[], { shared = true, plans }: { shared?: boolean; plans?: ReadonlyMap<string, ShownPlan> } = {}) {
  const view: SharedThreadView | undefined = shared
    ? { me: ME, avatars: {}, names: {}, ...threadTurns(messages as unknown as ChatMessage[], ME.userId, false) }
    : undefined;
  return renderWithProviders(
    <MessageActionsProvider actions={ACTIONS}>
      <SharedThreadContext value={view}>
        <MessageList messages={messages} plans={plans} />
      </SharedThreadContext>
    </MessageActionsProvider>,
  );
}

const row = (container: HTMLElement, id: string) => {
  const el = container.querySelector<HTMLElement>(`[data-message-id="${id}"]`);
  if (!el) throw new Error(`no row ${id}`);
  return el;
};
/** The sender's name drawn over the bubble, not the one said to a screen reader. */
const shownName = (el: HTMLElement) =>
  [...el.querySelectorAll<HTMLElement>('[data-testid="message-sender"]')].find((s) => !s.classList.contains('sr-only')) ?? null;

describe('shared thread: placement', () => {
  const thread = [
    user('maya-0', 0, MAYA),
    reply('reply-0', 0),
    user('mine-1', 1, ME),
    reply('reply-1', 1),
  ];

  it("puts another member's message at the start, under their name and picture", () => {
    const { container } = renderThread(thread);
    const theirs = row(container, 'maya-0');
    expect(theirs.dataset.sender).toBe('theirs');
    expect(theirs.className).toContain('justify-start');
    expect(theirs.className).not.toContain('justify-end');
    expect(shownName(theirs)).toHaveTextContent('Maya Okafor');
    expect(within(theirs).getByTestId('message-sender-avatar')).toHaveTextContent('MO');
    expect(theirs.querySelector('.pf-bubble-theirs')).toHaveTextContent('maya-0 text');
  });

  it("keeps the reader's own at the end in the usual bubble, its sender said only to a screen reader", () => {
    const { container } = renderThread(thread);
    const mine = row(container, 'mine-1');
    expect(mine.dataset.sender).toBeUndefined();
    expect(mine.className).toContain('justify-end');
    expect(mine.querySelector('.pf-bubble-theirs')).toBeNull();
    expect(mine.querySelector('[data-testid="message-sender-avatar"]')).toBeNull();
    expect(shownName(mine)).toBeNull();
    expect(within(mine).getByTestId('message-sender')).toHaveClass('sr-only');
    expect(within(mine).getByTestId('message-sender')).toHaveTextContent('You');
  });

  it("leaves the agent's replies as they were, on the page with no bubble", () => {
    const { container } = renderThread(thread);
    for (const id of ['reply-0', 'reply-1']) {
      const el = row(container, id);
      expect(el.dataset.messageRole).toBe('assistant');
      expect(el.dataset.sender).toBeUndefined();
      expect(el.querySelector('.pf-bubble')).toBeNull();
      expect(el.querySelector('[data-testid="message-sender"]')).toBeNull();
    }
  });

  it("puts a steer another member sent at the start too, marked as steered", () => {
    const { container } = renderThread([
      user('mine-0', 0, ME),
      reply('reply-0a', 0),
      user('maya-steer', 0, { userId: MAYA.userId, name: null } as unknown as Sender, { steeringDelivered: true, isHistory: false }),
      reply('reply-0b', 0),
    ]);
    const steer = row(container, 'maya-steer');
    expect(steer.dataset.sender).toBe('theirs');
    expect(within(steer).getByText('Steered')).toBeInTheDocument();
    // Named by id alone, it reads as someone in the workspace.
    expect(shownName(steer)).toHaveTextContent('Someone');
  });
});

describe('shared thread: runs of one member', () => {
  it('names the first message of a run and folds the name and picture into it for the rest', () => {
    const { container } = renderThread([
      user('maya-1', 0, MAYA),
      user('maya-2', 0, MAYA, { steeringDelivered: true }),
      user('sam-1', 0, SAM, { steeringDelivered: true }),
      reply('reply-0', 0),
      user('maya-3', 1, MAYA),
      reply('reply-1', 1),
    ]);
    const first = row(container, 'maya-1');
    const second = row(container, 'maya-2');
    expect(shownName(first)).toHaveTextContent('Maya Okafor');
    expect(within(first).getByTestId('message-sender-avatar')).toHaveTextContent('MO');
    // The rest of the run: no name or picture, the picture's room kept so the
    // bubbles line up, still said to a screen reader, and stacked closer.
    expect(shownName(second)).toBeNull();
    expect(within(second).getByTestId('message-sender-avatar')).toBeEmptyDOMElement();
    expect(within(second).getByTestId('message-sender')).toHaveClass('sr-only');
    expect(within(second).getByTestId('message-sender')).toHaveTextContent('Maya Okafor');
    expect(second.parentElement?.style.marginTop).toBe('8px');
    expect(first.parentElement?.style.marginTop).toBe('');
    // Another member breaks the run, and so does a reply between.
    expect(shownName(row(container, 'sam-1'))).toHaveTextContent('Sam Rivera');
    expect(row(container, 'sam-1').parentElement?.style.marginTop).toBe('');
    expect(shownName(row(container, 'maya-3'))).toHaveTextContent('Maya Okafor');
  });

  it('ends the run at a plan drawn under the message above', () => {
    const plan: ShownPlan = { steps: [{ title: 'Pull the filings', status: 'in_progress' }], state: 'ended' };
    const { container } = renderThread([
      user('maya-1', 0, MAYA),
      user('maya-2', 0, MAYA, { steeringDelivered: true }),
      reply('reply-0', 0),
    ], { plans: new Map([['maya-1', plan]]) });
    expect(shownName(row(container, 'maya-2'))).toHaveTextContent('Maya Okafor');
    expect(row(container, 'maya-2').parentElement?.style.marginTop).toBe('');
  });

  it("never runs the reader's own messages together with anyone's", () => {
    const { container } = renderThread([
      user('maya-1', 0, MAYA),
      user('mine-1', 0, ME, { steeringDelivered: true }),
      user('maya-2', 0, MAYA, { steeringDelivered: true }),
      reply('reply-0', 0),
    ]);
    expect(row(container, 'mine-1').parentElement?.style.marginTop).toBe('');
    expect(shownName(row(container, 'maya-2'))).toHaveTextContent('Maya Okafor');
  });
});

describe('personal threads', () => {
  it('draw every message as before: at the end, no sender, no run', () => {
    const { container } = renderThread([
      user('u-1', 0, null),
      user('u-2', 0, null, { steeringDelivered: true }),
      reply('reply-0', 0),
    ], { shared: false });
    for (const id of ['u-1', 'u-2']) {
      const el = row(container, id);
      expect(el).toHaveClass('justify-end');
      expect(el).not.toHaveClass('justify-start');
      expect(el.dataset.sender).toBeUndefined();
      expect(el.parentElement?.style.marginTop).toBe('');
      expect(el.querySelector('.pf-bubble-theirs')).toBeNull();
    }
    expect(container.querySelector('[data-testid="message-sender"], [data-testid="message-sender-avatar"]')).toBeNull();
    expect(within(row(container, 'u-1')).getByTitle('Edit message')).toBeInTheDocument();
  });
});

describe("shared thread: no rewrite of another member's turn", () => {
  it("offers copy but no edit on another member's message, and edit on the reader's own latest", () => {
    const { container } = renderThread([
      user('maya-0', 0, MAYA),
      reply('reply-0', 0),
      user('mine-1', 1, ME),
      reply('reply-1', 1),
    ]);
    const theirs = row(container, 'maya-0');
    expect(within(theirs).getByTitle('Copy message')).toBeInTheDocument();
    expect(within(theirs).queryByTitle('Edit message')).toBeNull();
    expect(within(row(container, 'mine-1')).getByTitle('Edit message')).toBeInTheDocument();
    // Regenerate is offered on the reader's own turn only.
    expect(within(row(container, 'reply-0')).queryByTitle('Regenerate response')).toBeNull();
    expect(within(row(container, 'reply-1')).getByTitle('Regenerate response')).toBeInTheDocument();
  });

  it("offers no retry on a failed reply to another member's turn, and retry on the reader's own", () => {
    const failed = { error: true, structuredError: { message: 'boom' } };
    const theirs = renderThread([user('maya-0', 0, MAYA), reply('reply-0', 0, failed)]);
    expect(within(row(theirs.container, 'reply-0')).queryByTitle('Retry')).toBeNull();
    expect(within(row(theirs.container, 'maya-0')).queryByTitle('Edit message')).toBeNull();
    theirs.unmount();

    const mine = renderThread([user('mine-0', 0, ME), reply('reply-0', 0, failed)]);
    expect(within(row(mine.container, 'reply-0')).getByTitle('Retry')).toBeInTheDocument();
  });
});

describe('shared thread: copy action row', () => {
  it("lines another member's actions up at the start under their bubble", () => {
    const { container } = renderThread([user('maya-0', 0, MAYA), reply('reply-0', 0)]);
    const copy = within(row(container, 'maya-0')).getByTitle('Copy message');
    expect(copy.parentElement?.className).toContain('justify-start');
    expect(screen.getAllByTitle('Copy message').length).toBeGreaterThan(1);
  });
});
