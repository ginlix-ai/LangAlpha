/**
 * Turn projection: UI position → backend turn_index.
 *
 * Assistant bubbles are never pruned from state (their positional count IS the
 * backend turn_index that edit/regenerate/feedback address), so the projection
 * runs over the RAW array and only visibility filtering happens afterward. The
 * bug this locks: the regenerate affordance used to be computed over the raw
 * list, so a settled-empty steering continuation — hidden at render — stole it
 * from the last bubble the user can actually see.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { act, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import MessageList from '../MessageList';
import { MessageActionsProvider, type MessageActions } from '../messageList/MessageActionsContext';
import { computeTurnTails, newestTurn, projectTurns } from '../messageList/turnProjection';
import { visibleProjection } from '../messageList/contentProjection';
import type { MessageRecord } from '../messageList/types';

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

type Msg = Record<string, unknown>;

const assistant = (id: string, overrides: Msg = {}): Msg => ({
  id,
  role: 'assistant',
  content: 'answer',
  contentType: 'text',
  timestamp: new Date(),
  isStreaming: false,
  contentSegments: [{ type: 'text', content: 'answer', order: 0 }],
  reasoningProcesses: {},
  toolCallProcesses: {},
  ...overrides,
});

/** Settled with nothing to paint — stays in state, hidden at render. */
const emptyAssistant = (id: string, overrides: Msg = {}): Msg =>
  assistant(id, { content: '', contentSegments: [], ...overrides });

const userMsg = (id: string, overrides: Msg = {}): Msg => ({
  id, role: 'user', content: 'ask', contentType: 'text', timestamp: new Date(), isStreaming: false, ...overrides,
});

const bubble = (container: HTMLElement, id: string) =>
  container.querySelector(`[data-message-id="${id}"]`);

const regenerateBtn = (container: HTMLElement, id: string) =>
  bubble(container, id)?.querySelector('[title="chat.actions.regenerate"]') ?? null;

// The two formulas the projection replaced, verbatim in shape: the edit path
// counted non-steering assistants BEFORE a user message; the feedback path
// counted up to AND INCLUDING an assistant bubble, minus one.
const legacyEditTurnIndex = (messages: Msg[], rawIndex: number) =>
  messages.slice(0, rawIndex).filter((m) => m.role === 'assistant' && !m.isSteering).length;

const legacyFeedbackTurnIndex = (messages: Msg[], rawIndex: number) =>
  messages.slice(0, rawIndex + 1).filter((m) => m.role === 'assistant' && !m.isSteering).length - 1;

function renderList(messages: Msg[], actions: MessageActions) {
  return renderWithProviders(
    <MessageActionsProvider actions={actions}>
      <MessageList messages={messages as MessageRecord[]} isLoading={false} />
    </MessageActionsProvider>,
  );
}

describe('turnProjection — raw turn semantics', () => {
  it('maps edit/regenerate turns across hidden steering and non-steering bubbles', () => {
    const messages: Msg[] = [
      userMsg('u0'),
      assistant('a0'),
      userMsg('u1-steering', { steeringDelivered: true }),
      assistant('a0-cont', { isSteering: true }),
      userMsg('u1'),
      emptyAssistant('a1-empty'), // hidden at render, still a backend turn
      userMsg('u2'),
      assistant('a2'),
    ];
    const projected = projectTurns(messages as MessageRecord[]);

    expect(projected.map((p) => p.turnIndex)).toEqual([0, 0, 1, 0, 1, 1, 2, 2]);

    // Every user bubble matches the edit path's exclusive count; every assistant
    // bubble matches the feedback path's inclusive-minus-one count.
    projected.forEach(({ message, rawIndex, turnIndex }) => {
      if (message.role === 'user') expect(turnIndex).toBe(legacyEditTurnIndex(messages, rawIndex));
      else expect(turnIndex).toBe(legacyFeedbackTurnIndex(messages, rawIndex));
    });

    // Hiding the empty bubble must not shift anyone's turn.
    const visible = visibleProjection(projected);
    expect(visible.map((p) => p.message.id)).toEqual(['u0', 'a0', 'u1-steering', 'a0-cont', 'u1', 'u2', 'a2']);
    expect(visible.map((p) => p.turnIndex)).toEqual([0, 0, 1, 0, 1, 2, 2]);
  });

  it('attributes a steering continuation to the turn it continues (feedback path)', () => {
    const messages: Msg[] = [
      userMsg('u0'),
      assistant('a0'),
      userMsg('u1-steering', { steeringDelivered: true }),
      assistant('a1-continuation', { isSteering: true }),
    ];
    const projected = projectTurns(messages as MessageRecord[]);
    const cont = projected[3];

    expect(cont.turnIndex).toBe(0);
    // Same answer today's useChatFeedback derivation gives.
    expect(cont.turnIndex).toBe(legacyFeedbackTurnIndex(messages, 3));
  });

  it('marks only the last VISIBLE bubble of each turn as the tail', () => {
    const messages: Msg[] = [
      userMsg('u0'),
      assistant('a0'),
      assistant('a0-cont', { isSteering: true }),
      userMsg('u1'),
      assistant('a1'),
    ];
    const visible = visibleProjection(projectTurns(messages as MessageRecord[]));
    // [u0, a0, a0-cont, u1, a1] → a0 is mid-turn, a0-cont and a1 are tails.
    expect(computeTurnTails(visible)).toEqual([false, false, true, false, true]);
  });
});

// A paged transcript opens mid-thread, so position alone would name turn 0 for
// its first bubble. The chat view stamps every bubble with its backend turn.
describe('turnProjection — stamped turns on a paged transcript', () => {
  const page: Msg[] = [
    userMsg('history-user-75', { turnIndex: 75 }),
    assistant('history-assistant-75', { turnIndex: 75 }),
    userMsg('history-user-76', { turnIndex: 76 }),
    assistant('history-assistant-76', { turnIndex: 76 }),
  ];
  const turns = (messages: Msg[]) => projectTurns(messages as MessageRecord[]).map((p) => p.turnIndex);

  it('keeps the turns a page that starts at turn 75 is stamped with', () => {
    expect(turns(page)).toEqual([75, 75, 76, 76]);
  });

  it('continues an unstamped bubble from the last stamped turn', () => {
    expect(turns([...page, userMsg('user-live'), assistant('assistant-live')])).toEqual([75, 75, 76, 76, 77, 77]);
  });

  it('keeps a stamped steering bubble on the turn it steered without opening one', () => {
    const steered: Msg[] = [
      ...page.slice(0, 2),
      userMsg('history-steering-user-75-0-0', { turnIndex: 75, steeringDelivered: true }),
      assistant('history-assistant-steering-75-0', { turnIndex: 75, isSteering: true }),
      userMsg('user-live'),
      assistant('assistant-live'),
    ];
    expect(turns(steered)).toEqual([75, 75, 75, 75, 76, 76]);
  });

  it('folds an unstamped steering continuation into the stamped turn it continues', () => {
    expect(turns([...page.slice(0, 2), assistant('a75-cont', { isSteering: true })])).toEqual([75, 75, 75]);
  });

  it('ends a turn a reconnect continued on its last bubble, not on each', () => {
    // A dropped stream resumes on a bubble of its own, on the turn it continues.
    const continued: Msg[] = [
      ...page.slice(0, 2),
      assistant('assistant-reconnect-75', { turnIndex: 75 }),
      ...page.slice(2),
    ];
    const visible = visibleProjection(projectTurns(continued as MessageRecord[]));
    expect(computeTurnTails(visible)).toEqual([false, false, true, false, true]);
  });

  it('names the newest turn by its assistant bubbles alone', () => {
    expect(newestTurn(page as MessageRecord[])).toBe(76);
    // A parked message has not opened its turn yet.
    expect(newestTurn([...page, userMsg('parked')] as MessageRecord[])).toBe(76);
    expect(newestTurn([userMsg('first')] as MessageRecord[])).toBeNull();
    expect(newestTurn([])).toBeNull();
  });
});

const retryBtn = (container: HTMLElement, id: string) =>
  bubble(container, id)?.querySelector('[title="chat.actions.retry"]') ?? null;

describe('MessageList — a reply replay could not load', () => {
  const failure = { message: 'This response could not be loaded.', kind: 'internal' };

  it('offers a Regenerate, since its run did not fail and the server refuses a Retry', () => {
    const messages: Msg[] = [
      userMsg('u0'),
      emptyAssistant('a0', { error: true, unloaded: true, structuredError: failure }),
    ];
    const { container } = renderList(messages, { onRegenerate: vi.fn(), onRetry: vi.fn() });

    expect(retryBtn(container, 'a0')).toBeNull();
    expect(regenerateBtn(container, 'a0')).not.toBeNull();
  });

  it('keeps the Retry on a reply whose run failed', () => {
    const messages: Msg[] = [
      userMsg('u0'),
      emptyAssistant('a0', { error: true, structuredError: failure }),
    ];
    const { container } = renderList(messages, { onRegenerate: vi.fn(), onRetry: vi.fn() });

    expect(retryBtn(container, 'a0')).not.toBeNull();
    expect(regenerateBtn(container, 'a0')).toBeNull();
  });
});

describe('MessageList — a turn that opened hidden', () => {
  // Its words rode its send's context, which the checkpoint a regenerate
  // replays does not hold, so no bubble of it offers one.
  it('offers no Regenerate on its reply, nor on the tail a steer split off it', () => {
    const messages: Msg[] = [
      assistant('a0', { turnIndex: 0, hiddenOpener: true }),
      userMsg('u-steer', { turnIndex: 0, steeringDelivered: true }),
      assistant('a0-cont', { turnIndex: 0, isSteering: true }),
      userMsg('u1', { turnIndex: 1 }),
      assistant('a1', { turnIndex: 1 }),
    ];
    const { container } = renderList(messages, { onRegenerate: vi.fn() });

    expect(regenerateBtn(container, 'a0')).toBeNull();
    expect(regenerateBtn(container, 'a0-cont')).toBeNull();
    // The turns after it are ordinary ones.
    expect(regenerateBtn(container, 'a1')).not.toBeNull();
  });

  it('keeps the Retry of its failed reply', () => {
    const messages: Msg[] = [emptyAssistant('a0', { turnIndex: 0, hiddenOpener: true, error: true })];
    const { container } = renderList(messages, { onRegenerate: vi.fn(), onRetry: vi.fn() });

    expect(regenerateBtn(container, 'a0')).toBeNull();
    expect(bubble(container, 'a0')?.querySelector('[title="chat.actions.retry"]')).not.toBeNull();
  });
});

describe('MessageList — regenerate lands on the last visible bubble', () => {
  it('keeps regenerate on the previous visible bubble when the steering continuation settled empty', () => {
    const onRegenerate = vi.fn();
    const messages: Msg[] = [
      userMsg('u0'),
      assistant('a0'),
      userMsg('u1-steering', { steeringDelivered: true }),
      // Steering resumed but its content landed on a0 — nothing to paint.
      emptyAssistant('a0-cont-empty', { isSteering: true }),
    ];
    const { container } = renderList(messages, { onRegenerate });

    expect(bubble(container, 'a0-cont-empty')).toBeNull();
    // Without the visible-list tail scan, the hidden bubble held the affordance
    // and the transcript showed no regenerate at all.
    expect(regenerateBtn(container, 'a0')).not.toBeNull();

    fireEvent.click(regenerateBtn(container, 'a0') as HTMLElement);
    expect(onRegenerate).toHaveBeenCalledWith('a0');
  });

  it('gives a painted steering continuation the regenerate, not the bubble it continues', () => {
    const messages: Msg[] = [
      userMsg('u0'),
      assistant('a0'),
      userMsg('u1-steering', { steeringDelivered: true }),
      assistant('a0-cont', { isSteering: true }),
    ];
    const { container } = renderList(messages, { onRegenerate: vi.fn() });

    expect(regenerateBtn(container, 'a0')).toBeNull();
    expect(regenerateBtn(container, 'a0-cont')).not.toBeNull();
  });

  it('rates a steering continuation against the turn it continues', async () => {
    const onThumbUp = vi.fn().mockResolvedValue(null);
    const messages: Msg[] = [
      userMsg('u0'),
      assistant('a0'),
      userMsg('u1-steering', { steeringDelivered: true }),
      assistant('a1-continuation', { isSteering: true }),
    ];
    const { container } = renderList(messages, { onThumbUp });

    const thumb = bubble(container, 'a1-continuation')?.querySelector('[title="chat.actions.goodResponse"]');
    expect(thumb).not.toBeNull();
    await act(async () => { fireEvent.click(thumb as HTMLElement); });

    expect(onThumbUp).toHaveBeenCalledWith(0);
  });

  it('offers no edit pencil on a steering user bubble', () => {
    const messages: Msg[] = [
      userMsg('u0'),
      assistant('a0'),
      userMsg('u1-steering', { steeringDelivered: true }),
    ];
    const { container } = renderList(messages, { onEditMessage: vi.fn() });

    expect(bubble(container, 'u0')?.querySelector('[title="chat.actions.editMessage"]')).not.toBeNull();
    expect(bubble(container, 'u1-steering')?.querySelector('[title="chat.actions.editMessage"]')).toBeNull();
  });
});

// A send the server refused or never got holds no turn: the next send takes
// the number the view first gave it, and the two must not share a turn.
describe('turnProjection — a send the server never stored', () => {
  const refused: Msg[] = [
    userMsg('u0', { turnIndex: 0 }),
    assistant('a0', { turnIndex: 0 }),
    userMsg('u-refused', { turnIndex: 1, unadmitted: true }),
    assistant('a-refused', { turnIndex: 1, unadmitted: true, error: true }),
    userMsg('u1', { turnIndex: 1 }),
    assistant('a1', { turnIndex: 1 }),
  ];

  it('gives it no turn and leaves the next send its number', () => {
    expect(projectTurns(refused as MessageRecord[]).map((p) => p.turnIndex)).toEqual([0, 0, null, null, 1, 1]);
    expect(newestTurn(refused as MessageRecord[])).toBe(1);
  });

  it('keeps it out of the turn after it, so regenerate there cuts only that turn', () => {
    const visible = visibleProjection(projectTurns(refused as MessageRecord[]));
    expect(computeTurnTails(visible)).toEqual([false, true, false, true, false, true]);
  });
});

describe('MessageList — Retry on the newest reply only', () => {
  const retryBtn = (container: HTMLElement, id: string) =>
    bubble(container, id)?.querySelector('[title="chat.actions.retry"]') ?? null;
  const failed = (id: string, turnIndex: number) =>
    assistant(id, { turnIndex, error: true, content: '', contentSegments: [], structuredError: { message: 'Failed to send message. Please try again.' } });

  it('offers Retry on the newest failed reply and not on an older one', () => {
    const { container } = renderList(
      [userMsg('u0', { turnIndex: 0 }), failed('a0', 0), userMsg('u1', { turnIndex: 1 }), failed('a1', 1)],
      { onRetry: vi.fn() },
    );
    expect(retryBtn(container, 'a0')).toBeNull();
    expect(retryBtn(container, 'a1')).not.toBeNull();
  });

  it('offers none when a message was sent after the failed reply', () => {
    const { container } = renderList(
      [userMsg('u0', { turnIndex: 0 }), failed('a0', 0), userMsg('u-refused', { turnIndex: 1, unadmitted: true })],
      { onRetry: vi.fn() },
    );
    expect(retryBtn(container, 'a0')).toBeNull();
  });
});

describe('MessageList: a failed turn', () => {
  const PROSE = 'Revenue grew 12% on the quarter.';
  const FAILURE = 'Error code: 503 - overloaded';
  const streamedThenFailed = (overrides: Msg = {}) => assistant('a0', {
    turnIndex: 0,
    error: true,
    content: PROSE,
    contentSegments: [{ type: 'text', content: PROSE, order: 0 }],
    ...overrides,
  });
  const prose = (container: HTMLElement) =>
    [...(bubble(container, 'a0')?.querySelectorAll('[data-testid="markdown-content"]') ?? [])].map((el) => el.textContent);

  it('keeps what it streamed as its reply and shows the failure once, after it', () => {
    const { container, getAllByText } = renderList(
      [userMsg('u0', { turnIndex: 0 }), streamedThenFailed({ structuredError: { message: FAILURE, kind: 'upstream', statusCode: 503 } })],
      { onRetry: vi.fn() },
    );
    expect(prose(container)).toEqual([PROSE]);
    expect(getAllByText(FAILURE)).toHaveLength(1);
    expect(getAllByText('chat.errorUpstreamHeadlineStatus')).toHaveLength(1);
    expect(bubble(container, 'a0')?.querySelector('[title="chat.actions.retry"]')).not.toBeNull();
  });

  it('draws no card for a failure the banner reports', () => {
    const { container, queryByText } = renderList(
      [userMsg('u0', { turnIndex: 0 }), streamedThenFailed()],
      { onRetry: vi.fn() },
    );
    expect(prose(container)).toEqual([PROSE]);
    expect(queryByText('chat.errorInternalHeadline')).toBeNull();
    expect(bubble(container, 'a0')?.querySelector('[title="chat.actions.retry"]')).not.toBeNull();
  });

  it('shows the card in place of a reply when the turn produced nothing', () => {
    const { container, getAllByText } = renderList(
      [userMsg('u0', { turnIndex: 0 }), streamedThenFailed({ content: '', contentSegments: [], structuredError: { message: '', kind: 'internal' } })],
      { onRetry: vi.fn() },
    );
    expect(prose(container)).toEqual([]);
    expect(getAllByText('chat.errorInternalHeadline')).toHaveLength(1);
  });
});
