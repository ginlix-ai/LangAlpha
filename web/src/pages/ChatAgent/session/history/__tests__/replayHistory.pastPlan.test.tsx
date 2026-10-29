/**
 * A plan an older thread submitted still reads back on reload.
 *
 * The agent no longer has `SubmitPlan`, but threads written before it lost the
 * tool still carry the call (its `description` is the plan), the review
 * interrupt, the resume that answered it, and the call's result. The plan has
 * to show as a record with its outcome, and nothing about it may come back to
 * life: no card to answer, nothing armed for the composer. A turn that held
 * only the plan must not paint as a blank bubble either.
 */
import React from 'react';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import '@testing-library/jest-dom';
import { within } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';

const api = vi.hoisted(() => ({ replayThreadHistory: vi.fn() }));

vi.mock('../../../utils/api', () => ({
  replayThreadHistory: api.replayThreadHistory,
}));

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
    ...await vi.importActual<typeof import('@/lib/framer')>('@/lib/framer'),
    motion: new Proxy({} as Record<string, unknown>, {
      get: (_t, key: string) => (key === 'create' ? make : make(key)),
    }),
    AnimatePresence: ({ children }: { children?: React.ReactNode }) =>
      ReactActual.createElement(ReactActual.Fragment, null, children),
    animate: () => ({ stop: () => {} }),
  };
});

vi.mock('../../../components/Markdown', () => ({
  default: ({ content }: { content: string }) => <div data-testid="markdown-content">{content}</div>,
}));

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'light', setTheme: () => {} }),
}));

import { loadConversationHistory } from '../replayHistory';
import MessageList from '../../../components/MessageList';
import type { MessageRecord } from '../../../components/messageList/types';
import { buildRuntime, makeDeps, replayOf } from './replayHarness';

const PLAN = '## Plan\n1. Pull the 10-K\n2. Chart revenue by segment';
const CALL_ID = 'call-plan-1';

const user = (turn: number, content: string, metadata?: Record<string, unknown>) => ({
  event: 'user_message',
  data: { thread_id: 'thread-1', turn_index: turn, content, ...(metadata ? { metadata } : {}) },
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

const submitPlan = (turn: number, name = 'SubmitPlan') => toolCall(turn, CALL_ID, name, { description: PLAN });

const review = (turn: number) => ({
  event: 'interrupt',
  data: {
    thread_id: 'thread-1',
    turn_index: turn,
    interrupt_id: 'int-plan-1',
    action_requests: [{ name: 'SubmitPlan', args: { description: PLAN }, description: PLAN }],
  },
});

async function replay(items: Array<Record<string, unknown>>) {
  const { rt, read } = buildRuntime();
  api.replayThreadHistory.mockImplementation(replayOf(items));
  await loadConversationHistory(rt, makeDeps());
  const { container } = renderWithProviders(
    <MessageList messages={read() as unknown as MessageRecord[]} isLoading={false} />,
  );
  return { rt, container };
}

const bubble = (container: HTMLElement, id: string) =>
  container.querySelector<HTMLElement>(`[data-message-id="${id}"]`);

/** The plan reads back, and nothing about it can be acted on again. */
function expectSettledPlan(
  rt: Awaited<ReturnType<typeof replay>>['rt'],
  container: HTMLElement,
  label: string,
) {
  const planBubble = bubble(container, 'history-assistant-0');
  expect(planBubble).not.toBeNull();
  const scope = within(planBubble!);
  expect(scope.getByText(label)).toBeInTheDocument();
  expect(scope.getByText(/Pull the 10-K/)).toBeInTheDocument();
  expect(scope.queryByRole('button', { name: /^(approve|reject)$/i })).toBeNull();
  expect(rt.historyHasUnresolvedInterruptRef.current).toBe(false);
  expect(rt.unresolvedHistoryInterruptRef.current).toEqual([]);
}

beforeEach(() => vi.clearAllMocks());

describe('history replay: a past plan', () => {
  it('shows an approved plan as approved', async () => {
    const { rt, container } = await replay([
      user(0, 'Analyse the filing'),
      text(0, 'Here is how I will go about it.'),
      submitPlan(0),
      review(0),
      // A bare approve: the resume names the interrupt and says nothing.
      // Results streamed before tool status was recorded carry none.
      user(1, '', { hitl_interrupt_ids: ['int-plan-1'] }),
      result(1, CALL_ID, 'Plan approved. Proceed with execution.'),
      text(1, 'Pulling the filing now.'),
    ]);

    expectSettledPlan(rt, container, 'Plan approved');
  });

  it('reads a plan under the name the tool had before the PascalCase rename', async () => {
    const { rt, container } = await replay([
      user(0, 'Analyse the filing'),
      submitPlan(0, 'submit_plan'),
      review(0),
      user(1, '', { hitl_interrupt_ids: ['int-plan-1'] }),
      result(1, CALL_ID, 'Plan approved. Proceed with execution.', 'success'),
    ]);

    expectSettledPlan(rt, container, 'Plan approved');
  });

  it('shows a plan rejected with feedback as rejected, the feedback below it', async () => {
    const { rt, container } = await replay([
      user(0, 'Analyse the filing'),
      submitPlan(0),
      review(0),
      user(1, 'Use the 10-Q instead.', { hitl_interrupt_ids: ['int-plan-1'] }),
      // No status, so the error flag is not there to say it was a rejection.
      result(1, CALL_ID, 'User rejected the tool call for `SubmitPlan` with reason: Use the 10-Q instead.'),
      text(1, 'Switching to the 10-Q.'),
    ]);

    expectSettledPlan(rt, container, 'Plan rejected');
    expect(within(bubble(container, 'history-user-1')!).getByText('Use the 10-Q instead.')).toBeInTheDocument();
  });

  it('shows a plan rejected without feedback as rejected', async () => {
    const { rt, container } = await replay([
      user(0, 'Analyse the filing'),
      submitPlan(0),
      review(0),
      user(1, '', { hitl_interrupt_ids: ['int-plan-1'], hitl_answers: { 'int-plan-1': null } }),
      result(1, CALL_ID, `User rejected the tool call for \`SubmitPlan\` with id ${CALL_ID}`, 'error'),
    ]);

    expectSettledPlan(rt, container, 'Plan rejected');
  });

  it('shows a plan nobody answered as not reviewed, and arms nothing', async () => {
    // The thread stopped on the review, and the turn holds nothing but the
    // plan: it reads as a record, not as a blank bubble.
    const { rt, container } = await replay([
      user(0, 'Analyse the filing'),
      submitPlan(0),
      review(0),
    ]);

    expectSettledPlan(rt, container, 'Plan not reviewed');
  });

  it('does not call a plan approved when a later message cancelled its call', async () => {
    // A new message on a thread left on the review closes the dangling call
    // with a cancellation, which is not an approval.
    const { rt, container } = await replay([
      user(0, 'Analyse the filing'),
      submitPlan(0),
      review(0),
      user(1, 'Never mind, just summarize it.'),
      result(1, CALL_ID, `Tool call SubmitPlan with id ${CALL_ID} was cancelled - another message came in before it could be completed.`, 'success'),
      text(1, 'Here is the summary.'),
    ]);

    expectSettledPlan(rt, container, 'Plan not reviewed');
  });

  it('reads a call the runtime closed as not completed, error status and all, as not reviewed', async () => {
    // Newer runtimes word the same patch differently and stamp it as an
    // error, which must not read as a rejection.
    const { rt, container } = await replay([
      user(0, 'Analyse the filing'),
      submitPlan(0),
      review(0),
      user(1, 'Never mind, just summarize it.'),
      result(1, CALL_ID, `Tool call SubmitPlan with id ${CALL_ID} did not complete - no result was recorded. It may have been cancelled or interrupted.`, 'error'),
      text(1, 'Here is the summary.'),
    ]);

    expectSettledPlan(rt, container, 'Plan not reviewed');
  });
});
