/**
 * Which wake turns fold into the quiet checks' row. A turn that ended quietly
 * and changed nothing folds with the quiet turns beside it; one that set or
 * cancelled a wakeup on the way shows as any turn does; one still running
 * shows until it ends, then joins the row above it.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import '@testing-library/jest-dom';
import { fireEvent } from '@testing-library/react';
import { renderWithProviders } from '@/test/utils';
import MessageList from '../MessageList';
import { MessageActionsProvider } from '../messageList/MessageActionsContext';
import type { MessageRecord } from '../messageList/types';
import { deliveredNoticeRow } from '../../session/wakeupEvents';

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
const at = (m: number) => new Date(Date.UTC(2030, 0, 2, 14, m));

// Made-up labels only.
function wakeTurn(turn: number, args: Record<string, unknown>, over: Msg = {}): Msg[] {
  const notice = {
    wakeup_id: `wk-${turn}`, short_id: `w_${turn}`, label: `check ${turn}`, kind: 'at', notice: 'fire',
    seq: 1, total: 1, last: true, skipped: 0, fired_at: at(turn).toISOString(),
  };
  return [
    {
      id: `history-wakeup-wake-${turn}`, role: 'notification', content: '', variant: 'info', timestamp: at(turn),
      wakeup: { source: 'wake', notices: [notice], turnIndex: turn },
    },
    {
      id: `a${turn}`, role: 'assistant', content: '', contentType: 'text', timestamp: at(turn), isStreaming: false,
      turnIndex: turn, completedAt: at(turn).getTime() + 2000,
      contentSegments: [{ type: 'tool_call', order: 0, toolCallId: `tc-${turn}` }],
      reasoningProcesses: {},
      toolCallProcesses: {
        [`tc-${turn}`]: {
          toolName: 'schedule_wakeup', toolCall: { name: 'schedule_wakeup', args },
          toolCallResult: {
            content: 'Ended without a reply.', content_type: 'text', tool_call_id: `tc-${turn}`,
            artifact: { quiet: true, changed: 'triggers' in args },
          },
          isInProgress: false, isComplete: true, isFailed: false, order: 0,
        },
      },
      ...over,
    },
  ];
}

const opening: Msg[] = [
  { id: 'u0', role: 'user', content: 'set the checks', contentType: 'text', timestamp: at(0), isStreaming: false, turnIndex: 0 },
  {
    id: 'a0', role: 'assistant', content: 'set', contentType: 'text', timestamp: at(0), isStreaming: false, turnIndex: 0,
    completedAt: at(0).getTime() + 2000, contentSegments: [{ type: 'text', content: 'set', order: 0 }],
    reasoningProcesses: {}, toolCallProcesses: {},
  },
];

const QUIET = { quiet: 'nothing new' };
const CHANGED = { triggers: [{ at: '2030-01-02T15:00' }], quiet: 'nothing new' };

function renderList(messages: Msg[], isLoading = false) {
  const view = (msgs: Msg[], loading: boolean) => (
    <MessageActionsProvider actions={{}}>
      <MessageList messages={msgs as MessageRecord[]} isLoading={loading} />
    </MessageActionsProvider>
  );
  const result = renderWithProviders(view(messages, isLoading));
  return { ...result, rerenderList: (msgs: Msg[], loading: boolean) => result.rerender(view(msgs, loading)) };
}

const folds = (container: HTMLElement) =>
  Array.from(container.querySelectorAll('[data-quiet-checks]')).map((el) => el.getAttribute('data-quiet-checks'));
const wakeRows = (container: HTMLElement) => container.querySelectorAll('[data-wakeup]').length;

describe('quiet checks fold', () => {
  it('folds back-to-back quiet turns into one row that opens to one line each', () => {
    const { container } = renderList([...opening, ...wakeTurn(1, QUIET), ...wakeTurn(2, QUIET), ...wakeTurn(3, QUIET)]);
    expect(folds(container)).toEqual(['3']);
    expect(wakeRows(container)).toBe(0);

    const toggle = container.querySelector('[data-quiet-checks] button') as HTMLElement;
    expect(toggle).toHaveAttribute('aria-expanded', 'false');
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute('aria-expanded', 'true');
    expect(container.querySelectorAll('[data-quiet-checks] li')).toHaveLength(3);
  });

  it('shows a turn that changed a wakeup, which splits the run around it', () => {
    const { container } = renderList([
      ...opening, ...wakeTurn(1, QUIET), ...wakeTurn(2, QUIET), ...wakeTurn(3, CHANGED), ...wakeTurn(4, QUIET),
    ]);
    expect(folds(container)).toEqual(['2', '1']);
    expect(wakeRows(container)).toBe(1);
  });

  // A row of another kind inside a quiet turn (a task's report, a divider)
  // would be hidden by folding the turn, or would leave it half folded.
  it('shows a quiet turn whole when another row sits among its replies', () => {
    const [opener, reply] = wakeTurn(2, QUIET);
    const quote = {
      toolName: 'get_quote', toolCall: { name: 'get_quote', args: {} },
      isInProgress: false, isComplete: true, isFailed: false, order: 0,
    };
    const { container } = renderList([
      ...opening, ...wakeTurn(1, QUIET),
      opener,
      { ...reply, id: 'a2-first', contentSegments: [{ type: 'tool_call', order: 0, toolCallId: 'tc-q' }], toolCallProcesses: { 'tc-q': quote } },
      { id: 'n2', role: 'notification', content: 'Context summarized', variant: 'info', timestamp: at(2) },
      { ...reply, id: 'a2-rest' },
    ]);
    expect(folds(container)).toEqual(['1']);
    expect(wakeRows(container)).toBe(1);
    expect(container.textContent).toContain('Context summarized');
  });

  it('folds a price alert the quiet turn took in, from before wakeups, into its entry', () => {
    const [opener, reply] = wakeTurn(2, QUIET);
    const alert = deliveredNoticeRow({
      content: '', market_alert: { target_id: 'tg-1', notice: 'hit', symbol: 'ACME', kind: 'above', value: 205 },
    } as never, at(2));
    const { container } = renderList([
      ...opening, ...wakeTurn(1, QUIET),
      opener, { ...reply, id: 'a2-first' }, alert as unknown as Msg, { ...reply, id: 'a2-rest' },
    ]);
    expect(folds(container)).toEqual(['2']);
    expect(container.querySelector('[data-market-alert]')).toBeNull();
    fireEvent.click(container.querySelector('[data-quiet-checks] button') as HTMLElement);
    expect(container.querySelectorAll('[data-quiet-checks] li')).toHaveLength(2);
  });

  it('shows a running quiet turn until it ends, then folds it into the row above', () => {
    const running = wakeTurn(3, QUIET, { isStreaming: true });
    const { container, rerenderList } = renderList([...opening, ...wakeTurn(1, QUIET), ...wakeTurn(2, QUIET), ...running], true);
    expect(folds(container)).toEqual(['2']);
    expect(wakeRows(container)).toBe(1);

    rerenderList([...opening, ...wakeTurn(1, QUIET), ...wakeTurn(2, QUIET), ...wakeTurn(3, QUIET)], false);
    expect(folds(container)).toEqual(['3']);
    expect(wakeRows(container)).toBe(0);
  });
});
