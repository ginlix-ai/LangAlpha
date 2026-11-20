/**
 * What decides that a turn can fold, and what keeps it from settling.
 *
 * Two ways a turn used to lose the fold silently. A transcript whose producer
 * writes ISO-string timestamps (the market panel) had no readable start, and a
 * turn with no start gets no row at all, so the whole panel quietly kept the
 * pre-fold layout. And a background task outlives the stream that launched it:
 * the turn settled the moment the main stream ended, collapsing the one row
 * that says the work is still going.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';
import MessageList from '../MessageList';
import { MessageActionsProvider } from '../messageList/MessageActionsContext';
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

function renderList(messages: Msg[], isLoading = false, actions: Record<string, unknown> = {}) {
  return renderWithProviders(
    <MessageActionsProvider actions={actions}>
      <MessageList messages={messages as MessageRecord[]} isLoading={isLoading} />
    </MessageActionsProvider>,
  );
}

const foldState = (container: HTMLElement) =>
  container.querySelector('[data-turn-fold]')?.getAttribute('data-turn-fold') ?? null;

describe('turn fold — reading a turn\'s start time', () => {
  it('folds a turn whose producer writes ISO-string timestamps', () => {
    // An ISO string on both roles, which the untyped record allows beside a Date.
    const startedAt = new Date('2026-09-21T15:04:00.000Z').toISOString();
    const repliedAt = new Date('2026-09-21T15:04:12.000Z').toISOString();
    const { container } = renderList([
      { id: 'u0', role: 'user', content: 'ask', contentType: 'text', timestamp: startedAt, isStreaming: false },
      {
        id: 'a0', role: 'assistant', content: 'answer', contentType: 'text',
        timestamp: repliedAt, isStreaming: false,
        completedAt: Date.parse('2026-09-21T15:04:12.000Z'),
        contentSegments: [
          { type: 'tool_call', order: 0, toolCallId: 'tc-0' },
          { type: 'text', content: 'answer', order: 1 },
        ],
        reasoningProcesses: {},
        toolCallProcesses: {
          'tc-0': {
            toolName: 'WebSearch', toolCall: { args: {} },
            isInProgress: false, isComplete: true, isFailed: false,
          },
        },
      },
    ]);

    expect(foldState(container)).toBe('collapsed');
    // Both ends of the turn were read, so the header carries a duration rather
    // than the bare `chat.worked` a turn with no end time falls back to.
    expect(container.querySelector('[data-turn-fold] button')?.textContent).toContain('chat.workedFor');
  });
});

describe('turn fold — a refused send sharing the turn\'s count', () => {
  it('heads the answer of the send the turn ran, below its question', () => {
    // The server refused the first send before admitting it, so the next one
    // was admitted under the same count: both pairs carry turn 43.
    const at = (s: number) => new Date(Date.UTC(2026, 8, 21, 15, 4, s));
    const { container } = renderList([
      { id: 'u-refused', role: 'user', content: 'first try', contentType: 'text', timestamp: at(0), isStreaming: false, turnIndex: 43 },
      {
        id: 'a-refused', role: 'assistant', content: 'refused', contentType: 'text', timestamp: at(1), isStreaming: false,
        turnIndex: 43, error: 'Server error 500', contentSegments: [{ type: 'text', content: 'refused', order: 0 }],
        reasoningProcesses: {}, toolCallProcesses: {},
      },
      { id: 'u-sent', role: 'user', content: 'go on', contentType: 'text', timestamp: at(30), isStreaming: false, turnIndex: 43 },
      {
        id: 'a-sent', role: 'assistant', content: 'answer', contentType: 'text', timestamp: at(32), isStreaming: false,
        turnIndex: 43, completedAt: at(42).getTime(),
        contentSegments: [
          { type: 'tool_call', order: 0, toolCallId: 'tc-0' },
          { type: 'text', content: 'answer', order: 1 },
        ],
        reasoningProcesses: {},
        toolCallProcesses: {
          'tc-0': {
            toolName: 'WebSearch', toolCall: { args: {} },
            isInProgress: false, isComplete: true, isFailed: false,
          },
        },
      },
    ]);

    const rows = container.querySelectorAll('[data-turn-fold]');
    expect(rows).toHaveLength(1);
    const question = Array.from(container.querySelectorAll('*')).find((el) => el.textContent === 'go on');
    expect(question).toBeTruthy();
    expect(question!.compareDocumentPosition(rows[0]) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  });
});

describe('turn fold — a stamp on a bubble the list never paints', () => {
  it('times a steered turn whose continuation settled empty', () => {
    // The agent resumed after steering and produced nothing, so the
    // continuation is both a steering continuation and an orphan:
    // `isOrphanAssistantMessage` keeps it off screen, while finalization and
    // both replay paths stamped the turn's end on it and nowhere else.
    const { container } = renderList([
      { id: 'u0', role: 'user', content: 'ask', contentType: 'text', timestamp: new Date('2026-09-21T15:04:00.000Z'), isStreaming: false },
      {
        id: 'a0', role: 'assistant', content: 'answer', contentType: 'text',
        timestamp: new Date('2026-09-21T15:04:02.000Z'), isStreaming: false,
        contentSegments: [
          { type: 'tool_call', order: 0, toolCallId: 'tc-0' },
          { type: 'text', content: 'answer', order: 1 },
        ],
        reasoningProcesses: {},
        toolCallProcesses: {
          'tc-0': { toolName: 'WebSearch', toolCall: { args: {} }, isInProgress: false, isComplete: true, isFailed: false },
        },
      },
      {
        id: 'a1', role: 'assistant', content: '', contentType: 'text', isSteering: true,
        timestamp: new Date('2026-09-21T15:04:20.000Z'), isStreaming: false,
        contentSegments: [], reasoningProcesses: {}, toolCallProcesses: {},
        completedAt: Date.parse('2026-09-21T15:04:20.000Z'),
      },
    ]);

    expect(foldState(container)).toBe('collapsed');
    expect(container.querySelector('[data-turn-fold] button')?.textContent).toContain('chat.workedFor');
  });
});

describe('turn fold — a running turn whose bubble is hidden between model calls', () => {
  it('leaves the turn before it settled', () => {
    // Between two model calls the bubble is not streaming, and one whose only
    // call so far is TodoWrite draws nothing, so the list hides it. The
    // session still loading belongs to that turn, not to the last one painted.
    const { container } = renderList([
      { id: 'u0', role: 'user', content: 'ask', contentType: 'text', timestamp: new Date(), isStreaming: false },
      {
        id: 'a0', role: 'assistant', content: 'answer', contentType: 'text',
        timestamp: new Date(), isStreaming: false, completedAt: Date.now(),
        contentSegments: [
          { type: 'tool_call', order: 0, toolCallId: 'tc-0' },
          { type: 'text', content: 'answer', order: 1 },
        ],
        reasoningProcesses: {},
        toolCallProcesses: {
          'tc-0': { toolName: 'WebSearch', toolCall: { args: {} }, isInProgress: false, isComplete: true, isFailed: false },
        },
      },
      { id: 'u1', role: 'user', content: 'track the steps', contentType: 'text', timestamp: new Date(), isStreaming: false },
      {
        id: 'a1', role: 'assistant', content: '', contentType: 'text',
        timestamp: new Date(), isStreaming: false,
        contentSegments: [{ type: 'tool_call', order: 0, toolCallId: 'tc-todo' }],
        reasoningProcesses: {},
        toolCallProcesses: {
          'tc-todo': {
            toolName: 'TodoWrite',
            toolCall: { args: { todos: [{ content: 'Pull the 10-K', status: 'pending' }] } },
            toolCallResult: { content: 'Updated todo list' },
            isInProgress: false, isComplete: true, isFailed: false,
          },
        },
      },
    ], true);

    expect(container.querySelector('[data-message-id="a1"]')).toBeNull();
    expect(foldState(container)).toBe('collapsed');
  });
});

describe('turn fold — a background task outliving its stream', () => {
  const taskTurn = (inProgress: boolean): Msg[] => [
    { id: 'u0', role: 'user', content: 'ask', contentType: 'text', timestamp: new Date(), isStreaming: false },
    {
      id: 'a0', role: 'assistant', content: 'answer', contentType: 'text',
      timestamp: new Date(), isStreaming: false,
      contentSegments: [
        { type: 'tool_call', order: 0, toolCallId: 'tc-task' },
        { type: 'text', content: 'answer', order: 1 },
      ],
      reasoningProcesses: {},
      toolCallProcesses: {
        // TaskOutput is always-live: buildRenderBlocks pins it to the live zone
        // for its whole in-progress life, stream or no stream.
        'tc-task': {
          toolName: 'TaskOutput', toolCall: { args: {} },
          isInProgress: inProgress, isComplete: !inProgress, isFailed: false,
          _createdAt: Date.now(),
        },
      },
    },
  ];

  it('keeps the turn live while the task runs, and settles it when the task lands', () => {
    // isLoading false and nothing streaming: by wall-clock the turn is over.
    const { container } = renderList(taskTurn(true));
    expect(foldState(container)).toBe('live');
    expect(container.querySelector('[data-activity-state="live"]')).not.toBeNull();

    // The task lands: the turn settles and takes its process rows with it.
    const settled = renderList(taskTurn(false));
    expect(foldState(settled.container)).toBe('collapsed');
    expect(settled.container.querySelector('[data-activity-state]')).toBeNull();
  });
});

describe('turn fold — the bubble reads the turn\'s liveness, not its own', () => {
  const withDeliverable = (taskRunning: boolean): Msg[] => [
    { id: 'u0', role: 'user', content: 'ask', contentType: 'text', timestamp: new Date(), isStreaming: false },
    {
      id: 'a0', role: 'assistant', content: 'answer', contentType: 'text',
      timestamp: new Date(), isStreaming: false,
      contentSegments: [
        { type: 'tool_call', order: 0, toolCallId: 'tc-write' },
        { type: 'tool_call', order: 1, toolCallId: 'tc-task' },
        { type: 'text', content: 'answer', order: 2 },
      ],
      reasoningProcesses: {},
      toolCallProcesses: {
        'tc-write': {
          toolName: 'Write', order: 0, isFailed: false, isComplete: true, isInProgress: false,
          toolCallResult: { content: 'ok' },
          toolCall: { name: 'Write', args: { file_path: 'results/report.md', content: 'x' } },
        },
        'tc-task': {
          toolName: 'TaskOutput', toolCall: { args: {} },
          isInProgress: taskRunning, isComplete: !taskRunning, isFailed: false,
          _createdAt: Date.now(),
        },
      },
    },
  ];

  it('holds the deliverables deck back while a background task is still running', () => {
    // Publishing the deck over results a running task can still rewrite is the
    // bug here.
    const running = renderList(withDeliverable(true), false, { onOpenFile: vi.fn() });
    expect(foldState(running.container)).toBe('live');
    expect(running.container.querySelector('.turn-file-card')).toBeNull();

    const landed = renderList(withDeliverable(false), false, { onOpenFile: vi.fn() });
    expect(foldState(landed.container)).toBe('collapsed');
    expect(landed.container.querySelector('.turn-file-card')).not.toBeNull();
  });

  /** The row is always mounted to reserve its height; `inert` is what hides it. */
  const actionsHidden = (c: HTMLElement) => {
    const copy = c.querySelector('button[title="chat.actions.copyMessage"]');
    expect(copy).not.toBeNull();
    return copy!.closest('[aria-hidden]')?.getAttribute('aria-hidden') === 'true';
  };

  it('keeps rating and regenerate out of reach until the task lands', () => {
    // Rating an answer a running task can still change asks about a reply that
    // does not exist yet, and regenerate would race the task.
    const running = renderList(withDeliverable(true), false, { onRegenerate: vi.fn(), onThumbUp: vi.fn() });
    expect(actionsHidden(running.container)).toBe(true);

    const landed = renderList(withDeliverable(false), false, { onRegenerate: vi.fn(), onThumbUp: vi.fn() });
    expect(actionsHidden(landed.container)).toBe(false);
  });
});

describe('turn fold — a steer splitting the turn', () => {
  it('keeps the reply the steer answered, and folds the working on both sides', () => {
    // The agent takes a steer in only at its next model call, so the bubble
    // before it closes with what the reader was answered with when they spoke
    // again. Folded as the turn's working, that answer left the collapsed turn
    // while the steer that replied to it stayed.
    const done = { isInProgress: false, isComplete: true, isFailed: false };
    const { container, getByText, queryByText } = renderList([
      { id: 'u0', role: 'user', content: 'ask', contentType: 'text', timestamp: new Date('2026-11-20T15:40:00.000Z'), isStreaming: false, turnIndex: 0 },
      {
        id: 'a0', role: 'assistant', content: '', contentType: 'text', turnIndex: 0,
        timestamp: new Date('2026-11-20T15:40:02.000Z'), isStreaming: false,
        contentSegments: [
          { type: 'text', content: 'checking the tape', order: 0 },
          { type: 'tool_call', order: 1, toolCallId: 'tc-0' },
          { type: 'text', content: 'first answer', order: 2 },
        ],
        reasoningProcesses: {},
        toolCallProcesses: { 'tc-0': { toolName: 'WebSearch', toolCall: { args: {} }, ...done } },
      },
      {
        id: 'u1', role: 'user', content: 'steer', contentType: 'text', turnIndex: 0, steeringDelivered: true,
        timestamp: new Date('2026-11-20T15:41:00.000Z'), isStreaming: false,
      },
      {
        id: 'a1', role: 'assistant', content: '', contentType: 'text', isSteering: true, turnIndex: 0,
        timestamp: new Date('2026-11-20T15:41:02.000Z'), isStreaming: false,
        completedAt: Date.parse('2026-11-20T15:41:30.000Z'),
        contentSegments: [
          { type: 'text', content: 'pulling the chain', order: 0 },
          { type: 'tool_call', order: 1, toolCallId: 'tc-1' },
          { type: 'text', content: 'second answer', order: 2 },
        ],
        reasoningProcesses: {},
        toolCallProcesses: { 'tc-1': { toolName: 'WebSearch', toolCall: { args: {} }, ...done } },
      },
    ]);

    expect(foldState(container)).toBe('collapsed');
    expect(getByText('first answer')).toBeInTheDocument();
    expect(getByText('second answer')).toBeInTheDocument();
    expect(queryByText('checking the tape')).toBeNull();
    expect(queryByText('pulling the chain')).toBeNull();
  });
});

describe('turn fold — which of a turn\'s texts read as its answer', () => {
  const done = { isInProgress: false, isComplete: true, isFailed: false };
  const call = (toolName: string) => ({ toolName, toolCall: { args: {} }, ...done });
  const ANSWER = [
    '## Summary', '', '| Ticker | Move |', '|---|---|', '| AAA | +3% |', '| BBB | -2% |', '',
    '## Takeaway', '', 'Both moved on the same print.',
  ].join('\n');
  const turn = (segments: Record<string, unknown>[], extra: Record<string, unknown> = {}) => [
    { id: 'u0', role: 'user', content: 'ask', contentType: 'text', timestamp: new Date('2026-11-20T15:40:00.000Z'), isStreaming: false },
    {
      id: 'a0', role: 'assistant', content: '', contentType: 'text',
      timestamp: new Date('2026-11-20T15:40:02.000Z'), isStreaming: false,
      completedAt: Date.parse('2026-11-20T15:41:00.000Z'),
      contentSegments: segments, reasoningProcesses: {},
      toolCallProcesses: { 'tc-0': call('WebSearch'), 'tc-1': call('TodoWrite'), 'tc-2': call('ExecuteCode') },
      ...extra,
    },
  ];

  it('keeps an answer the agent wrote before one more call, beside its closing line', () => {
    const { container, getByText, queryByText } = renderList(turn([
      { type: 'text', content: 'Checking the tape.', order: 0 },
      { type: 'tool_call', order: 1, toolCallId: 'tc-0' },
      { type: 'text', content: ANSWER, order: 2 },
      { type: 'tool_call', order: 3, toolCallId: 'tc-1' },
      { type: 'text', content: 'Saved it to the notes.', order: 4 },
    ]));

    expect(getByText(/Both moved on the same print/)).toBeInTheDocument();
    expect(getByText('Saved it to the notes.')).toBeInTheDocument();
    expect(queryByText('Checking the tape.')).toBeNull();
    // The turn-end landing reads the answer from its top, not the line under it.
    expect(container.querySelector('[data-reply-start]')).toHaveTextContent('Both moved on the same print');
  });

  it('reads a turn that stopped mid-work as one with no answer, not as answered by its last line', () => {
    // With no answer, the turn lays out its work rather than standing a line
    // of narration alone under the fold row as if it were the reply.
    const { container } = renderList(turn([
      { type: 'text', content: 'Checking the tape.', order: 0 },
      { type: 'tool_call', order: 1, toolCallId: 'tc-0' },
      { type: 'text', content: 'Now let me compute the ratios:', order: 2 },
      { type: 'tool_call', order: 3, toolCallId: 'tc-2' },
    ]));

    expect(foldState(container)).toBe('process');
  });

  it('keeps a short closing answer the agent ticked its todo list after', () => {
    const { container, getByText } = renderList(turn([
      { type: 'text', content: 'Checking the tape.', order: 0 },
      { type: 'tool_call', order: 1, toolCallId: 'tc-0' },
      { type: 'text', content: 'The first is cheaper on earnings.', order: 2 },
      { type: 'tool_call', order: 3, toolCallId: 'tc-1' },
      // The list the call draws, as the stream and the replay both write it.
      { type: 'todo_list', order: 4, todoListId: 'tl-1' },
    ], { todoListProcesses: { 'tl-1': { todos: [{ content: 'Compare', status: 'completed' }], total: 1, completed: 1, in_progress: 0, pending: 0, order: 4 } } }));

    expect(foldState(container)).toBe('collapsed');
    expect(getByText('The first is cheaper on earnings.')).toBeInTheDocument();
  });

  it('keeps the text leading into an approval the user is asked for', () => {
    const { getByText } = renderList(turn([
      { type: 'text', content: 'Checking the tape.', order: 0 },
      { type: 'tool_call', order: 1, toolCallId: 'tc-0' },
      { type: 'text', content: 'Placing a limit order for ten shares.', order: 2 },
      { type: 'tool_approval', order: 3, proposalId: 'p-1' },
      { type: 'text', content: 'Waiting on your approval.', order: 4 },
    ]));

    expect(getByText('Placing a limit order for ten shares.')).toBeInTheDocument();
  });

  it('keeps the text leading into a question the user is asked', () => {
    const { getByText } = renderList(turn([
      { type: 'text', content: 'Your profile is empty, so I prepared three starting points.', order: 0 },
      { type: 'user_question', order: 1, questionId: 'q-1' },
      { type: 'text', content: 'Pick one and I will start.', order: 2 },
    ], { userQuestions: { 'q-1': { question: 'Which one?', options: [], interruptId: 'q-1', status: 'answered' } } }));

    expect(getByText('Your profile is empty, so I prepared three starting points.')).toBeInTheDocument();
  });
});
