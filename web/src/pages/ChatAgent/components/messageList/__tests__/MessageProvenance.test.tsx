/**
 * The caption over a message nobody typed here: what it says, where it links,
 * and that a read-only view draws it without the link. A report-back's line
 * of its own: what it names, how long the run took, which side it takes. The
 * mark on a message a turn already running took in, alone and beside a sender.
 */
import React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { MemoryRouter, Route, Routes, useLocation } from 'react-router';
import { MessageProvenanceLabel } from '../MessageProvenance';
import { MessageActionsProvider, type MessageActions } from '../MessageActionsContext';
import { ReportRow } from '../ReportRow';
import { UserBubble } from '../UserBubble';
import MessageList from '../../MessageList';
import type { MessageProvenance } from '@/types/chat';
import { deliveredEntryRow } from '../../../session/stream/steeringDelivery';
import type { MessageRecord } from '../types';

vi.mock('@/hooks/useUser', () => ({
  useUser: () => ({ user: null }),
}));

vi.mock('@/contexts/ThemeContext', () => ({
  useTheme: () => ({ theme: 'dark' }),
}));

function Where() {
  const location = useLocation();
  return <output data-testid="where">{location.pathname + location.search}</output>;
}

function renderLabel(
  provenance: MessageProvenance,
  { actions = {}, readOnly = false }: { actions?: MessageActions; readOnly?: boolean } = {},
) {
  return render(
    <MemoryRouter initialEntries={['/chat/t/here']}>
      <MessageActionsProvider actions={actions}>
        <Routes>
          <Route path="*" element={<><MessageProvenanceLabel provenance={provenance} readOnly={readOnly} /><Where /></>} />
        </Routes>
      </MessageActionsProvider>
    </MemoryRouter>,
  );
}

describe('MessageProvenanceLabel', () => {
  it('names a channel with no link', () => {
    renderLabel({ kind: 'channel', platform: 'imessage' });
    expect(screen.getByText('Via iMessage')).toBeInTheDocument();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('opens the automation a run came from', () => {
    renderLabel({ kind: 'automation', automationId: 'auto-1', name: 'Morning brief' });
    fireEvent.click(screen.getByRole('button', { name: 'Automation · Morning brief' }));
    expect(screen.getByTestId('where')).toHaveTextContent('/automations?id=auto-1');
  });

  it('draws the label without its link in a read-only view', () => {
    renderLabel({ kind: 'automation', automationId: 'auto-1', name: null }, { readOnly: true });
    expect(screen.getByText('Automation')).toBeInTheDocument();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('goes back to the dispatching thread through the host', () => {
    const onOpenDispatcher = vi.fn();
    renderLabel({ kind: 'agent', agent: 'chief_of_staff', threadId: 'th-home' }, { actions: { onOpenDispatcher } });
    fireEvent.click(screen.getByRole('button', { name: 'From Chief of Staff' }));
    expect(onOpenDispatcher).toHaveBeenCalledWith('th-home');
  });

  it('labels a steer the Chief of Staff sent into a running turn, without a link to its Home thread', () => {
    const row = deliveredEntryRow(
      { content: 'add the margin bridge', origin: { type: 'agent', agent: 'chief_of_staff' }, request_key: 'rk-cos' },
      () => 'row',
      2,
    );
    renderLabel(row!.provenance!, { actions: { onOpenDispatcher: vi.fn() } });
    expect(screen.getByText('From Chief of Staff')).toBeInTheDocument();
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('reads an unrecorded dispatcher as the Chief of Staff', () => {
    renderLabel({ kind: 'agent', agent: null, threadId: 'th-old' });
    expect(screen.getByText('From Chief of Staff')).toBeInTheDocument();
  });

  it('leaves an unrecorded dispatcher unlabelled in a share', () => {
    renderLabel({ kind: 'agent', agent: null, threadId: 'th-old' }, { readOnly: true });
    expect(screen.queryByText('From Chief of Staff')).not.toBeInTheDocument();
  });

  it('leaves a report to the line the list draws for it', () => {
    renderLabel({ kind: 'analyst_report', threadId: 'th-ws', workspaceId: 'ws-1', workspaceName: 'Semis' });
    expect(screen.queryByText(/Semis/)).toBeNull();
    renderLabel(
      { kind: 'task_report', taskId: 'k7Xm2p', displayId: 'Task-k7Xm2p', description: 'Pull filings', failed: false },
      { actions: { onOpenSubagentTask: vi.fn() } },
    );
    expect(screen.queryByText(/Pull filings|Task-k7Xm2p/)).toBeNull();
  });
});

type AnalystReport = Extract<MessageProvenance, { kind: 'analyst_report' }>;

function renderReportRow(over: Partial<AnalystReport> = {}, readOnly = false) {
  const provenance: AnalystReport = { kind: 'analyst_report', threadId: 'th-ws', workspaceId: 'ws-1', workspaceName: 'Semis', ...over };
  return render(
    <MemoryRouter initialEntries={['/chat/t/here']}>
      <Routes>
        <Route path="*" element={<><ReportRow id="msg-report" provenance={provenance} readOnly={readOnly} /><Where /></>} />
      </Routes>
    </MemoryRouter>,
  );
}

/** What the line says. The name is set apart from the outcome, so the words
 *  are read off the label whole, as its title carries them. */
function said(words: string) {
  expect(screen.getByTitle(words)).toHaveTextContent(words);
}

describe('ReportRow: an analyst\'s', () => {
  it.each<[AnalystReport['outcome'], string]>([
    ['completed', 'Semis analyst reported back'],
    ['error', "Semis analyst couldn't finish"],
    ['cancelled', 'Semis analyst was stopped'],
  ])('reads a %s run in words, leaving how long it took to its hand-off\'s row', (outcome, words) => {
    renderReportRow({ outcome });
    said(words);
  });

  it('reads a report from before outcomes as reported back', () => {
    renderReportRow({});
    said('Semis analyst reported back');
  });

  it('names no workspace it does not know', () => {
    renderReportRow({ workspaceName: null, outcome: 'completed' });
    said('The analyst reported back');
  });

  it('leads with the inbox a report arrives in, and keeps the alert for a run that did not finish', () => {
    const { container } = renderReportRow({ outcome: 'completed' });
    expect(container.querySelector('.lucide-inbox')).not.toBeNull();
    expect(container.querySelector('.lucide-circle-alert')).toBeNull();
  });

  it('opens the Analyst\'s thread, and takes the focus the report link brings', () => {
    const { container } = renderReportRow({ outcome: 'completed' });
    // By its message: a thread handed work twice reports twice.
    const row = container.querySelector<HTMLElement>('[data-message-id="msg-report"]')!;
    row.focus();
    expect(row).toHaveFocus();
    const open = screen.getByRole('button', { name: 'Open' });
    expect(open).toHaveAccessibleDescription('Semis analyst reported back');
    fireEvent.click(open);
    expect(screen.getByTestId('where')).toHaveTextContent('/chat/t/th-ws');
  });

  it('draws no link in a read-only view', () => {
    renderReportRow({ outcome: 'completed' }, true);
    expect(screen.queryByRole('button')).toBeNull();
  });
});

type TaskReport = Extract<MessageProvenance, { kind: 'task_report' }>;

function renderTaskRow(over: Partial<TaskReport> = {}, { actions = {}, readOnly = false }: { actions?: MessageActions; readOnly?: boolean } = {}) {
  const provenance: TaskReport = {
    kind: 'task_report', taskId: 'k7Xm2p', displayId: 'Task-k7Xm2p', description: 'Pull filings', failed: false, ...over,
  };
  return render(
    <MessageActionsProvider actions={actions}>
      <ReportRow id="msg-task" provenance={provenance} readOnly={readOnly} />
    </MessageActionsProvider>,
  );
}

describe('ReportRow: a subagent\'s', () => {
  it('names the task by the title its row shows', () => {
    renderTaskRow({ description: 'Pull the 10-Q filings\nthen the segment tables' });
    said('Pull the 10-Q filings finished');
  });

  it('reads a failure in words beside the alert', () => {
    const { container } = renderTaskRow({ failed: true });
    said('Pull filings failed');
    expect(container.querySelector('.lucide-circle-alert')).not.toBeNull();
  });

  it('names a task with no title by its id', () => {
    renderTaskRow({ description: null });
    said('Task-k7Xm2p finished');
  });

  it('wears its role, as its row\'s avatar does', () => {
    const { container } = renderTaskRow({ subagentType: 'research' });
    expect(container.querySelector('.lucide-telescope')).not.toBeNull();
  });

  it('opens the subagent', () => {
    const onOpenSubagentTask = vi.fn();
    renderTaskRow({}, { actions: { onOpenSubagentTask } });
    const open = screen.getByRole('button', { name: 'Open' });
    expect(open).toHaveAccessibleDescription('Pull filings finished');
    fireEvent.click(open);
    expect(onOpenSubagentTask).toHaveBeenCalledWith({ subagentId: 'task:k7Xm2p', description: 'Pull filings' });
  });

  it('draws no link in a read-only view', () => {
    renderTaskRow({}, { actions: { onOpenSubagentTask: vi.fn() }, readOnly: true });
    expect(screen.queryByRole('button')).toBeNull();
  });
});

describe('MessageList report row', () => {
  it('draws a subagent\'s report as a line on the agent\'s side, with no bubble or actions', () => {
    const messages: MessageRecord[] = [
      {
        id: 'history-user-1',
        role: 'user',
        content: 'Pull the filings',
        contentType: 'text',
        timestamp: new Date('2026-10-10T11:00:00Z'),
        isStreaming: false,
        isHistory: true,
        turnIndex: 1,
      },
      {
        id: 'history-user-2',
        role: 'user',
        content: '',
        contentType: 'text',
        timestamp: new Date('2026-10-10T12:00:00Z'),
        isStreaming: false,
        isHistory: true,
        turnIndex: 2,
        provenance: { kind: 'task_report', taskId: 'k7Xm2p', displayId: 'Task-k7Xm2p', description: null, failed: true },
      },
    ];
    const { container } = render(
      <MemoryRouter>
        <MessageActionsProvider actions={{ onEditMessage: vi.fn(), onOpenSubagentTask: vi.fn() }}>
          <MessageList messages={messages} />
        </MessageActionsProvider>
      </MemoryRouter>,
    );
    expect(screen.getByRole('button', { name: 'Open' })).toHaveAccessibleDescription('Task-k7Xm2p failed');
    // The typed message above keeps its actions; the report row has none.
    expect(screen.getAllByTitle('Copy message')).toHaveLength(1);
    const report = container.querySelector<HTMLElement>('[data-message-id="history-user-2"]');
    expect(report?.querySelector('[title="Copy message"]')).toBeNull();
    expect(report?.textContent).toBe('Task-k7Xm2p failedOpen');
    expect(report?.className).not.toContain('justify-end');
  });

  it('draws an Analyst\'s report on the side outside senders take, with its way into the thread', () => {
    const messages: MessageRecord[] = [
      {
        id: 'history-user-3',
        role: 'user',
        content: '',
        contentType: 'text',
        timestamp: new Date('2026-10-10T12:00:00Z'),
        isStreaming: false,
        isHistory: true,
        turnIndex: 1,
        provenance: { kind: 'analyst_report', threadId: 'th-ws', workspaceId: 'ws-1', workspaceName: 'Semis', outcome: 'completed' },
      },
    ];
    const { container } = render(
      <MemoryRouter>
        <MessageActionsProvider actions={{ onEditMessage: vi.fn() }}>
          <MessageList messages={messages} />
        </MessageActionsProvider>
      </MemoryRouter>,
    );
    const report = container.querySelector<HTMLElement>('[data-message-id="history-user-3"]');
    expect(report?.textContent).toBe('Semis analyst reported backOpen');
    expect(report?.className).toContain('justify-end');
    // The hand-off it answers lands on it by its message, and focuses it.
    expect(report?.tabIndex).toBe(-1);
    expect(report?.querySelector('[title="Copy message"]')).toBeNull();
    expect(screen.getByRole('button', { name: 'Open' })).toBeInTheDocument();
  });

  it('stands reports that came in together as one block, whichever side each takes', () => {
    const at = new Date('2026-10-10T12:00:00Z');
    const row = (id: string, provenance: MessageProvenance): MessageRecord => ({
      id, role: 'user', content: '', contentType: 'text', timestamp: at, isStreaming: false, isHistory: true, turnIndex: 1, provenance,
    });
    const { container } = render(
      <MemoryRouter>
        <MessageList
          messages={[
            row('r-1', { kind: 'analyst_report', threadId: 'th-ws', workspaceId: 'ws-1', workspaceName: 'Semis', outcome: 'completed' }),
            row('r-2', { kind: 'task_report', taskId: 'k7Xm2p', displayId: 'Task-k7Xm2p', description: 'Pull filings', failed: false }),
          ]}
        />
      </MemoryRouter>,
    );
    const first = container.querySelector<HTMLElement>('[data-message-id="r-1"]');
    const second = container.querySelector<HTMLElement>('[data-message-id="r-2"]');
    expect(first?.style.marginTop).toBe('');
    expect(second?.style.marginTop).toBe('0px');
  });
});

const HINT = 'Taken in by a run already going';

describe('UserBubble: the steered mark', () => {
  const CHANNEL: MessageProvenance = { kind: 'channel', platform: 'imessage' };

  function renderBubble(props: { steered?: boolean; provenance?: MessageProvenance; className?: string }) {
    return render(
      <MemoryRouter>
        <MessageActionsProvider actions={{}}>
          <UserBubble {...props}>Skip 2019</UserBubble>
        </MessageActionsProvider>
      </MemoryRouter>,
    );
  }
  const follows = (a: HTMLElement, b: HTMLElement) => !!(a.compareDocumentPosition(b) & Node.DOCUMENT_POSITION_FOLLOWING);

  it('captions a steered message with the steered word, and says what it means on hover', () => {
    renderBubble({ steered: true });
    const mark = screen.getByTitle(HINT);
    expect(mark).toHaveTextContent('Steered');
    // Over the bubble, as a sender's caption is.
    expect(follows(mark, screen.getByText('Skip 2019'))).toBe(true);
  });

  it('draws the mark with the sender\'s caption, the sender keeping the bubble\'s edge', () => {
    renderBubble({ steered: true, provenance: CHANNEL });
    const mark = screen.getByTitle(HINT);
    expect(mark).toHaveTextContent('Steered');
    const sender = screen.getByText('Via iMessage');
    expect(follows(mark, sender)).toBe(true);
    expect(follows(sender, screen.getByText('Skip 2019'))).toBe(true);
  });

  it('captions a message that opened its turn with its sender alone', () => {
    renderBubble({ provenance: CHANNEL });
    expect(screen.getByText('Via iMessage')).toBeInTheDocument();
    expect(screen.queryByTitle(HINT)).toBeNull();
  });

  it('draws the bubble bare when nothing sent it and no turn took it in', () => {
    const { container } = renderBubble({});
    expect(screen.queryByTitle(HINT)).toBeNull();
    expect(screen.queryByText('Steered')).toBeNull();
    expect(container.firstElementChild).toHaveTextContent(/^Skip 2019$/);
  });

  it('puts the caller\'s class on the captioned column, not the bubble under it', () => {
    const { container } = renderBubble({ steered: true, className: 'ml-8' });
    const column = container.firstElementChild as HTMLElement;
    expect(column).toHaveClass('ml-8');
    expect(screen.getByText('Skip 2019')).not.toHaveClass('ml-8');
  });
});

describe('MessageList: a steered message', () => {
  const user = (id: string, over: Partial<MessageRecord> = {}): MessageRecord => ({
    id, role: 'user', content: `said ${id}`, contentType: 'text', timestamp: new Date('2026-10-10T12:00:00Z'),
    isStreaming: false, isHistory: true, turnIndex: 0, ...over,
  });

  it('marks a message the running turn took in, and not the one that opened it', () => {
    const { container } = render(
      <MemoryRouter>
        <MessageActionsProvider actions={{ onEditMessage: vi.fn() }}>
          <MessageList messages={[user('opener'), user('steer', { steeringDelivered: true })]} />
        </MessageActionsProvider>
      </MemoryRouter>,
    );
    const marked = (id: string) => container.querySelector(`[data-message-id="${id}"] [title="${HINT}"]`);
    expect(marked('steer')).toHaveTextContent('Steered');
    expect(marked('opener')).toBeNull();
  });
});
