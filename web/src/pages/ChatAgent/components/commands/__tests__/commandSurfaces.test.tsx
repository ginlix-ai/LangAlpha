/**
 * A background command on the transcript's surfaces: its tool card follows
 * it after the hand-off, a call with no command reads as it always has, and
 * the turn its report starts is captioned by its description, with the way
 * back to that card.
 */
import React from 'react';
import { describe, expect, it, vi } from 'vitest';
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { queryKeys } from '@/lib/queryKeys';
import type { MessageProvenance } from '@/types/chat';
import type { CommandRun } from '@/types/commands';
import { MessageActionsProvider, type MessageActions } from '../../messageList/MessageActionsContext';
import { ReportRow } from '../../messageList/ReportRow';
import { ToolCallRow } from '../../messageList/ToolCallRow';
import MessageList from '../../MessageList';
import type { ToolActivityItem } from '../../messageList/activityTypes';
import type { MessageRecord } from '../../messageList/types';
import { ThreadCommandsContext } from '../threadCommandsContext';

vi.mock('../../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  stopCommand: vi.fn().mockResolvedValue({ stopped: true, command: null }),
}));

vi.mock('@/hooks/useUser', () => ({ useUser: () => ({ user: null }) }));
vi.mock('@/contexts/ThemeContext', () => ({ useTheme: () => ({ theme: 'dark' }) }));

import { stopCommand } from '../../../utils/api';

const THREAD = '7d1c2a4e-0b3f-4c55-9a61-2f8e5d9b0c17';
const M = 60_000;

function command(over: Partial<CommandRun> = {}): CommandRun {
  const now = Date.now();
  return {
    command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run 10-year backtest', status: 'running',
    exit_code: null, failure_reason: null, tool_call_id: 'call_1', log_path: '.agents/threads/7d1c2a4e/commands/bg-3f9a1c2e7d10.log',
    started_at: new Date(now - 17 * M - 5_000).toISOString(), handed_off_at: new Date(now - 12 * M).toISOString(), ended_at: null,
    ...over,
  } as CommandRun;
}

const listed = (...commands: CommandRun[]) => ({ thread_id: THREAD, commands });

/** `ui` in a chat whose host read the listing `commands`, or in a view that
 *  lists none (a share, a subagent's transcript) without. */
function withCommands(ui: React.ReactElement, commands?: CommandRun[], actions: MessageActions = {}) {
  const client = new QueryClient();
  if (commands) client.setQueryData(queryKeys.threads.commands(THREAD), listed(...commands));
  const view = render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <ThreadCommandsContext value={commands ? THREAD : null}>
          <MessageActionsProvider actions={actions}>{ui}</MessageActionsProvider>
        </ThreadCommandsContext>
      </MemoryRouter>
    </QueryClientProvider>,
  );
  return { ...view, client };
}

function bashCall(id = 'call_1', toolName = 'Bash'): ToolActivityItem {
  return {
    type: 'tool_call', id, toolCallId: id, toolName,
    toolCall: { args: { command: 'python backtest.py', description: 'Run 10-year backtest' } },
    toolCallResult: { content: 'Still running after 5m, so it moved to the background as command_id bg-3f9a1c2e7d10.' },
    isComplete: true, _liveState: 'completed',
  };
}

describe('the tool card of a call that moved to the background', () => {
  it('runs on in the background, with how long and a Stop', async () => {
    const { container } = withCommands(<ToolCallRow item={bashCall()} />, [command()]);
    expect(container.querySelector('[data-command-state="running"]')).toHaveTextContent('In background 17m');
    fireEvent.click(screen.getByRole('button', { name: 'Stop Run 10-year backtest' }));
    await waitFor(() => expect(stopCommand).toHaveBeenCalledWith(THREAD, 'bg-3f9a1c2e7d10'));
  });

  it('reads as stopping, with no second Stop, once the server takes a stop it has yet to finish', async () => {
    vi.mocked(stopCommand).mockResolvedValueOnce({ stopped: false, command: command({ stopping: true }) });
    const { container, client } = withCommands(<ToolCallRow item={bashCall()} />, [command()]);
    fireEvent.click(screen.getByRole('button', { name: 'Stop Run 10-year backtest' }));
    // Past the request itself, which reads as stopping while it is out.
    await waitFor(() => expect(
      client.getQueryData<ReturnType<typeof listed>>(queryKeys.threads.commands(THREAD))?.commands[0].stopping,
    ).toBe(true));
    await waitFor(() => expect(client.isMutating()).toBe(0));
    const button = screen.getByRole('button', { name: 'Stop Run 10-year backtest' });
    expect(button).toBeDisabled();
    expect(button).toHaveTextContent('Stopping…');
    expect(container.querySelector('[data-command-state="running"]')).not.toBeNull();
  });

  const ran41m = { started_at: new Date(Date.now() - 60 * M).toISOString(), ended_at: new Date(Date.now() - 19 * M).toISOString() };

  it.each<[Partial<CommandRun>, string]>([
    [{ status: 'completed', exit_code: 0, ...ran41m }, 'Completed in 41m'],
    [{ status: 'failed', exit_code: 1, failure_reason: 'exit', ...ran41m }, 'Failed after 41m'],
    [{ status: 'failed', failure_reason: 'after_turn_limit', ...ran41m }, 'Stopped early after 41m'],
    [{ status: 'stopped', ...ran41m }, 'Stopped after 41m'],
  ])('says how it ended and how long it took, with no Stop: %o', (over, words) => {
    const { container } = withCommands(<ToolCallRow item={bashCall()} />, [command(over)]);
    expect(container.querySelector('[data-command-state]')).toHaveTextContent(words);
    expect(screen.queryByRole('button', { name: /^Stop/ })).toBeNull();
  });

  it('follows its command as the host reads the listing again', async () => {
    const { container, client } = withCommands(<ToolCallRow item={bashCall()} />, [command()]);
    act(() => client.setQueryData(queryKeys.threads.commands(THREAD), listed(command({ status: 'stopped', ...ran41m }))));
    await waitFor(() => expect(container.querySelector('[data-command-state]')).toHaveTextContent('Stopped after 41m'));
  });

  it('reads as it always has for a call with no command, or outside a thread that lists them', () => {
    const other = withCommands(<ToolCallRow item={bashCall('call_2')} />, [command()]);
    expect(other.container.querySelector('[data-command-state]')).toBeNull();
    other.unmount();
    const bare = withCommands(<ToolCallRow item={bashCall()} />);
    expect(bare.container.querySelector('[data-command-state]')).toBeNull();
  });

  it('follows only the tools that hand off, so an old BashOutput call reads as it did and watches nothing', () => {
    const { container, client } = withCommands(<ToolCallRow item={bashCall('call_1', 'BashOutput')} />, [command()]);
    expect(container.querySelector('[data-command-state]')).toBeNull();
    expect(container).toHaveTextContent('BashOutput');
    expect(client.getQueryCache().find({ queryKey: queryKeys.threads.commands(THREAD) })?.getObserversCount()).toBe(0);
  });
});

type CommandReport = Extract<MessageProvenance, { kind: 'command_report' }>;
const report = (over: Partial<CommandReport> = {}): CommandReport => ({
  kind: 'command_report', tool: 'Bash', description: 'Run 10-year backtest',
  status: 'completed', failureReason: null, toolCallId: 'call_1', ...over,
});

function said(words: string) {
  expect(screen.getByTitle(words)).toHaveTextContent(words);
}

describe('ReportRow: a background command\'s', () => {
  it('says it finished, by its description, beside the check', () => {
    const { container } = withCommands(<ReportRow id="r" provenance={report()} />);
    said('Run 10-year backtest finished');
    expect(container.querySelector('.lucide-circle-check')).not.toBeNull();
  });

  it('says it failed beside the X', () => {
    const { container } = withCommands(<ReportRow id="r" provenance={report({ status: 'failed', failureReason: 'exit' })} />);
    said('Run 10-year backtest failed');
    expect(container.querySelector('.lucide-circle-x')).not.toBeNull();
  });

  it('says one stopped an hour after its turn stopped early, beside the X', () => {
    const { container } = withCommands(<ReportRow id="r" provenance={report({ status: 'failed', failureReason: 'after_turn_limit' })} />);
    said('Run 10-year backtest stopped early');
    expect(container.querySelector('.lucide-circle-x')).not.toBeNull();
  });

  it('says one the user stopped was stopped, beside the stop', () => {
    const { container } = withCommands(<ReportRow id="r" provenance={report({ status: 'stopped' })} />);
    said('Run 10-year backtest was stopped');
    expect(container.querySelector('.lucide-circle-stop')).not.toBeNull();
  });

  it("names one with no description by its tool, never its id", () => {
    withCommands(<ReportRow id="r" provenance={report({ description: null, tool: 'ExecuteCode' })} />);
    said('Execute Code finished');
    expect(screen.queryByText(/bg-/)).toBeNull();
  });

  it('opens the tool card it started from', () => {
    const onToolCallDetailClick = vi.fn();
    withCommands(<ReportRow id="r" provenance={report()} />, undefined, { onToolCallDetailClick });
    const open = screen.getByRole('button', { name: 'Open' });
    expect(open).toHaveAccessibleDescription('Run 10-year backtest finished');
    fireEvent.click(open);
    expect(onToolCallDetailClick).toHaveBeenCalledWith('call_1');
  });

  it('has no way in for a command whose call had no id, or in a read-only view', () => {
    const actions = { onToolCallDetailClick: vi.fn() };
    const lost = withCommands(<ReportRow id="r" provenance={report({ toolCallId: null })} />, [command({ tool_call_id: 'call_9' })], actions);
    expect(screen.queryByRole('button')).toBeNull();
    lost.unmount();
    withCommands(<ReportRow id="r" provenance={report()} readOnly />, undefined, actions);
    expect(screen.queryByRole('button')).toBeNull();
  });

  it('is drawn by the transcript as a line on the agent\'s side, with no bubble', () => {
    const messages: MessageRecord[] = [{
      id: 'history-user-2', role: 'user', content: '', contentType: 'text', timestamp: new Date('2030-01-01T12:00:00Z'),
      isStreaming: false, isHistory: true, turnIndex: 2, provenance: report(),
    }];
    const { container } = withCommands(<MessageList messages={messages} />, undefined, { onToolCallDetailClick: vi.fn(), onEditMessage: vi.fn() });
    const row = container.querySelector<HTMLElement>('[data-message-id="history-user-2"]');
    expect(row?.textContent).toBe('Run 10-year backtest finishedOpen');
    expect(row?.className).not.toContain('justify-end');
    expect(row?.querySelector('[title="Copy message"]')).toBeNull();
  });
});
