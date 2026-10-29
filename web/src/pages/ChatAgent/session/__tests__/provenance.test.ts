// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { reportRowProvenance, senderProvenance, withLiveReportRow } from '../provenance';
import { handleHistorySteeringDelivered, handleHistoryUserMessage } from '../history/historyHandlers';
import { buildEntries } from '../../components/minimapEntries';
import { createAssistantMessage } from '../../hooks/utils/messageHelpers';
import type { ReportSource } from '@/types/api';
import type { ChatMessage, UserMessage } from '@/types/chat';
import type { MessageRecord } from '../../hooks/utils/types';

describe('senderProvenance', () => {
  it('labels a hand-off by the agent that dispatched it', () => {
    expect(senderProvenance({ origin: { type: 'agent', id: 'th-home', agent: 'chief_of_staff' } }))
      .toEqual({ kind: 'agent', agent: 'chief_of_staff', threadId: 'th-home' });
  });

  it('leaves the dispatcher open on a hand-off recorded before it was named', () => {
    expect(senderProvenance({ origin: { type: 'agent', id: 'th-old' } }))
      .toEqual({ kind: 'agent', agent: null, threadId: 'th-old' });
  });

  it('names an automation, or only says it was one', () => {
    expect(senderProvenance({ origin: { type: 'automation', id: 'auto-1', name: 'Morning brief' } }))
      .toEqual({ kind: 'automation', automationId: 'auto-1', name: 'Morning brief' });
    expect(senderProvenance({ origin: { type: 'automation', id: 'auto-1' } }))
      .toEqual({ kind: 'automation', automationId: 'auto-1', name: null });
  });

  it('labels a channel message by its platform alone', () => {
    for (const platform of ['telegram', 'slack', 'discord', 'feishu', 'imessage']) {
      expect(senderProvenance({ platform })).toEqual({ kind: 'channel', platform });
    }
  });

  it('gives this app\'s own surfaces and unlisted apps no label', () => {
    expect(senderProvenance({ platform: 'web' })).toBeNull();
    expect(senderProvenance({ platform: 'market_view:AAPL' })).toBeNull();
    expect(senderProvenance({ platform: 'carrier_pigeon' })).toBeNull();
    // The request's pattern admits these, and an object's own keys must not count as apps.
    expect(senderProvenance({ platform: 'constructor' })).toBeNull();
    expect(senderProvenance({ platform: '__proto__' })).toBeNull();
    expect(senderProvenance({})).toBeNull();
    expect(senderProvenance(undefined)).toBeNull();
  });

  it('names who sent a turn before where it arrived', () => {
    expect(senderProvenance({ origin: { type: 'automation', id: 'auto-1' }, platform: 'telegram' }))
      .toMatchObject({ kind: 'automation' });
  });

  it('gives a system origin no label', () => {
    expect(senderProvenance({ origin: { type: 'system' } })).toBeNull();
  });
});

describe('reportRowProvenance', () => {
  it('names the analyst reporting back, and its workspace when known', () => {
    expect(reportRowProvenance({ kind: 'analyst', thread_id: 'th-ws', workspace_id: 'ws-1', workspace_name: 'Semis' }))
      .toEqual({ kind: 'analyst_report', threadId: 'th-ws', workspaceId: 'ws-1', workspaceName: 'Semis' });
    expect(reportRowProvenance({ kind: 'analyst', thread_id: 'th-ws' }))
      .toEqual({ kind: 'analyst_report', threadId: 'th-ws', workspaceId: null, workspaceName: null });
  });

  it('carries how the analyst\'s run ended, and which runs it answers', () => {
    expect(reportRowProvenance({
      kind: 'analyst', thread_id: 'th-ws', workspace_id: 'ws-1', workspace_name: 'Semis',
      outcome: 'cancelled', run_ids: ['run-1', 'run-1b'],
    })).toEqual({
      kind: 'analyst_report', threadId: 'th-ws', workspaceId: 'ws-1', workspaceName: 'Semis',
      outcome: 'cancelled', runIds: ['run-1', 'run-1b'],
    });
  });

  it('leaves the outcome and runs out on a report that has none', () => {
    // Rows stored before they were kept.
    const row = reportRowProvenance({ kind: 'analyst', thread_id: 'th-ws' });
    expect(row).not.toHaveProperty('outcome');
    expect(row).not.toHaveProperty('runIds');
    expect(row).not.toHaveProperty('steerIds');
  });

  it('keeps the steers a report took in, even none, which an older report cannot say', () => {
    expect(reportRowProvenance({ kind: 'analyst', thread_id: 'th-ws', run_ids: ['run-2'], steer_ids: ['steer-1'] }))
      .toMatchObject({ runIds: ['run-2'], steerIds: ['steer-1'] });
    expect(reportRowProvenance({ kind: 'analyst', thread_id: 'th-ws', run_ids: ['run-1'], steer_ids: [] }))
      .toMatchObject({ steerIds: [] });
  });

  it('names the task, and whether it failed', () => {
    expect(reportRowProvenance({ kind: 'task', task_id: 'k7Xm2p', display_id: 'Task-k7Xm2p', description: 'Pull filings', failed: true }))
      .toEqual({ kind: 'task_report', taskId: 'k7Xm2p', displayId: 'Task-k7Xm2p', description: 'Pull filings', failed: true });
  });

  it('carries the subagent\'s role, when the report has it', () => {
    expect(reportRowProvenance({
      kind: 'task', task_id: 'k7Xm2p', display_id: 'Task-k7Xm2p', failed: false, subagent_type: 'research',
    })).toEqual({
      kind: 'task_report', taskId: 'k7Xm2p', displayId: 'Task-k7Xm2p', description: null, failed: false,
      subagentType: 'research',
    });
  });

  it('names a background command by what the agent said it does, and how it ended', () => {
    expect(reportRowProvenance({
      kind: 'command_report', command_id: 'bg-3f9a1c2e7d10', tool: 'Bash', description: 'Run 10-year backtest',
      status: 'completed', tool_call_id: 'call_1',
    })).toEqual({
      kind: 'command_report', tool: 'Bash', description: 'Run 10-year backtest',
      status: 'completed', failureReason: null, toolCallId: 'call_1',
    });
    expect(reportRowProvenance({
      kind: 'command_report', command_id: 'bg-77c0e41a9b2e', tool: 'ExecuteCode', status: 'failed', failure_reason: 'after_turn_limit',
    })).toEqual({
      kind: 'command_report', tool: 'ExecuteCode', description: null,
      status: 'failed', failureReason: 'after_turn_limit', toolCallId: null,
    });
  });

  it('keeps a report-back without a source hidden', () => {
    // A share's replay drops the source; older rows never had one.
    expect(reportRowProvenance(undefined)).toBeNull();
  });
});

describe('withLiveReportRow', () => {
  const source: ReportSource = { kind: 'analyst', thread_id: 'th-a', workspace_name: 'NVDA Earnings' };
  const user = (id: string, extra: Partial<UserMessage> = {}): UserMessage => ({
    id, role: 'user', content: '', contentType: 'text', timestamp: new Date(0), isStreaming: false, ...extra,
  });
  const transcript = (): ChatMessage[] => [
    user('u0', { content: 'Compare the gaps', turnIndex: 0 }),
    createAssistantMessage('a0', 0),
    createAssistantMessage('a-live', undefined),
  ];

  it('places the report row ahead of the bubble the attach opened', () => {
    const next = withLiveReportRow(transcript(), 'run-1', source, 'a-live');
    expect(next.map((m) => m.id)).toEqual(['u0', 'a0', 'report-run-1', 'a-live']);
    expect(next[2]).toMatchObject({
      role: 'user',
      content: '',
      provenance: { kind: 'analyst_report', threadId: 'th-a', workspaceName: 'NVDA Earnings' },
      runId: 'run-1',
    });
    expect(next[2]).not.toHaveProperty('turnIndex');
  });

  it('stamps the row with the turn its run was admitted to', () => {
    const next = withLiveReportRow(transcript(), 'run-1', source, 'a-live', 4);
    expect(next[2]).toMatchObject({ id: 'report-run-1', turnIndex: 4 });
  });

  it('keeps one row when the stream replays from its start', () => {
    const once = withLiveReportRow(transcript(), 'run-1', source, 'a-live');
    expect(withLiveReportRow(once, 'run-1', source, 'a-live')).toBe(once);
  });

  const task: ReportSource = { kind: 'task', task_id: 'k3', display_id: 'Task-k3', failed: false };
  const taskRow = (id: string, runId: string) => user(id, {
    turnIndex: 1, runId, provenance: { kind: 'task_report', taskId: 'k3', displayId: 'Task-k3', description: null, failed: false },
  });

  it('keeps the row history drew for a run still going when it reconnects', () => {
    const fromHistory: ChatMessage[] = [...transcript().slice(0, 2), taskRow('u1', 'run-1'), createAssistantMessage('a-live', undefined)];
    expect(withLiveReportRow(fromHistory, 'run-1', task, 'a-live')).toBe(fromHistory);
  });

  it('keeps the row of a run that ended before the attach, its reply under it', () => {
    const fromHistory: ChatMessage[] = [
      ...transcript().slice(0, 2),
      taskRow('u1', 'run-1'),
      createAssistantMessage('a1', 1),
      createAssistantMessage('a-live', undefined),
    ];
    expect(withLiveReportRow(fromHistory, 'run-1', task, 'a-live')).toBe(fromHistory);
  });

  it('gives a run its own row under the row a failed report-back left', () => {
    const left: ChatMessage[] = [...transcript().slice(0, 2), taskRow('u1', 'run-0'), createAssistantMessage('a-live', undefined)];
    const next = withLiveReportRow(left, 'run-1', source, 'a-live');
    expect(next.map((m) => m.id)).toEqual(['u0', 'a0', 'u1', 'report-run-1', 'a-live']);
  });
});

describe('handleHistoryUserMessage provenance', () => {
  const replay = (event: Record<string, unknown>, live: MessageRecord[] = []) => {
    const messages: MessageRecord[] = [...live];
    handleHistoryUserMessage({
      event: { event: 'user_message', turn_index: 4, timestamp: '2026-10-10T12:00:00Z', ...event },
      pairIndex: 4,
      assistantMessagesByPair: new Map(),
      pairStateByPair: new Map(),
      refs: {
        recentlySentTracker: { isRecentlySent: () => false },
        currentMessageRef: { current: null },
        newMessagesStartIndexRef: { current: 0 },
      },
      setMessages: (updater) => {
        const next = updater(messages.slice());
        messages.length = 0;
        messages.push(...next);
      },
    });
    return messages.filter((m) => m.role === 'user');
  };

  it('stamps the label on the bubble it belongs to', () => {
    const [bubble] = replay({ content: 'What moved NVDA?', metadata: { platform: 'telegram' } });
    expect(bubble).toMatchObject({ content: 'What moved NVDA?', provenance: { kind: 'channel', platform: 'telegram' } });
  });

  it('leaves the user\'s own message unlabelled', () => {
    const [bubble] = replay({ content: 'What moved NVDA?', metadata: { platform: 'web' } });
    expect(bubble.provenance).toBeUndefined();
  });

  it('replaces a report-back\'s hidden message with its label, keeping the text out', () => {
    const rows = replay({
      content: '<system>\nThe analysis you dispatched finished.\n</system>',
      query_type: 'system',
      report_run_id: 'run-4',
      metadata: { report_source: { kind: 'analyst', thread_id: 'th-ws', workspace_name: 'Semis' } },
    });
    expect(rows).toEqual([expect.objectContaining({
      id: 'history-user-4',
      content: '',
      turnIndex: 4,
      runId: 'run-4',
      provenance: { kind: 'analyst_report', threadId: 'th-ws', workspaceId: null, workspaceName: 'Semis' },
    })]);
  });

  it('takes over the row a live attach drew before the reload', () => {
    const live: MessageRecord[] = [
      { id: 'report-run-4', role: 'user', content: '', runId: 'run-4', provenance: { kind: 'task_report' } },
      { id: 'a-live', role: 'assistant', content: '' },
    ];
    const rows = replay({
      content: '<system>done</system>',
      query_type: 'system',
      report_run_id: 'run-4',
      metadata: { report_source: { kind: 'task', task_id: 'k3', display_id: 'Task-k3', failed: false } },
    }, live);
    expect(rows.map((m) => m.id)).toEqual(['history-user-4']);
  });

  it('keeps a report-back hidden when its row names no source', () => {
    expect(replay({ content: '<system>done</system>', query_type: 'system', metadata: {} })).toEqual([]);
  });
});

describe('handleHistorySteeringDelivered provenance', () => {
  it('labels a steer by the chat app it came from, as a turn of its own would be', () => {
    const messages: MessageRecord[] = [];
    handleHistorySteeringDelivered({
      event: {
        event: 'steering_delivered',
        messages: [
          { content: 'from the phone', timestamp: 1_700_000_000, platform: 'telegram' },
          { content: 'from the web', timestamp: 1_700_000_001, platform: 'web' },
        ],
      },
      pairIndex: 4,
      assistantMessagesByPair: new Map(),
      pairStateByPair: new Map(),
      refs: { newMessagesStartIndexRef: { current: 0 } },
      setMessages: (updater) => {
        const next = updater(messages.slice());
        messages.length = 0;
        messages.push(...next);
      },
    });
    const [phone, web] = messages.filter((m) => m.role === 'user');
    expect(phone).toMatchObject({ content: 'from the phone', provenance: { kind: 'channel', platform: 'telegram' } });
    expect(web.provenance).toBeUndefined();
  });
});

describe('handleHistorySteeringDelivered report entries', () => {
  const replay = (messagesIn: Record<string, unknown>[], live: MessageRecord[] = []) => {
    const messages: MessageRecord[] = [...live];
    handleHistorySteeringDelivered({
      event: { event: 'steering_delivered', messages: messagesIn },
      pairIndex: 4,
      assistantMessagesByPair: new Map(),
      pairStateByPair: new Map(),
      refs: { newMessagesStartIndexRef: { current: 0 } },
      setMessages: (updater) => {
        const next = updater(messages.slice());
        messages.length = 0;
        messages.push(...next);
      },
    });
    return messages;
  };

  it('draws a report as its caption row ahead of the user\'s bubble, then the steering placeholder', () => {
    const rows = replay([
      { content: '', report_source: { kind: 'task', task_id: 'k3', display_id: 'Task-k3', failed: false }, timestamp: 1_700_000_000 },
      { content: 'and margins', timestamp: 1_700_000_001 },
    ]);
    expect(rows).toHaveLength(3);
    expect(rows[0].id).toBe('history-steering-report-4-1-0');
    expect(rows[0]).toMatchObject({
      role: 'user',
      content: '',
      isHistory: true,
      steeringDelivered: true,
      turnIndex: 4,
      provenance: { kind: 'task_report', taskId: 'k3', displayId: 'Task-k3', description: null, failed: false },
    });
    expect(rows[1]).toMatchObject({ role: 'user', content: 'and margins', steeringDelivered: true });
    expect(rows[1].id).toBe('history-steering-user-4-1-1');
    expect(rows[2]).toMatchObject({ role: 'assistant', isSteering: true });
  });

  it('skips an entry with neither a report source nor words', () => {
    const rows = replay([{ content: '' }]);
    expect(rows.filter((m) => m.role === 'user')).toEqual([]);
  });

  it('takes no live steer\'s place, even one with no words', () => {
    const steer = { id: 's1', role: 'user', content: '', steering: true, turnIndex: 4 };
    const rows = replay([{ content: '', report_source: { kind: 'task', task_id: 'k3', display_id: 'Task-k3', failed: false } }], [steer]);
    expect(rows.map((m) => m.id)).toEqual(['history-steering-report-4-1-0', 'history-assistant-steering-4-1', 's1']);
  });
});

describe('minimap entries', () => {
  it('folds a report-back\'s reply into the entry above, as while its row was hidden', () => {
    const messages: MessageRecord[] = [
      { id: 'u1', role: 'user', content: 'Size up semis' },
      { id: 'a1', role: 'assistant', content: 'Dispatched.', contentSegments: [{ type: 'text', content: 'Dispatched.', order: 0 }] },
      { id: 'r2', role: 'user', content: '', provenance: { kind: 'analyst_report', threadId: 't', workspaceId: null, workspaceName: 'Semis' } },
      { id: 'a2', role: 'assistant', content: 'Semis report is in.', contentSegments: [{ type: 'text', content: 'Semis report is in.', order: 0 }] },
    ];
    const entries = buildEntries(messages, false);
    expect(entries.map((e) => e.id)).toEqual(['u1']);
    expect(entries[0].reply).toContain('Semis report is in.');
  });
});
