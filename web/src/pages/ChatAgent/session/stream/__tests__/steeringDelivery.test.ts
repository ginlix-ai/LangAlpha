// @vitest-environment node
import { describe, expect, it } from 'vitest';
import type { ReportSource } from '@/types/api';
import type { ChatMessage, UserMessage } from '@/types/chat';
import type { DeliveredMarketAlert, DeliveredReport, DeliveredSteer, SteeringDeliveredEntry } from '@/types/sse';
import { createAssistantMessage } from '../../../hooks/utils/messageHelpers';
import { deliveredEntryRow, foldSteeringDelivery, type DeliveredRowKind } from '../steeringDelivery';

const TASK: ReportSource = { kind: 'task', task_id: 'k3', display_id: 'Task-k3', description: 'Pull filings', failed: false };
const TASK_PROVENANCE = { kind: 'task_report', taskId: 'k3', displayId: 'Task-k3', description: 'Pull filings', failed: false };
const BANNER = { message: 'Steer failed to send' };

const report: DeliveredReport = { content: '', report_source: TASK, timestamp: 1_700_000_000 };
// Neutral placeholder ticker and made-up prices only.
const NOTICE = {
  target_id: 'tg-1', notice: 'hit', symbol: 'ACME', kind: 'above', value: 205, pct_from: null,
  fired_price: 205.12, fired_at: '2030-01-01T15:05:00Z', expires_at: null,
} as const;
const alert: DeliveredMarketAlert = { content: '', market_alert: NOTICE, timestamp: 1_700_000_000 };
const ALERT_ID = 'market-alert-tg-1:hit';
// The notice as a price wakeup's, which its row is drawn from.
const AS_WAKEUP = {
  wakeup_id: 'tg-1', kind: 'price', notice: 'fire', seq: 1, total: 1, fired_at: '2030-01-01T15:05:00Z',
  price: { symbol: 'ACME', kind: 'above', value: 205, pct_from: null, fired_price: 205.12 },
};
const steer = (content: string, extra: Partial<DeliveredSteer> = {}): DeliveredSteer => ({ content, ...extra });

type Failed = UserMessage & { queueError?: unknown };
const user = (id: string, extra: Partial<Failed> = {}): Failed => ({
  id, role: 'user', content: '', contentType: 'text', timestamp: new Date(0), isStreaming: false, turnIndex: 0, ...extra,
});
const waiting = (id: string, content: string, extra: Partial<Failed> = {}) => user(id, { content, steering: true, ...extra });
const failed = (id: string, content: string, extra: Partial<Failed> = {}) => user(id, { content, queueError: BANNER, ...extra });
const transcript = (): ChatMessage[] => [user('u0', { content: 'Compare the gaps' }), createAssistantMessage('a1', 0)];

function mint() {
  let n = 0;
  return (kind: DeliveredRowKind) => `${kind}-${n++}`;
}

const fold = (prev: ChatMessage[], entries: SteeringDeliveredEntry[], turnIndex: number | undefined = 0) =>
  foldSteeringDelivery(prev, entries, turnIndex, mint());
const byId = (messages: ChatMessage[], id: string) => messages.find((m) => m.id === id);

describe('deliveredEntryRow', () => {
  const at = new Date(1_700_000_000_000);
  const id = (kind: DeliveredRowKind) => `row-${kind}`;

  it('draws a report as its caption row, delivered steering in the turn it landed in', () => {
    expect(deliveredEntryRow(report, id, 2)).toEqual({
      id: 'row-report',
      role: 'user',
      content: '',
      contentType: 'text',
      timestamp: at,
      isStreaming: false,
      provenance: TASK_PROVENANCE,
      steeringDelivered: true,
      turnIndex: 2,
    });
  });

  it('leaves a report\'s turn unstamped when it is not known', () => {
    const row = deliveredEntryRow({ content: '', report_source: { kind: 'analyst', thread_id: 'th-a', workspace_name: 'Semis' } }, id, undefined);
    expect(row).toMatchObject({
      provenance: { kind: 'analyst_report', threadId: 'th-a', workspaceId: null, workspaceName: 'Semis' },
      steeringDelivered: true,
    });
    expect(row).not.toHaveProperty('turnIndex');
  });

  it('draws the user\'s words labelled with the chat app they came from', () => {
    expect(deliveredEntryRow(steer('from the phone', { platform: 'telegram', timestamp: 1_700_000_000 }), id, 2)).toEqual({
      id: 'row-user',
      role: 'user',
      content: 'from the phone',
      contentType: 'text',
      timestamp: at,
      isStreaming: false,
      steeringDelivered: true,
      turnIndex: 2,
      provenance: { kind: 'channel', platform: 'telegram' },
    });
  });

  it('labels a steer the Chief of Staff sent into the turn, and keeps its request key', () => {
    const row = deliveredEntryRow(
      steer('add the margin bridge', { origin: { type: 'agent', agent: 'chief_of_staff' }, request_key: 'rk-cos' }),
      id,
      2,
    );
    // The delivered origin names the agent only, never its Home thread.
    expect(row).toMatchObject({
      content: 'add the margin bridge',
      steeringDelivered: true,
      requestKey: 'rk-cos',
      provenance: { kind: 'agent', agent: 'chief_of_staff', threadId: null },
    });
  });

  it('draws the user\'s own steer unlabelled', () => {
    const row = deliveredEntryRow(steer('margins', { platform: 'web', request_key: 'rk-1' }), id, 2);
    expect(row).not.toHaveProperty('provenance');
    expect(row).toMatchObject({ requestKey: 'rk-1' });
  });

  it('draws nothing for what a share leaves of a report', () => {
    expect(deliveredEntryRow(steer(''), id, 2)).toBeNull();
  });

  it("draws a price alert as its row, under its notice's id whatever the caller mints", () => {
    const row = deliveredEntryRow(alert, id, 2);
    expect(row).toMatchObject({
      id: ALERT_ID,
      role: 'notification',
      content: '',
      variant: 'info',
      timestamp: at,
      wakeup: { source: 'delivered', legacy: true, notices: [AS_WAKEUP] },
    });
    expect(deliveredEntryRow(alert, (kind) => `history-${kind}`, undefined)).toEqual(row);
  });

  it('draws nothing for an alert naming no target', () => {
    const named = { content: '', market_alert: { symbol: 'ACME' } } as unknown as DeliveredMarketAlert;
    expect(deliveredEntryRow(named, id, 2)).toBeNull();
  });
});

describe('foldSteeringDelivery: claiming', () => {
  it('marks the waiting steer delivered where it sits and makes no copy', () => {
    const prev = [...transcript(), waiting('s1', 'also check margins')];
    const { messages, clearedErrors } = fold(prev, [steer('also check margins')]);
    expect(messages.map((m) => m.id)).toEqual(['u0', 'a1', 's1']);
    expect(byId(messages, 's1')).toMatchObject({ steering: false, steeringDelivered: true });
    expect(clearedErrors).toEqual([]);
  });

  it('takes the waiting copy over a failed one', () => {
    const prev = [...transcript(), failed('f1', 'margins'), waiting('s1', 'margins')];
    const { messages } = fold(prev, [steer('margins')]);
    expect(byId(messages, 's1')).toMatchObject({ steeringDelivered: true });
    expect(byId(messages, 'f1')).toBe(prev[2]);
  });

  it('takes a failed steer the server may have queued, and clears its banner', () => {
    const prev = [...transcript(), failed('f1', 'margins')];
    const { messages, clearedErrors } = fold(prev, [steer('margins')]);
    expect(byId(messages, 'f1')).toMatchObject({ steeringDelivered: true });
    expect(byId(messages, 'f1')).not.toHaveProperty('queueError');
    expect(clearedErrors).toEqual([BANNER]);
  });

  it('keeps a banner another failed steer still holds', () => {
    const prev = [...transcript(), failed('f1', 'margins'), failed('f2', 'and capex')];
    const { clearedErrors } = fold(prev, [steer('margins')]);
    expect(clearedErrors).toEqual([]);
  });

  it.each([
    // The server refuses a steer before it queues it, so no delivery is its.
    ['a refused steer', failed('f1', 'margins', { queueRefused: true })],
    ['a steer of another turn', waiting('s1', 'margins', { turnIndex: 1 })],
  ])('leaves %s alone and draws the words as their own row', (_, bubble) => {
    const prev = [...transcript(), bubble];
    const { messages } = fold(prev, [steer('margins')]);
    expect(messages[2]).toBe(bubble);
    expect(messages[3]).toMatchObject({ id: 'user-0', content: 'margins', steeringDelivered: true, turnIndex: 0 });
  });

  it('returns the transcript itself when it draws and claims nothing', () => {
    const prev = [...transcript(), waiting('s1', 'margins')];
    expect(fold(prev, [steer('')]).messages).toBe(prev);
  });
});

describe('foldSteeringDelivery: claiming by request key', () => {
  const keyed = (content: string, key: string) => steer(content, { request_key: key });

  it('claims the bubble sent under the key, not one with the same words', () => {
    const prev = [...transcript(), waiting('s1', 'margins', { requestKey: 'rk-other' }), waiting('s2', 'margins', { requestKey: 'rk-2' })];
    const { messages, settledKeys } = fold(prev, [keyed('margins', 'rk-2')]);
    expect(byId(messages, 's2')).toMatchObject({ steering: false, steeringDelivered: true });
    expect(byId(messages, 's1')).toBe(prev[2]);
    expect(messages).toHaveLength(prev.length);
    expect(settledKeys).toEqual(['rk-2']);
  });

  it('claims the bubble under the key whatever its words say', () => {
    // The entry's text is the model's copy; the key is what names the send.
    const prev = [...transcript(), waiting('s1', 'margins pls', { requestKey: 'rk-1' })];
    const { messages } = fold(prev, [keyed('margins', 'rk-1')]);
    expect(messages.map((m) => m.id)).toEqual(['u0', 'a1', 's1']);
    expect(byId(messages, 's1')).toMatchObject({ steeringDelivered: true, content: 'margins pls' });
  });

  it('claims a steer waiting under an earlier turn, which joins the delivering turn where it is drawn', () => {
    const prev = [...transcript(), waiting('s1', 'margins', { requestKey: 'rk-1', turnIndex: 0 }), user('u1', { content: 'next', turnIndex: 1 }), createAssistantMessage('a2', 1)];
    const { messages } = fold(prev, [report, keyed('margins', 'rk-1')], 1);
    expect(byId(messages, 's1')).toMatchObject({ steeringDelivered: true, turnIndex: 1 });
    // It places nothing: the report goes at the end of the delivering turn.
    expect(messages.map((m) => m.id)).toEqual(['u0', 'a1', 's1', 'u1', 'a2', 'report-0']);
  });

  it('of copies under one key claims the one still waiting and removes the rest', () => {
    const prev = [
      ...transcript(),
      failed('f1', 'margins', { requestKey: 'rk-1' }),
      failed('f2', 'margins', { requestKey: 'rk-1', queueRefused: true }),
      waiting('s1', 'margins', { requestKey: 'rk-1' }),
    ];
    const { messages, clearedErrors, settledKeys } = fold(prev, [keyed('margins', 'rk-1')]);
    expect(messages.map((m) => m.id)).toEqual(['u0', 'a1', 's1']);
    expect(byId(messages, 's1')).toMatchObject({ steeringDelivered: true });
    // The removed copies' banner goes with them (the view clears the one it names).
    expect([...new Set(clearedErrors)]).toEqual([BANNER]);
    expect(settledKeys).toEqual(['rk-1']);
  });

  it('with none waiting claims a failed copy the server may have taken over a refused one', () => {
    const prev = [
      ...transcript(),
      failed('f1', 'margins', { requestKey: 'rk-1', queueRefused: true }),
      failed('f2', 'margins', { requestKey: 'rk-1' }),
    ];
    const { messages } = fold(prev, [keyed('margins', 'rk-1')]);
    expect(messages.map((m) => m.id)).toEqual(['u0', 'a1', 'f2']);
    expect(byId(messages, 'f2')).toMatchObject({ steeringDelivered: true });
    expect(byId(messages, 'f2')).not.toHaveProperty('queueError');
  });

  it('leaves a copy already delivered alone and draws an entry no bubble carries as its own row', () => {
    const prev = [...transcript(), user('d1', { content: 'margins', requestKey: 'rk-1', steeringDelivered: true }), waiting('s1', 'margins', { requestKey: 'rk-2' })];
    const { messages } = fold(prev, [keyed('margins', 'rk-1')]);
    expect(byId(messages, 'd1')).toBe(prev[2]);
    expect(byId(messages, 's1')).toBe(prev[3]);
    expect(byId(messages, 'user-0')).toMatchObject({ content: 'margins', requestKey: 'rk-1', steeringDelivered: true });
  });
});

describe('foldSteeringDelivery: placing reports', () => {
  it.each<[string, ChatMessage[], SteeringDeliveredEntry[], string[]]>([
    [
      'above the steer it is delivered with',
      [...transcript(), waiting('s1', 'margins')],
      [report, steer('margins')],
      ['u0', 'a1', 'report-0', 's1'],
    ],
    [
      // The user sent s1 just after the run took its queue: replay draws the
      // report above s1, so the live view does too.
      'alone, above a steer still waiting in the turn',
      [...transcript(), waiting('s1', 'margins')],
      [report],
      ['u0', 'a1', 'report-0', 's1'],
    ],
    [
      'alone, at the end when the waiting steer is another turn\'s',
      [...transcript(), waiting('s1', 'margins', { turnIndex: 1 })],
      [report],
      ['u0', 'a1', 's1', 'report-0'],
    ],
    [
      // Sent from another tab or a chat app: delivered, so above the steer
      // the model has yet to read.
      'a steer sent elsewhere, above a steer still waiting',
      [...transcript(), waiting('s1', 'margins')],
      [steer('and costs')],
      ['u0', 'a1', 'user-0', 's1'],
    ],
    [
      'reports, then a steer sent elsewhere, after the steer it delivers',
      [...transcript(), waiting('s1', 'margins'), waiting('s2', 'and costs')],
      [report, steer('margins'), steer('and capex')],
      ['u0', 'a1', 'report-0', 's1', 'user-1', 's2'],
    ],
    [
      'ahead of the user\'s rows when nothing is pending (reconnect)',
      transcript(),
      [report, steer('and margins')],
      ['u0', 'a1', 'report-0', 'user-1'],
    ],
  ])('%s', (_, prev, entries, ids) => {
    expect(fold(prev, entries).messages.map((m) => m.id)).toEqual(ids);
  });

  it('leaves the user\'s waiting steer waiting when only reports arrive', () => {
    const prev = [...transcript(), waiting('s1', 'margins')];
    const { messages } = fold(prev, [report]);
    expect(byId(messages, 's1')).toBe(prev[2]);
    expect(byId(messages, 'report-0')).toMatchObject({ provenance: TASK_PROVENANCE, steeringDelivered: true, turnIndex: 0 });
  });
});

describe('foldSteeringDelivery: placing price alerts', () => {
  it.each<[string, ChatMessage[], SteeringDeliveredEntry[], string[]]>([
    [
      'above the steer it is delivered with',
      [...transcript(), waiting('s1', 'margins')],
      [alert, steer('margins')],
      ['u0', 'a1', ALERT_ID, 's1'],
    ],
    [
      'alone, above a steer still waiting in the turn',
      [...transcript(), waiting('s1', 'margins')],
      [alert],
      ['u0', 'a1', ALERT_ID, 's1'],
    ],
    [
      'with a report, in the order delivered, ahead of the user\'s rows',
      transcript(),
      [alert, report, steer('and margins')],
      ['u0', 'a1', ALERT_ID, 'report-0', 'user-1'],
    ],
  ])('%s', (_, prev, entries, ids) => {
    expect(fold(prev, entries).messages.map((m) => m.id)).toEqual(ids);
  });

  it('leaves the user\'s waiting steer waiting when only alerts arrive', () => {
    const prev = [...transcript(), waiting('s1', 'margins')];
    const { messages } = fold(prev, [alert]);
    expect(byId(messages, 's1')).toBe(prev[2]);
  });

  it('draws an alert already on screen once', () => {
    const prev = [...transcript(), ...fold(transcript(), [alert]).messages.slice(2)];
    expect(fold(prev, [alert]).messages).toBe(prev);
  });
});
