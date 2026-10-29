// @vitest-environment node
import { describe, it, expect } from 'vitest';
import {
  callChangesWakeups, coerceWakeNotice, deliveredNoticeRow, quietTurnStatus, wakeTurnOpener,
} from '../wakeupEvents';

// Made-up ids, labels and levels only.
const NOTICE = {
  wakeup_id: 'wk-1',
  short_id: 'w_ab12',
  label: 'morning check',
  kind: 'at',
  notice: 'fire',
  seq: 1,
  total: 1,
  last: true,
  skipped: 0,
  fired_at: '2030-01-02T14:00:00Z',
  timezone: 'America/New_York',
  cron: null,
  price: null,
  order: null,
};

type Call = {
  toolName?: string; toolCall?: { name?: string; args?: unknown }; toolCallResult?: unknown; order?: number;
};
type Segment = { type: string; content?: string; order: number };
const bubble = (...calls: Call[]) => ({
  toolCallProcesses: Object.fromEntries(calls.map((c, i) => [`tc${i}`, c])),
});
const said = (content: string, order: number) => ({ type: 'text', content, order });
const withText = (b: ReturnType<typeof bubble>, ...segments: Segment[]) => ({ ...b, contentSegments: segments });
const ended = { content: 'Ended without a reply.', artifact: { quiet: true, changed: false } };
const refused = { content: 'quiet only ends a turn a wakeup started', artifact: { quiet: false, changed: false } };
// A schedule_wakeup call whose result accepted quiet, unless `over` says otherwise.
const wake = (args: unknown, over: Call = {}): Call => ({
  toolName: 'schedule_wakeup', toolCall: { name: 'schedule_wakeup', args }, toolCallResult: ended, ...over,
});

describe('quietTurnStatus', () => {
  it('reads the status a quiet end passed', () => {
    expect(quietTurnStatus([bubble({ toolName: 'get_quote', order: 1 }, wake({ quiet: ' nothing new ' }, { order: 2 }))]))
      .toBe('nothing new');
  });

  it('reads the last call across the turn\'s bubbles', () => {
    expect(quietTurnStatus([bubble(wake({ quiet: 'nothing new' }, { order: 1 })), bubble({ toolName: 'get_quote', order: 1 })]))
      .toBeNull();
    expect(quietTurnStatus([bubble({ toolName: 'get_quote', order: 1 }), bubble(wake({ quiet: 'nothing new' }, { order: 1 }))]))
      .toBe('nothing new');
  });

  it('counts a turn that set or cancelled a wakeup as changed, a refused entry too', () => {
    const set = wake({ triggers: [{ at: '2030-01-02T15:00' }] }, { order: 1, toolCallResult: refused });
    expect(quietTurnStatus([bubble(set, wake({ quiet: 'nothing new' }, { order: 2 }))])).toBeNull();
    expect(quietTurnStatus([bubble(wake({ cancel: '["w_ab12"]', quiet: 'nothing new' }))])).toBeNull();
  });

  it('is not quiet when the tool did not accept quiet, is still running, or passed no status', () => {
    expect(quietTurnStatus([bubble(wake({ quiet: 'nothing new' }, { toolCallResult: refused }))])).toBeNull();
    // Arguments the tool could not parse: it answers, and ends nothing.
    expect(quietTurnStatus([bubble(wake({ quiet: 'nothing new', extra: true }, { toolCallResult: { content: 'x' } }))]))
      .toBeNull();
    expect(quietTurnStatus([bubble(wake({ quiet: 'nothing new' }, { toolCallResult: null }))])).toBeNull();
    expect(quietTurnStatus([bubble(wake({ quiet: '  ' }))])).toBeNull();
    expect(quietTurnStatus([])).toBeNull();
  });

  // A background task's report can re-invoke the agent after a quiet end,
  // and what it says then is a reply.
  it('is not quiet when the agent spoke after the quiet end', () => {
    const quiet = bubble(wake({ quiet: 'nothing new' }, { order: 1 }));
    expect(quietTurnStatus([quiet, withText(bubble(), said('The task finished: NVDA held.', 0))])).toBeNull();
    expect(quietTurnStatus([withText(quiet, said('The task finished.', 2))])).toBeNull();
    expect(quietTurnStatus([withText(quiet, said('Checking the tape.', 0), said('  ', 2))])).toBe('nothing new');
  });
});

describe('callChangesWakeups', () => {
  it('reads lists the model sent as JSON text', () => {
    expect(callChangesWakeups({ triggers: '[]', cancel: '[]', quiet: 'x' })).toBe(false);
    expect(callChangesWakeups({ triggers: '[{"at": "2030-01-02T15:00"}]' })).toBe(true);
    expect(callChangesWakeups({ cancel: 'all' })).toBe(true);
    expect(callChangesWakeups({ triggers: null })).toBe(false);
    expect(callChangesWakeups({ triggers: {} })).toBe(false);
    expect(callChangesWakeups(undefined)).toBe(false);
  });
});

describe('wakeup rows', () => {
  const delivered = (wakeup: unknown) => deliveredNoticeRow({ content: '', wakeup } as never, new Date(0));
  const target = (market_alert: unknown) => deliveredNoticeRow({ content: '', market_alert } as never, new Date(0));

  it('keys a wake turn\'s row on the turn, so a replay and a live attach draw one', () => {
    const row = wakeTurnOpener(4, { wakeup: { v: 1, notices: [NOTICE, { label: 'no id' }] } }, new Date(0));
    expect(row?.id).toBe('history-wakeup-wake-4');
    expect(row?.wakeup).toMatchObject({ source: 'wake', turnIndex: 4 });
    expect(row?.wakeup?.notices.map((n) => n.wakeup_id)).toEqual(['wk-1']);
    expect(wakeTurnOpener(4, { llm_model: 'm' }, new Date(0))).toBeNull();
  });

  it('keys a delivered row on its notice slot, and draws none for an entry naming no wakeup', () => {
    expect(delivered({ ...NOTICE, seq: 3, total: 5 })?.id).toBe('wakeup-wk-1:fire:3');
    expect(delivered({ ...NOTICE, notice: 'lapse' })?.id).toBe('wakeup-wk-1:lapse:1');
    expect(delivered({ label: 'no id' })).toBeNull();
  });

  it('reads a price target\'s notice as a price wakeup\'s, under the ids its rows always had', () => {
    const wake = wakeTurnOpener(2, { market_target: { notices: [{ target_id: 'tg-a' }, { target_id: 'tg-b', notice: 'expiring' }] } }, new Date(0));
    expect(wake?.id).toBe('history-market-alert-wake-2');
    expect(wake?.wakeup).toMatchObject({ source: 'wake', turnIndex: 2, legacy: true });
    expect(wake?.wakeup?.notices.map((n) => [n.wakeup_id, n.kind, n.notice, n.price])).toEqual([
      ['tg-a', 'price', 'fire', null], ['tg-b', 'price', 'lapse', null],
    ]);
    expect(wakeTurnOpener(2, {}, new Date(0), 'market_target')?.wakeup).toMatchObject({ notices: [], legacy: true });

    const hit = target({ target_id: 'tg-a', symbol: 'ACME', kind: 'above', value: 205, fired_price: 205.12 });
    expect(hit?.id).toBe('market-alert-tg-a:hit');
    expect(hit?.wakeup?.notices[0]).toMatchObject({
      kind: 'price', notice: 'fire', seq: 1, total: 1,
      price: { symbol: 'ACME', kind: 'above', value: 205, fired_price: 205.12, armed_price: null },
    });
    expect(target({ target_id: 'tg-a', notice: 'expiring' })?.id).toBe('market-alert-tg-a:expiring');
    expect(target({ target_id: 'tg-a', notice: 'later', kind: 'sideways', value: '205' })?.wakeup?.notices[0])
      .toMatchObject({ notice: 'fire', price: null });
    expect(target({ symbol: 'ACME' })).toBeNull();
  });
});

describe('coerceWakeNotice', () => {
  it('keeps the currency an order\'s fill is priced in', () => {
    const notice = coerceWakeNotice({
      ...NOTICE,
      kind: 'order',
      order: { vendor_order_id: 'v-1', summary: 'Buy 10 ACME', outcome: 'filled', filled_qty: 10, qty: 10, avg_fill_price: 199.98, currency: 'USD' },
    });
    expect(notice?.order).toMatchObject({ avg_fill_price: 199.98, currency: 'USD' });
    expect(coerceWakeNotice({ ...NOTICE, kind: 'order', order: { summary: 'Buy 10 ACME' } })?.order?.currency).toBeNull();
  });
});
