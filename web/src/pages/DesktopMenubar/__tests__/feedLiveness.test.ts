// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { OVERDUE_MS, panelLiveness, rowLiveness, type LivenessRow } from '../feedLiveness';

const NOW = 1_800_000_000_000;

function row(over: Partial<LivenessRow> = {}): LivenessRow {
  return {
    key: 'AAPL',
    isIndex: false,
    fields: { tier: 'realtime', freshness: { label: 'live', measured: true, closed: false } },
    updatedAt: NOW - 5_000,
    fetching: false,
    streaming: false,
    ...over,
  };
}

const lastClose = { tier: 'delayed_15m' as const, freshness: { label: 'live' as const, measured: true, closed: true } };
const stale = { tier: 'realtime' as const, freshness: { label: 'stale' as const, measured: true, closed: false } };

describe('panelLiveness', () => {
  it('reads Live while the socket streams, over an index at its last close', () => {
    const state = panelLiveness([row({ key: 'GSPC', isIndex: true, fields: lastClose }), row({ streaming: true })], NOW);
    expect(state).toMatchObject({ dot: 'live', row: { kind: 'streaming' } });
  });

  it('reads a polled current quote as realtime, and only indices at their close as last close', () => {
    expect(panelLiveness([row()], NOW)?.dot).toBe('realtime');
    expect(panelLiveness([row({ isIndex: true, fields: lastClose })], NOW)).toMatchObject({
      dot: 'closed',
      row: { spec: { key: 'marketView.header.statusLastClose' } },
    });
  });

  it('lets one stale row name the whole panel', () => {
    const state = panelLiveness([row({ streaming: true }), row({ key: 'MSFT', fields: stale })], NOW);
    expect(state).toMatchObject({ dot: 'warning', row: { spec: { key: 'marketView.header.statusStale' } } });
  });

  it('calls a figure not refreshed past the limit overdue, ahead of a stale label', () => {
    const overdue = row({ key: 'GSPC', isIndex: true, fields: lastClose, updatedAt: NOW - OVERDUE_MS - 1 });
    expect(panelLiveness([row({ fields: stale }), overdue], NOW)?.row.kind).toBe('overdue');
    // A read in flight is not overdue: the panel just opened and asked again.
    expect(rowLiveness({ ...overdue, fetching: true }, NOW)?.kind).toBe('freshness');
    // An overdue row is overdue even while the socket streams it: no tick has reached it either.
    expect(rowLiveness({ ...overdue, streaming: true }, NOW)?.kind).toBe('overdue');
  });

  it('says nothing for rows that carry no freshness', () => {
    expect(panelLiveness([row({ fields: { tier: null, freshness: null } })], NOW)).toBeNull();
    expect(panelLiveness([], NOW)).toBeNull();
  });
});
