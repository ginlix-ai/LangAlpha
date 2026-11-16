import { describe, it, expect } from 'vitest';
import { awayRefetch } from '../computerAway';

const backupBody = { from: { kind: 'backup', unreachable: 'not_connected' } };

describe('awayRefetch', () => {
  it('stops on a 4xx even while a backup body from an earlier read is kept', () => {
    const query = { state: { error: null as unknown, data: backupBody as unknown } };
    expect(awayRefetch(query, 'not_connected')).toBe(15_000);

    query.state.error = { status: 404 };
    expect(awayRefetch(query, 'not_connected')).toBe(false);
  });

  it('keeps asking past a failure that says nothing about the machine', () => {
    const query = { state: { error: null as unknown, data: backupBody as unknown } };
    awayRefetch(query, 'not_connected');

    query.state.error = { status: 502 };
    expect(awayRefetch(query, null)).toBe(15_000);
  });
});
