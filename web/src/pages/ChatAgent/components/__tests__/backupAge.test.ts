// @vitest-environment node
import { describe, expect, it } from 'vitest';

import { backupAge } from '../backupAge';

const NOW = Date.parse('2026-03-02T12:00:00Z');

describe('backupAge', () => {
  it('drops a time under a minute old, which would read "taken now"', () => {
    expect(backupAge(new Date(NOW - 30_000).toISOString(), 'en-US', NOW)).toBe('');
    // A server clock a little ahead reads the same.
    expect(backupAge(new Date(NOW + 20_000).toISOString(), 'en-US', NOW)).toBe('');
  });

  it('says how long ago an older backup was taken', () => {
    expect(backupAge(new Date(NOW - 6 * 60_000).toISOString(), 'en-US', NOW)).not.toBe('');
  });

  it('is empty without a time', () => {
    expect(backupAge(null, 'en-US', NOW)).toBe('');
    expect(backupAge('not a date', 'en-US', NOW)).toBe('');
  });
});
