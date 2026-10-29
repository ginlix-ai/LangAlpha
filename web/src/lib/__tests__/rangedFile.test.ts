// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { parseContentRange } from '../rangedFile';

describe('parseContentRange', () => {
  it('reads a satisfied range', () => {
    expect(parseContentRange('bytes 0-4194303/40701591')).toEqual({ first: 0, last: 4194303, total: 40701591 });
    expect(parseContentRange('Bytes 10-10/11')).toEqual({ first: 10, last: 10, total: 11 });
  });

  it('refuses what gives no position or no length to read on from', () => {
    expect(parseContentRange(undefined)).toBeNull();
    expect(parseContentRange('bytes */40701591')).toBeNull();
    expect(parseContentRange('bytes 0-99/*')).toBeNull();
    expect(parseContentRange('bytes 5-4/10')).toBeNull();
    expect(parseContentRange('bytes 0-10/10')).toBeNull();
    expect(parseContentRange('items 0-1/2')).toBeNull();
  });
});
