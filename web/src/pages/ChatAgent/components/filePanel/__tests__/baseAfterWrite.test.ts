import { describe, it, expect } from 'vitest';
import { baseAfterWrite } from '../useFileEdit';

describe('baseAfterWrite', () => {
  const read = { version: '17:42+u', sha256: 'abc' };

  it('takes the base a write returned', () => {
    expect(baseAfterWrite({ version: '18:50', sha256: null }, read)).toEqual({ version: '18:50', sha256: null });
    expect(baseAfterWrite({ version: null, sha256: 'def' }, read)).toEqual({ version: null, sha256: 'def' });
  });

  it('keeps the read base when the write named nothing to check against', () => {
    expect(baseAfterWrite({ version: null, sha256: null }, read)).toBe(read);
  });

  it('has nothing to keep on a file never read', () => {
    const none = { version: null, sha256: null };
    expect(baseAfterWrite(none, null)).toBe(none);
  });
});
