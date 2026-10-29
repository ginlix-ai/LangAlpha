// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { authSessionCheck, registerAuthReset, runAuthResets } from '@/lib/authResets';

describe('authSessionCheck', () => {
  it('fails once a sign-out runs, and a check taken after it holds', () => {
    const before = authSessionCheck();
    expect(before()).toBe(true);

    runAuthResets();
    const after = authSessionCheck();
    expect(before()).toBe(false);
    expect(after()).toBe(true);
  });

  // A reset can wake what it tears down, and that work must already read as
  // the last account's.
  it('has moved on by the time the resets run', () => {
    const before = authSessionCheck();
    let seen: boolean | null = null;
    registerAuthReset(() => { seen = before(); });
    runAuthResets();
    expect(seen).toBe(false);
  });
});
