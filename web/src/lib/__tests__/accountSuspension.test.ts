// @vitest-environment node
import { afterEach, describe, it, expect } from 'vitest';
import { accountSuspendedNotice, noteIfSuspended, resetAccountSuspended } from '../accountSuspension';

afterEach(() => resetAccountSuspended());

describe('account suspension', () => {
  it('notes a route refusal and a stream frame alike', () => {
    expect(noteIfSuspended({ type: 'account_suspended', organization: 'Northfield' })).toBe(true);
    expect(accountSuspendedNotice()).toEqual({ organization: 'Northfield' });
    resetAccountSuspended();
    expect(noteIfSuspended({ error_type: 'account_suspended' })).toBe(true);
    expect(accountSuspendedNotice()).toEqual({ organization: null });
  });

  it('ignores every other refusal', () => {
    expect(noteIfSuspended({ type: 'policy_denied', rule: 'trading' })).toBe(false);
    expect(noteIfSuspended(null)).toBe(false);
    expect(accountSuspendedNotice()).toBeNull();
  });

  it('keeps the first name it is given', () => {
    noteIfSuspended({ type: 'account_suspended' });
    noteIfSuspended({ type: 'account_suspended', organization: 'Northfield' });
    noteIfSuspended({ type: 'account_suspended', organization: 'Other' });
    noteIfSuspended({ type: 'account_suspended' });
    expect(accountSuspendedNotice()).toEqual({ organization: 'Northfield' });
  });
});
