import { describe, expect, it } from 'vitest';
import { runAuthResets } from '../authResets';
import { holdPageLeave, reloadHeld } from '../reloadHolds';

const asked = () => {
  const event = new Event('beforeunload', { cancelable: true });
  window.dispatchEvent(event);
  return event.defaultPrevented;
};

describe('a hold on leaving the page', () => {
  it('asks before the page goes while any hold is out, and a second release does nothing', () => {
    const one = holdPageLeave();
    const two = holdPageLeave();
    expect(asked()).toBe(true);
    one();
    one();
    expect(asked()).toBe(true);
    expect(reloadHeld()).toBe(true);
    two();
    expect(asked()).toBe(false);
    expect(reloadHeld()).toBe(false);
  });

  it("ends with its account: the last account's stalled work holds neither the page nor the next account's holds", () => {
    const stalled = holdPageLeave();
    runAuthResets();
    expect(asked()).toBe(false);
    expect(reloadHeld()).toBe(false);

    const next = holdPageLeave();
    // Landing late, the old release takes nothing of the next account's.
    stalled();
    expect(asked()).toBe(true);
    next();
    expect(asked()).toBe(false);
  });
});
