/**
 * The gate on ChatAgent's redirect-out-of-a-thread-route effect.
 *
 * Regression: a disabled `threads.detail` query can hold an error this route
 * never asked for — the lookup only runs when the workspace id can't be read
 * off the URL or the navigation state. The redirect effect re-runs on every
 * navigation, so an ungated read of that error replaced every push into
 * /chat/* with a bounce back to the workspace gallery, making workspace cards,
 * thread rows and new-thread all look dead.
 */
import { describe, it, expect } from 'vitest';
import { shouldLeaveThreadRoute } from '../threadRouteGuard';

describe('shouldLeaveThreadRoute', () => {
  it('ignores an error parked on a lookup this route never requested', () => {
    expect(shouldLeaveThreadRoute(false, new Error('Thread ID is required'), false)).toBe(false);
  });

  it('leaves the route when the lookup we asked for actually failed', () => {
    expect(shouldLeaveThreadRoute(true, new Error('not found'), false)).toBe(true);
  });

  it('stays put on 403 so the access-denied surface can render', () => {
    expect(shouldLeaveThreadRoute(true, new Error('forbidden'), true)).toBe(false);
  });

  it('stays put when the lookup succeeded', () => {
    expect(shouldLeaveThreadRoute(true, null, false)).toBe(false);
  });
});
