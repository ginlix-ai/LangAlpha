// @vitest-environment node
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
    expect(shouldLeaveThreadRoute(false, new Error('Thread ID is required'), false, false)).toBe(false);
  });

  it('leaves the route when the lookup we asked for actually failed', () => {
    expect(shouldLeaveThreadRoute(true, new Error('not found'), false, false)).toBe(true);
  });

  it('stays put on 403 so the access-denied surface can render', () => {
    expect(shouldLeaveThreadRoute(true, new Error('forbidden'), true, false)).toBe(false);
  });

  it('stays put when the lookup succeeded', () => {
    expect(shouldLeaveThreadRoute(true, null, false, true)).toBe(false);
  });

  it('stays put when a refetch of a thread already read fails', () => {
    const blip = Object.assign(new Error('Network Error'), { response: undefined });
    expect(shouldLeaveThreadRoute(true, blip, false, true)).toBe(false);
  });

  it('leaves once a thread already read is gone', () => {
    const gone = Object.assign(new Error('not found'), { response: { status: 404 } });
    expect(shouldLeaveThreadRoute(true, gone, false, true)).toBe(true);
  });
});
