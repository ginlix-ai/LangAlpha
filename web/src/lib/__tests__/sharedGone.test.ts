import { describe, expect, it } from 'vitest';
import { readSharedGone, sharedGoneNav } from '../sharedGone';

describe('sharedGone', () => {
  it('carries each reason through the navigation that leaves for the notice', () => {
    for (const reason of ['removed', 'deleted', 'thread', 'private'] as const) {
      const nav = sharedGoneNav(reason);
      expect(nav.replace).toBe(true);
      expect(readSharedGone(nav.state)).toBe(reason);
    }
  });

  it('reads nothing from any other arrival', () => {
    expect(readSharedGone(null)).toBeNull();
    expect(readSharedGone({ workspaceId: 'ws-1' })).toBeNull();
    expect(readSharedGone({ sharedGone: 'other' })).toBeNull();
  });
});
