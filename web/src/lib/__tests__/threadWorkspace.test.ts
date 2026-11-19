// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { threadWorkspaceFor } from '../threadWorkspace';

const SHARED = 'ws-shared';
const isShared = (id: string) => id === SHARED;

describe('threadWorkspaceFor', () => {
  it('keeps the named workspace until the thread is read, or when it is its own', () => {
    expect(threadWorkspaceFor('ws-a', null, isShared)).toBe('ws-a');
    expect(threadWorkspaceFor('ws-a', { workspaceId: 'ws-a', shared: false }, isShared)).toBe('ws-a');
    expect(threadWorkspaceFor(null, { workspaceId: 'ws-a', shared: false }, isShared)).toBe('ws-a');
  });

  it("opens a thread in its own workspace when the name crosses a shared workspace's line", () => {
    // A shared thread named under a personal workspace.
    expect(threadWorkspaceFor('ws-mine', { workspaceId: SHARED, shared: true }, isShared)).toBe(SHARED);
    // A personal thread named under a shared workspace.
    expect(threadWorkspaceFor(SHARED, { workspaceId: 'ws-mine', shared: false }, isShared)).toBe('ws-mine');
  });

  it('leaves a personal pair as named, as the server does (a Home thread on a former Home row)', () => {
    expect(threadWorkspaceFor('home-now', { workspaceId: 'home-before', shared: false }, isShared)).toBe('home-now');
  });
});
