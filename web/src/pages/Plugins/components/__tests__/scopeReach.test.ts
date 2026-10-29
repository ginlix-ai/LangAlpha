import { describe, expect, it } from 'vitest';
import { readChecklist } from '../ScopeControl';

const scopeReach = (...args: Parameters<typeof readChecklist>) => readChecklist(...args).reach;

/**
 * The badge and a user server's state line both read this, so its branches
 * are the whole contract for what a row claims about where it is on. A row
 * that new workspaces start without must never read as all workspaces: that
 * wording promises the next workspace a server it will not have.
 */
const workspaces = [
  { id: 'ws-a', name: 'Alpha' },
  { id: 'ws-b', name: 'Beta' },
  { id: 'ws-c', name: 'Gamma' },
];
const home = { id: 'home', name: 'Home' };

describe('scopeReach on a row new workspaces start with', () => {
  it('reads all workspaces when none has switched it off', () => {
    expect(scopeReach(workspaces, [], undefined, true)).toEqual({ kind: 'all' });
  });

  it('counts only the live workspaces that switched it off', () => {
    // A disable left behind by a deleted workspace does not count.
    expect(scopeReach(workspaces, ['ws-b', 'ws-gone'], undefined, true)).toEqual({
      kind: 'allExcept',
      count: 1,
    });
  });

  it('reads the deny-list when the backend predates the setting', () => {
    // Absent is not false: every row inherited before the setting existed.
    expect(scopeReach(workspaces, ['ws-b'])).toEqual({ kind: 'allExcept', count: 1 });
  });

  it('counts Home only when the checklist offers it', () => {
    expect(scopeReach(workspaces, ['home'], home, true)).toEqual({
      kind: 'allExcept',
      count: 1,
    });
    expect(scopeReach(workspaces, ['home'], undefined, true)).toEqual({ kind: 'all' });
  });
});

describe('scopeReach on a row new workspaces start without', () => {
  it('names the one workspace it is on in', () => {
    expect(scopeReach(workspaces, ['ws-b', 'ws-c'], undefined, false)).toEqual({
      kind: 'only',
      name: 'Alpha',
    });
  });

  it('counts the workspaces it is on in, even when that is every one of them', () => {
    expect(scopeReach(workspaces, ['ws-c'], undefined, false)).toEqual({
      kind: 'some',
      count: 2,
    });
    expect(scopeReach(workspaces, [], undefined, false)).toEqual({ kind: 'some', count: 3 });
  });

  it('reads none when no workspace has it on', () => {
    expect(scopeReach(workspaces, ['ws-a', 'ws-b', 'ws-c'], undefined, false)).toEqual({
      kind: 'none',
    });
    expect(scopeReach([], [], undefined, false)).toEqual({ kind: 'none' });
  });

  it('ignores a disable for a deleted workspace', () => {
    expect(scopeReach(workspaces.slice(0, 2), ['ws-b', 'ws-gone'], undefined, false)).toEqual({
      kind: 'only',
      name: 'Alpha',
    });
  });

  it('counts Home as one of the workspaces it is on in', () => {
    expect(scopeReach(workspaces, ['ws-a', 'ws-b', 'ws-c'], home, false)).toEqual({
      kind: 'only',
      name: 'Home',
    });
    expect(scopeReach(workspaces, ['ws-b', 'ws-c'], home, false)).toEqual({
      kind: 'some',
      count: 2,
    });
    expect(scopeReach(workspaces, ['ws-a', 'ws-b', 'ws-c', 'home'], home, false)).toEqual({
      kind: 'none',
    });
  });
});
