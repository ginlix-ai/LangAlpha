import { describe, it, expect } from 'vitest';
import { clearDenyPlan, onlyInPlan } from '../utils/scopeTargets';

const LIVE = ['a', 'b', 'c'];

describe('onlyInPlan', () => {
  it('is empty when the row already matches the chosen set', () => {
    expect(onlyInPlan(['b', 'c'], LIVE, new Set(['a']))).toEqual([]);
  });

  it('enables chosen denied workspaces and disables unchosen active ones', () => {
    expect(onlyInPlan(['a'], LIVE, new Set(['a']))).toEqual([
      { workspaceId: 'a', enabled: true },
      { workspaceId: 'b', enabled: false },
      { workspaceId: 'c', enabled: false },
    ]);
  });

  it('touches only the workspaces whose state differs', () => {
    // b already denied and unchosen; only a (deny) and c (allow) flip.
    expect(onlyInPlan(['b', 'c'], LIVE, new Set(['c']))).toEqual([
      { workspaceId: 'a', enabled: false },
      { workspaceId: 'c', enabled: true },
    ]);
  });

  it('ignores stale deny ids that name no live workspace', () => {
    expect(onlyInPlan(['ghost'], LIVE, new Set(LIVE))).toEqual([]);
  });

  it('treats an undefined deny list as active everywhere', () => {
    expect(onlyInPlan(undefined, LIVE, new Set(['b']))).toEqual([
      { workspaceId: 'a', enabled: false },
      { workspaceId: 'c', enabled: false },
    ]);
  });
});

describe('clearDenyPlan', () => {
  it('re-enables every live denied workspace and nothing else', () => {
    expect(clearDenyPlan(['b', 'ghost'], LIVE)).toEqual([
      { workspaceId: 'b', enabled: true },
    ]);
  });

  it('is empty for a row with no denies', () => {
    expect(clearDenyPlan([], LIVE)).toEqual([]);
    expect(clearDenyPlan(undefined, LIVE)).toEqual([]);
  });
});
