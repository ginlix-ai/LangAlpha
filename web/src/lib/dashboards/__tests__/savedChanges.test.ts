/**
 * The list's changes as pure functions of the latest value: each either
 * builds the next list or says why it leaves the list alone.
 */
import { describe, it, expect } from 'vitest';
import {
  addDashboard,
  isPageListed,
  MAX_DASHBOARDS,
  removeDashboard,
  restoreDashboard,
  selectDashboard,
  type SavedDashboard,
  type SavedDashboards,
} from '../saved';

const app = (code: string, title = code.toUpperCase()): SavedDashboard => ({
  kind: 'app', code, title, path: null, workspaceId: 'ws', threadId: null,
});
const page = (code: string): SavedDashboard => ({
  kind: 'page', code, title: code, path: 'p.html', workspaceId: 'ws', threadId: null, artifactId: `art-${code}`,
});
const codes = (saved: SavedDashboards) => saved.list.map((d) => d.code);
const full = (): SavedDashboards => ({ current: null, list: Array.from({ length: MAX_DASHBOARDS }, (_, i) => app(`a${i}`)) });

describe('addDashboard', () => {
  it('adds at the front and shows it', () => {
    const out = addDashboard({ current: null, list: [app('a')] }, app('b'));
    expect(out).toEqual({ next: { current: 'b', list: [app('b'), app('a')] } });
  });

  it('refuses a new one at the cap', () => {
    expect(addDashboard(full(), app('new'))).toEqual({ refuse: 'full' });
  });

  it('refreshes one already listed in place at the front, even at the cap', () => {
    const out = addDashboard(full(), app('a3', 'renamed'));
    if (!('next' in out)) throw new Error('refused');
    expect(out.next.list).toHaveLength(MAX_DASHBOARDS);
    expect(out.next.list[0]).toEqual(app('a3', 'renamed'));
    expect(out.next.current).toBe('a3');
    expect(codes(out.next).filter((c) => c === 'a3')).toHaveLength(1);
  });
});

describe('removeDashboard', () => {
  it('says what it took and from where', () => {
    const out = removeDashboard({ current: 'p', list: [app('a'), page('p'), app('b')] }, 'p');
    expect(out).toEqual({
      next: { current: null, list: [app('a'), app('b')] },
      removed: { gone: page('p'), index: 1, wasCurrent: true },
    });
  });

  it('refuses a code not on the list', () => {
    expect(removeDashboard({ current: null, list: [app('a')] }, 'x')).toEqual({ refuse: 'absent' });
  });
});

describe('restoreDashboard', () => {
  const removed = { gone: page('p'), index: 1, wasCurrent: true };

  it('puts it back where it was and shows it again', () => {
    expect(restoreDashboard({ current: null, list: [app('a'), app('b')] }, removed))
      .toEqual({ next: { current: 'p', list: [app('a'), page('p'), app('b')] } });
  });

  it('leaves another choice made since in place', () => {
    const out = restoreDashboard({ current: 'b', list: [app('a'), app('b')] }, removed);
    expect(out).toEqual({ next: { current: 'b', list: [app('a'), page('p'), app('b')] } });
  });

  it('puts it at the end when the list has shrunk past its place', () => {
    const out = restoreDashboard({ current: null, list: [] }, { ...removed, index: 5 });
    expect(out).toEqual({ next: { current: 'p', list: [page('p')] } });
  });

  it('refuses when it is back already (re-added since)', () => {
    expect(restoreDashboard({ current: null, list: [page('p')] }, removed)).toEqual({ refuse: 'listed' });
  });

  it('refuses at the cap', () => {
    expect(restoreDashboard(full(), removed)).toEqual({ refuse: 'full' });
  });
});

describe('selectDashboard', () => {
  it('selects a listed one or the built-in', () => {
    const saved = { current: 'a', list: [app('a'), app('b')] };
    expect(selectDashboard(saved, 'b')).toEqual({ next: { ...saved, current: 'b' } });
    expect(selectDashboard(saved, null)).toEqual({ next: { ...saved, current: null } });
  });

  it('refuses a code not on the list', () => {
    expect(selectDashboard({ current: null, list: [app('a')] }, 'gone')).toEqual({ refuse: 'absent' });
  });
});

describe('isPageListed', () => {
  it('matches a page by its artifact, never an app', () => {
    const saved = { current: null, list: [page('p'), app('art-a')] };
    expect(isPageListed(saved, 'art-p')).toBe(true);
    expect(isPageListed(saved, 'art-a')).toBe(false);
  });
});
