/**
 * The window over a paged transcript: which turns it holds, the one older page
 * in flight, and the token that refuses a page something else overtook.
 */
import { describe, it, expect, afterEach } from 'vitest';
import { scrollMemory } from '@/lib/scrollMemory';
import { runAuthResets } from '@/lib/authResets';
import {
  HISTORY_PAGE_TURNS,
  HISTORY_RELOAD_MARGIN_TURNS,
  createHistoryWindow,
  newestPageLimit,
} from '../historyWindow';
import type { LoadedHistoryPage } from '../olderPages';

const pageFrom = (firstTurnIndex: number, hasMore = true, lastTurnIndex = 80): LoadedHistoryPage => ({
  firstTurnIndex,
  lastTurnIndex,
  hasMore,
  carry: { events: [], subagents: new Map(), steeredAgentIds: new Set() },
});

/** A window that loaded the newest page of thread-1, starting at `first`. */
function opened(first = 61) {
  const window = createHistoryWindow();
  window.land(window.beginReload(), 'thread-1', pageFrom(first));
  return window;
}

describe('newestPageLimit', () => {
  it('asks for one page on a first open', () => {
    expect(newestPageLimit(null, 80)).toBe(HISTORY_PAGE_TURNS);
  });

  it('covers every turn back to where it reaches, plus the margin it is given', () => {
    // Turns 40..80 on screen: 41 turns, and the margin for turns landing
    // between the status read and the replay.
    expect(newestPageLimit(40, 80, HISTORY_RELOAD_MARGIN_TURNS)).toBe(41 + HISTORY_RELOAD_MARGIN_TURNS);
    expect(newestPageLimit(40, 80)).toBe(41);
  });

  it('never asks for less than a page', () => {
    expect(newestPageLimit(78, 80, HISTORY_RELOAD_MARGIN_TURNS)).toBe(HISTORY_PAGE_TURNS);
    // A fork can leave the newest turn behind the oldest loaded one.
    expect(newestPageLimit(50, 10, HISTORY_RELOAD_MARGIN_TURNS)).toBe(HISTORY_PAGE_TURNS);
  });
});

describe('createHistoryWindow', () => {
  afterEach(() => scrollMemory.clear());

  it('has nothing to page back from before the first load lands', () => {
    const window = createHistoryWindow();
    expect(window.snapshot()).toBe('end');
    window.beginReload();
    expect(window.snapshot()).toBe('reloading');
    expect(window.request('thread-1')).toBeNull();
  });

  it('starts the page before the oldest loaded turn, one at a time', () => {
    const window = opened();
    expect(window.snapshot()).toBe('idle');
    const request = window.request('thread-1');
    expect(request).toMatchObject({ beforeTurn: 61 });
    expect(window.snapshot()).toBe('loading');
    expect(window.request('thread-1')).toBeNull();

    window.land(request!.token, 'thread-1', pageFrom(0, false));
    expect(window.snapshot()).toBe('end');
  });

  it('refuses the landing of a page a reload overtook, and stops its fetch', () => {
    const window = opened();
    const request = window.request('thread-1')!;
    const reload = window.beginReload();
    expect(request.signal.aborted).toBe(true);
    expect(window.isCurrent(request.token)).toBe(false);

    window.land(request.token, 'thread-1', pageFrom(41));
    window.fail(request.token);
    expect(window.snapshot()).toBe('reloading');
    window.land(reload, 'thread-1', pageFrom(61));
    expect(window.request('thread-1')).toMatchObject({ beforeTurn: 61 });
  });

  it('refuses a page for another thread', () => {
    expect(opened().request('thread-2')).toBeNull();
  });

  it('holds the top while a fork cuts, dropping the page on its way', () => {
    const window = opened();
    const request = window.request('thread-1')!;
    window.hold();
    expect(request.signal.aborted).toBe(true);
    expect(window.snapshot()).toBe('held');
    expect(window.request('thread-1')).toBeNull();

    window.release();
    expect(window.snapshot()).toBe('idle');
    expect(window.request('thread-1')).toMatchObject({ beforeTurn: 61 });
  });

  it('offers a retry after a page failed', () => {
    const window = opened();
    window.fail(window.request('thread-1')!.token);
    expect(window.snapshot()).toBe('failed');
    expect(window.request('thread-1')).toMatchObject({ beforeTurn: 61 });
  });

  it('tells subscribers when the status changes, and only then', () => {
    const window = opened();
    let calls = 0;
    const unsubscribe = window.subscribe(() => {
      calls += 1;
    });
    window.release();
    expect(calls).toBe(0);
    window.request('thread-1');
    expect(calls).toBe(1);
    unsubscribe();
    window.reset();
    expect(calls).toBe(1);
  });

  it('reaches back on a return to a thread left mid-way, and only then', () => {
    opened(41);
    const fresh = createHistoryWindow();
    // Left at the bottom, or never scrolled: the newest page is enough.
    expect(fresh.reloadLimit('thread-1', 80)).toBe(HISTORY_PAGE_TURNS);
    scrollMemory.set('thread:thread-1', 'bottom');
    expect(fresh.reloadLimit('thread-1', 80)).toBe(HISTORY_PAGE_TURNS);
    // Left mid-way: the offset is measured from turn 41 down, so the page
    // starts there again, with no margin above it.
    scrollMemory.set('thread:thread-1', 1200);
    expect(fresh.reloadLimit('thread-1', 80)).toBe(80 - 41 + 1);
    // A thread the window holds reloads what it holds.
    expect(opened(70).reloadLimit('thread-1', 80)).toBe(HISTORY_PAGE_TURNS);
  });

  it('reaches back to the oldest turn paged in at every reload, with one margin', () => {
    const window = opened(41);
    const limit = window.reloadLimit('thread-1', 80);
    expect(limit).toBe(80 - 41 + 1 + HISTORY_RELOAD_MARGIN_TURNS);
    // The reload's page starts the margin above turn 41, and older pages go
    // on above that; the next reload still reaches back only to 41.
    window.land(window.beginReload(), 'thread-1', pageFrom(80 - limit + 1));
    expect(window.reloadLimit('thread-1', 80)).toBe(limit);
    const request = window.request('thread-1')!;
    expect(request).toMatchObject({ beforeTurn: 41 - HISTORY_RELOAD_MARGIN_TURNS });

    // An older page moves the reach to where it starts.
    window.land(request.token, 'thread-1', pageFrom(16));
    expect(window.reloadLimit('thread-1', 80)).toBe(80 - 16 + 1 + HISTORY_RELOAD_MARGIN_TURNS);
    // So does a reload whose page starts after it, as when more turns
    // landed than the margin covers.
    window.land(window.beginReload(), 'thread-1', pageFrom(20));
    expect(window.reloadLimit('thread-1', 80)).toBe(80 - 20 + 1 + HISTORY_RELOAD_MARGIN_TURNS);
  });

  it("keeps the reach, not a reload's margin, across a return to the thread", () => {
    const window = opened(41);
    const limit = window.reloadLimit('thread-1', 80);
    window.land(window.beginReload(), 'thread-1', pageFrom(80 - limit + 1));
    // Left mid-way, its transcript starting the margin above turn 41.
    window.reset();
    scrollMemory.set('thread:thread-1', 1200);
    // The return starts the page where the saved offset is measured from,
    const back = window.reloadLimit('thread-1', 80);
    expect(back).toBe(limit);
    window.land(window.beginReload(), 'thread-1', pageFrom(80 - back + 1));
    // and its next reload still reaches back only to turn 41.
    expect(window.reloadLimit('thread-1', 80)).toBe(limit);
  });

  it('starts the reach again where a reload starts when the branch now ends before it', () => {
    const window = opened(41);
    // Forked elsewhere at turn 10: the reload holds turns 0..10, and turn 41
    // is gone.
    window.land(window.beginReload(), 'thread-1', pageFrom(0, false, 10));
    // Grown to turn 20 since, the next reload still holds turn 0.
    expect(window.reloadLimit('thread-1', 20)).toBe(20 - 0 + 1 + HISTORY_RELOAD_MARGIN_TURNS);
  });

  it('forgets the reach on sign-out, as it does the offsets', () => {
    opened(41);
    runAuthResets();
    // Another account's offset for the same id reaches back to nothing.
    scrollMemory.set('thread:thread-1', 1200);
    expect(createHistoryWindow().reloadLimit('thread-1', 80)).toBe(HISTORY_PAGE_TURNS);
  });
});
