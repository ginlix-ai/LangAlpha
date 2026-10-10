/**
 * Which turns a paged transcript holds, and whether it can page back further.
 *
 * A thread opens on its newest turns and older pages go above them only as the
 * reader scrolls up: nothing jumps to a turn that is not on screen (no URL
 * names a turn; the minimap, sources, files and tool links all point at loaded
 * bubbles), and a reload's place is a loaded bubble the reload's limit keeps.
 * The window holds the page the newest load or the last older page reported,
 * the one page in flight, and the token that refuses the landing of a page a
 * reload, a thread switch or a fork overtook.
 */
import { scrollMemory } from '@/lib/scrollMemory';
import type { HistoryCarry, LoadedHistoryPage } from './olderPages';

/**
 * Turns per replay page. A turn of a research thread can run to hundreds of
 * events, so this is about as much as replays in well under a second, and it
 * still fills a tall viewport several times over, which keeps the next page's
 * fetch ahead of a reader scrolling up.
 */
export const HISTORY_PAGE_TURNS = 20;

/**
 * Extra turns a reload asks for beyond the ones on screen. A reload asks for
 * the newest N turns, so turns that land between the status read it counts
 * from and the replay would otherwise push the oldest loaded turn off the top.
 * A return to a thread asks for none: its saved offset is measured from the
 * turn its transcript started at, so the page has to start there again.
 */
export const HISTORY_RELOAD_MARGIN_TURNS = 5;

/**
 * The limit for a replay of the newest page reaching back to turn `from`: one
 * page when nothing reaches further, otherwise every turn from `from` to the
 * newest the thread has, plus `margin`, so it never takes back what the
 * reader paged in.
 */
export function newestPageLimit(from: number | null, newestTurn: number, margin = 0): number {
  if (from === null) return HISTORY_PAGE_TURNS;
  return Math.max(HISTORY_PAGE_TURNS, newestTurn - from + 1 + margin);
}

/**
 * What the top of the transcript can do: `reloading` while the newest load
 * rebuilds it, `held` while a fork cuts it, `idle` with older turns to ask
 * for, `loading` while a page is on its way, `failed` after one did not
 * arrive, and `end` at the start of the thread or before anything loaded.
 */
export type OlderHistoryStatus = 'reloading' | 'held' | 'idle' | 'loading' | 'failed' | 'end';

/** An older page the window let start, and what it lands or fails with. */
export interface OlderPageRequest {
  beforeTurn: number;
  carry: HistoryCarry;
  signal: AbortSignal;
  token: number;
}

// Beside the scroll offset useChatScroll saves under `thread:<id>`: the turn
// the transcript started at, which that offset is measured from, so a return
// to the thread starts there again; and the reach, which the return keeps, so
// the margin a reload added above it is not taken as reach and asked again.
const startKey = (threadId: string) => `thread-start:${threadId}`;
const reachKey = (threadId: string) => `thread-reach:${threadId}`;

export function createHistoryWindow() {
  let threadId: string | null = null;
  let page: LoadedHistoryPage | null = null;
  // The oldest turn the reader paged in, which a reload reaches back to. The
  // reload's page can start up to the margin before it; taken as the reach,
  // that margin would be asked for again on top of itself at every reload.
  let reach: number | null = null;
  let base: Exclude<OlderHistoryStatus, 'held'> = 'end';
  let held = false;
  let token = 0;
  let inflight: AbortController | null = null;
  let status: OlderHistoryStatus = 'end';
  const listeners = new Set<() => void>();

  const publish = () => {
    const next = held && (base === 'idle' || base === 'failed') ? 'held' : base;
    if (next === status) return;
    status = next;
    for (const listener of listeners) listener();
  };
  // Stops the page in flight and refuses its landing.
  const overtake = () => {
    inflight?.abort();
    inflight = null;
    token += 1;
  };

  return {
    subscribe(listener: () => void): () => void {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    snapshot: (): OlderHistoryStatus => status,

    /** The limit for the newest load of `tid`: one page, or for a reload or a
     *  return to a thread left mid-way, every turn back to where it reached. */
    reloadLimit(tid: string, newestTurn: number): number {
      if (tid === threadId) return newestPageLimit(reach, newestTurn, HISTORY_RELOAD_MARGIN_TURNS);
      const start = scrollMemory.get(startKey(tid));
      const resumed = typeof start === 'number' && typeof scrollMemory.get(`thread:${tid}`) === 'number';
      return newestPageLimit(resumed ? start : null, newestTurn);
    },
    /** A newest load starts: no older page goes on a transcript it rebuilds. */
    beginReload(): number {
      overtake();
      base = 'reloading';
      publish();
      return token;
    },
    /** Where the transcript starts now, from the load or page `t` named. */
    land(t: number, tid: string, next: LoadedHistoryPage | null): void {
      if (t !== token) return;
      const saved = scrollMemory.get(reachKey(tid));
      const prior = tid !== threadId ? (typeof saved === 'number' ? saved : null)
        : base === 'reloading' ? reach
        : null;
      inflight = null;
      threadId = tid;
      page = next;
      // A reload of this thread, or a return to it, keeps the reach while its
      // page holds that turn. A branch cut short of it elsewhere starts the
      // reach again where the page starts.
      reach = prior !== null && next && next.firstTurnIndex <= prior && prior <= next.lastTurnIndex
        ? prior
        : (next?.firstTurnIndex ?? null);
      if (next && reach !== null) {
        scrollMemory.set(startKey(tid), next.firstTurnIndex);
        scrollMemory.set(reachKey(tid), reach);
      }
      base = next?.hasMore ? 'idle' : 'end';
      publish();
    },
    /** Starts the page before the oldest loaded one, or null when none may start. */
    request(tid: string): OlderPageRequest | null {
      if (held || tid !== threadId || !page?.hasMore) return null;
      if (base !== 'idle' && base !== 'failed') return null;
      overtake();
      inflight = new AbortController();
      base = 'loading';
      publish();
      return { beforeTurn: page.firstTurnIndex, carry: page.carry, signal: inflight.signal, token };
    },
    isCurrent: (t: number): boolean => t === token,
    fail(t: number): void {
      if (t !== token) return;
      inflight = null;
      base = 'failed';
      publish();
    },
    /** A fork cuts the transcript at an index, so nothing goes above the cut
     *  until it is made. */
    hold(): void {
      if (base === 'loading') {
        overtake();
        base = 'idle';
      }
      held = true;
      publish();
    },
    release(): void {
      held = false;
      publish();
    },
    /** The thread was left: its window, and its page in flight, go with it. */
    reset(): void {
      overtake();
      threadId = null;
      page = null;
      reach = null;
      base = 'end';
      held = false;
      publish();
    },
  };
}

export type HistoryWindow = ReturnType<typeof createHistoryWindow>;
