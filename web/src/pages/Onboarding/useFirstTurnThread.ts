import { useEffect, useState } from 'react';
import { userSessionStorage } from '@/lib/userStorage';

/** The thread route a new chat opens on, until its first message names it. */
const NEW_THREAD = '__default__';

// The tab's notes of the threads tracked below, so a reload in the middle of
// a first turn does not bring the guidance up over it.
const FIRST_TURN_KEY = 'onboarding.firstTurnThread';
const WELCOME_KEY = 'onboarding.welcomeThread';

interface Tracked {
  /** Set while a new chat is open: whether the welcome flow's hand-off opened it. */
  opening: { welcome: boolean } | null;
  /** The thread the last new chat became. */
  firstTurn: string | null;
  /** The thread the welcome flow's hand-off became. */
  welcome: string | null;
}

function isWelcomeHandoff(state: unknown): boolean {
  return (state as { handoff?: unknown } | null)?.handoff === true;
}

function remember(key: string, threadId: string | null): void {
  if (threadId) userSessionStorage.setItem(key, threadId);
  else userSessionStorage.removeItem(key);
}

/**
 * Whether the route shows a new chat's first turn, and whether that chat is
 * the welcome flow's conversation.
 *
 * Every new chat opens on `/chat/t/__default__`, and its first message renames
 * the route to the real thread id with a navigate that drops the route state.
 * So the first route after `__default__` is taken as the thread it became, and
 * whether the welcome flow's hand-off opened it (`state.handoff`) is carried
 * across from the visit that saw the state. A different thread opened later
 * reads as an ordinary one.
 */
export function useFirstTurnThread(
  pathname: string,
  state: unknown
): { firstTurn: boolean; welcome: boolean } {
  const threadId = pathname.match(/^\/chat\/t\/([^/]+)/)?.[1] ?? null;
  const [tracked, setTracked] = useState<Tracked>(() => ({
    opening: null,
    firstTurn: userSessionStorage.getItem(FIRST_TURN_KEY),
    welcome: userSessionStorage.getItem(WELCOME_KEY),
  }));

  // Adjusted during render, so the route that renames the chat is read as its
  // first turn on its first render.
  let next = tracked;
  if (threadId === NEW_THREAD) {
    // The chat clears its route state once it has read it, so the hand-off is
    // held for as long as the chat stays new.
    const welcome = tracked.opening?.welcome === true || isWelcomeHandoff(state);
    if (tracked.opening?.welcome !== welcome) next = { ...tracked, opening: { welcome } };
  } else if (tracked.opening) {
    // The rename lands on the real thread id; null when the user left the
    // thread view without sending a message.
    next = {
      opening: null,
      firstTurn: threadId,
      welcome: tracked.opening.welcome && threadId ? threadId : tracked.welcome,
    };
  }
  if (next !== tracked) setTracked(next);

  useEffect(() => {
    remember(FIRST_TURN_KEY, tracked.firstTurn);
  }, [tracked.firstTurn]);
  useEffect(() => {
    remember(WELCOME_KEY, tracked.welcome);
  }, [tracked.welcome]);

  if (threadId === NEW_THREAD) {
    return { firstTurn: true, welcome: next.opening?.welcome === true };
  }
  return {
    firstTurn: threadId !== null && threadId === next.firstTurn,
    welcome: threadId !== null && threadId === next.welcome,
  };
}
