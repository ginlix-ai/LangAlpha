import { useCallback, useEffect, useRef, useState } from 'react';
import { useIsMutating, useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { usePreferences } from '@/hooks/usePreferences';
import { PREFERENCE_MUTATION_KEY } from '@/hooks/useUpdatePreferences';
import { readSubagentsDefault } from '@/lib/subagentsDefault';
import { userSessionStorage } from '@/lib/userStorage';
import { threadDetailQuery } from '../utils/threadQueries';
import { useThreadFieldSave } from './useThreadFieldSave';

/** Per thread, for the session: closing the offer answers it for that thread,
 *  wherever the thread is open next. Its running out answers nothing. */
const OFFER_CLOSED_PREFIX = 'thread-subagents-offer-closed:';

interface UseThreadSubagentsOptions {
  /** The view's thread; `__default__` until the first send creates one. */
  threadId: string | null | undefined;
  /**
   * The navigation whose first send creates the thread, with the pick that
   * send carries, which the new row does not hold until then. A new chat's
   * view stays cached while unsent and the next such navigation takes it up,
   * so a new `key` replaces whatever the view held with its pick.
   */
  landing?: { key: string; allowed: boolean | null } | null;
}

/**
 * Whether the agent may hand work to subagents on a chat view's thread: what
 * the composer's toggle shows, what a send carries, and the one place a flip
 * is saved.
 *
 * The thread row is the source. A row with no value of its own follows the
 * user's default, as the server reads it, so changing the default moves it.
 * The server decides whether a value is the thread's own or follows the
 * default, against the default as committed, so a flip and a send name the
 * side picked. A flip on a live thread is saved on the row
 * (`useThreadFieldSave`), which is the only writer once the thread exists, so
 * its sends carry nothing. Until its row is read the switch shows nothing
 * (`allowed` null): the server runs the row, whichever side it is on. A
 * thread not created yet shows the default too, and a flip is held for the
 * send that creates it. A flip made while that send is
 * creating the thread is saved on the row once it is read.
 *
 * A flip to the side the user's default is not on also raises the offer to
 * make that side the default, once the row holds the flip, shown while the
 * thread stays there.
 */
export function useThreadSubagents({ threadId, landing = null }: UseThreadSubagentsOptions) {
  const { t } = useTranslation();
  const { preferences, isLoaded: preferencesRead } = usePreferences();
  const liveThreadId = threadId && threadId !== '__default__' ? threadId : null;

  const { data: thread } = useQuery({
    ...threadDetailQuery(liveThreadId ?? ''),
    enabled: !!liveThreadId,
  });
  const fallback = readSubagentsDefault(preferences);
  // The default the user chose, once read, which the offer is measured
  // against. Before that the fallback is what no preference means, a default
  // nobody has seen.
  const userDefault = preferencesRead ? fallback : null;
  // A preference write moves the cached default before it lands, and a
  // refused one moves it back, the offer with it.
  const defaultWriting = useIsMutating({ mutationKey: PREFERENCE_MUTATION_KEY }) > 0;
  // Null while there is no row: none yet, or one not read (loading, failed).
  const rowValue = liveThreadId && thread ? thread.subagents_allowed ?? fallback : null;

  // A value the row does not hold yet, shown over it until it does.
  const [held, setHeld] = useState<boolean | null>(landing?.allowed ?? null);
  // A value belongs to the thread it was set on, and one instance can move
  // between threads. A thread being created by its first send is not a move.
  const [heldThreadId, setHeldThreadId] = useState(liveThreadId);
  // The flip whose value is offered as the default. A navigation's first send
  // carries a flip made on another page's composer, which offers the same.
  const [offered, setOffered] = useState<boolean | null>(landing?.allowed ?? null);
  const [offerClosed, setOfferClosed] = useState(false);
  const [landingKey, setLandingKey] = useState(landing?.key ?? null);
  if (heldThreadId !== liveThreadId) {
    setHeldThreadId(liveThreadId);
    if (heldThreadId) {
      setHeld(null);
      setOffered(null);
      setOfferClosed(false);
    }
  }
  if (landing && landing.key !== landingKey) {
    setLandingKey(landing.key);
    setHeld(landing.allowed);
    setOffered(landing.allowed);
    setOfferClosed(false);
  }
  if (held !== null && rowValue === held) setHeld(null);
  // The default on the offered side answers the offer, whether an accept put
  // it there or a change made elsewhere, so the default moving away again
  // later does not raise it anew.
  if (offered !== null && offered === userDefault && !defaultWriting) setOffered(null);

  const save = useThreadFieldSave({
    threadId: liveThreadId,
    field: 'subagents_allowed',
    failedMessage: t('chat.pills.subagentsFailed'),
    // With no row cached the flip lives in the hold alone, so it goes back
    // there, unless the toggle has moved on since.
    onFailed: ({ value, before }) => {
      setHeld((current) => (current === value ? before : current));
      setOffered(null);
    },
  });

  // A flip made while the first send is creating the thread had no row to be
  // saved on, and the thread's later sends carry nothing, so it is saved once
  // the new row is read and still differs. The pick the send carried agrees
  // with the row and is let go above, so it is never written twice: a second
  // write would be judged against the default as it is by then, which another
  // tab may have moved onto the pick.
  const shownBeforeRef = useRef(liveThreadId);
  const createdRef = useRef<string | null>(null);
  useEffect(() => {
    if (shownBeforeRef.current === null && liveThreadId) createdRef.current = liveThreadId;
    shownBeforeRef.current = liveThreadId;
    if (!liveThreadId || createdRef.current !== liveThreadId || rowValue === null) return;
    createdRef.current = null;
    if (held === null) return;
    void save({ threadId: liveThreadId, value: held, before: null });
  }, [held, liveThreadId, rowValue, save]);

  // Which thread the composer shows now, for a save that lands after a move.
  const shownThreadRef = useRef(liveThreadId);
  useEffect(() => {
    shownThreadRef.current = liveThreadId;
  }, [liveThreadId]);

  /** Resolves false only when this flip's own save failed. */
  const setAllowed = useCallback((next: boolean): Promise<boolean> => {
    const before = held;
    setHeld(next);
    const closedForThread = !!liveThreadId
      && userSessionStorage.getItem(OFFER_CLOSED_PREFIX + liveThreadId) !== null;
    const offerable = !offerClosed && !closedForThread;
    if (!liveThreadId) {
      if (offerable) setOffered(next);
      return Promise.resolve(true);
    }
    // On a live thread the offer waits for the row to hold the flip. The
    // server sorts a value against the default as committed, so a default
    // accepted before the flip lands would leave the thread following it,
    // and one accepted after would leave the value the thread's own.
    return save({ threadId: liveThreadId, value: next, before }).then((saved) => {
      if (saved && offerable && shownThreadRef.current === liveThreadId) setOffered(next);
      return saved;
    });
  }, [held, liveThreadId, offerClosed, save]);

  const closeOffer = useCallback(() => {
    setOfferClosed(true);
    setOffered(null);
    if (liveThreadId) userSessionStorage.setItem(OFFER_CLOSED_PREFIX + liveThreadId, '1');
  }, [liveThreadId]);
  const expireOffer = useCallback(() => setOffered(null), []);

  const allowed = held ?? (liveThreadId ? rowValue : fallback);
  // A new thread's send names a pick, and otherwise nothing, so the thread
  // follows the default. Until the default is read the switch shows what no
  // preference means, which a stored default may contradict, so the send
  // names what is shown; the server keeps nothing of its own when it agrees.
  const toSend = liveThreadId
    ? undefined
    : held ?? (userDefault === null ? fallback : undefined);
  const offer = userDefault !== null && offered !== null && offered === allowed && offered !== userDefault
    ? offered
    : null;

  return { allowed, toSend, setAllowed, offer, closeOffer, expireOffer };
}
