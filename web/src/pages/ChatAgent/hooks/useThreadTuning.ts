import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import type { ThreadTuning } from '@/components/ui/chat-input.types';
import type { Thread } from '@/types/api';
import { threadDetailQuery } from '../utils/threadQueries';
import { useThreadFieldSave } from './useThreadFieldSave';

type TuningField = 'reasoning_effort' | 'fast_mode';
type Override<F extends TuningField> = Exclude<Thread[F], undefined>;

/** A pick the row does not hold yet. Boxed, since null is a pick too: the
 *  thread going back to following Settings. */
type Hold<V> = { value: V } | null;

/** What a thread's first send stores as its own, beside the turn's level. */
export interface TuningToSend {
  reasoningEffortOverride?: string;
  fastModeOverride?: boolean;
}

interface UseThreadTuningOptions {
  /** The view's thread; `__default__` until the first send creates one. */
  threadId: string | null | undefined;
  /**
   * The navigation whose first send creates the thread, with the picks the
   * composer that sent it made, which the new row does not hold until then.
   * A new chat's view stays cached while unsent and the next such navigation
   * takes it up, so a new `key` replaces whatever the view held.
   */
  landing?: { key: string; reasoningEffort: string | null; fastMode: boolean | null } | null;
}

/**
 * A chat view's thread's own reasoning effort and fast mode: what its
 * composer shows over Settings, what the send creating the thread stores, and
 * the one place a pick on a live thread is saved.
 *
 * The thread row is the source, and null there follows Settings (the model's
 * profile, else the account default, else the model's own default), which
 * stay the only place a default is set: a pick here never writes them. A pick
 * on a live thread is saved on the row (`useThreadFieldSave`), which is the
 * only writer, since a send names the turn's level and the server never keeps
 * it. Until the row is read nothing is claimed (each field undefined): the
 * server runs the row's own when a send names none. A thread not created yet
 * holds its picks for the send that creates it, which stores them. A pick made
 * while that send is creating the thread, or one the new row does not hold
 * because the create failed and the legacy send minted the thread, is saved
 * on the row once it is read.
 */
export function useThreadTuning({ threadId, landing = null }: UseThreadTuningOptions) {
  const { t } = useTranslation();
  const liveThreadId = threadId && threadId !== '__default__' ? threadId : null;

  const { data: thread } = useQuery({
    ...threadDetailQuery(liveThreadId ?? ''),
    enabled: !!liveThreadId,
  });

  const effort = useThreadOverride({
    liveThreadId,
    thread,
    field: 'reasoning_effort',
    landing: landing && { key: landing.key, value: landing.reasoningEffort },
    failedMessage: t('chat.pills.effortFailed'),
  });
  const fast = useThreadOverride({
    liveThreadId,
    thread,
    field: 'fast_mode',
    landing: landing && { key: landing.key, value: landing.fastMode },
    failedMessage: t('chat.pills.fastModeFailed'),
  });

  const tuning = useMemo<ThreadTuning>(
    () => ({ reasoningEffort: effort.value, fastMode: fast.value }),
    [effort.value, fast.value],
  );
  const toSend = useMemo<TuningToSend>(() => ({
    ...(effort.toSend !== undefined ? { reasoningEffortOverride: effort.toSend } : {}),
    ...(fast.toSend !== undefined ? { fastModeOverride: fast.toSend } : {}),
  }), [effort.toSend, fast.toSend]);

  return { tuning, toSend, setReasoningEffort: effort.set, setFastMode: fast.set };
}

function useThreadOverride<F extends TuningField>({
  liveThreadId,
  thread,
  field,
  landing,
  failedMessage,
}: {
  liveThreadId: string | null;
  thread: Thread | undefined;
  field: F;
  landing: { key: string; value: Override<F> } | null;
  failedMessage: string;
}) {
  // Undefined while there is no row: none yet, or one not read (loading, failed).
  const rowValue: Override<F> | undefined = liveThreadId && thread
    ? (thread[field] ?? null) as Override<F>
    : undefined;

  const landingHold = (value: Override<F>): Hold<Override<F>> => (value === null ? null : { value });
  const [held, setHeld] = useState<Hold<Override<F>>>(landing ? landingHold(landing.value) : null);
  // A pick belongs to the thread it was made on, and one instance can move
  // between threads. A thread being created by its first send is not a move.
  const [heldThreadId, setHeldThreadId] = useState(liveThreadId);
  const [landingKey, setLandingKey] = useState(landing?.key ?? null);
  if (heldThreadId !== liveThreadId) {
    setHeldThreadId(liveThreadId);
    if (heldThreadId) setHeld(null);
  }
  if (landing && landing.key !== landingKey) {
    setLandingKey(landing.key);
    setHeld(landingHold(landing.value));
  }
  if (held && rowValue !== undefined && rowValue === held.value) setHeld(null);

  const save = useThreadFieldSave({
    threadId: liveThreadId,
    field,
    failedMessage,
    // The row moving back carries the composer with it, so the hold goes and
    // the row shows through, unless the pick has moved on since. With no row
    // cached the composer falls back to Settings until the settle reads it.
    onFailed: ({ value }) => {
      setHeld((current) => (current && current.value === value ? null : current));
    },
  });

  // A pick made while the first send is creating the thread had no row to be
  // saved on, and a thread the legacy send minted (its create failed) stored
  // none, since a send's level is the turn's alone. Either way the new row,
  // once read, does not hold the pick, which is then saved there. A pick the
  // create stored agrees with the row and is let go above, so it is never
  // written twice.
  const shownBeforeRef = useRef(liveThreadId);
  const createdRef = useRef<string | null>(null);
  useEffect(() => {
    if (shownBeforeRef.current === null && liveThreadId) createdRef.current = liveThreadId;
    shownBeforeRef.current = liveThreadId;
    if (!liveThreadId || createdRef.current !== liveThreadId || rowValue === undefined) return;
    createdRef.current = null;
    if (held === null) return;
    void save({ threadId: liveThreadId, value: held.value, before: null });
  }, [held, liveThreadId, rowValue, save]);

  /** Resolves false only when this pick's own save failed. */
  const set = useCallback((next: Override<F>): Promise<boolean> => {
    setHeld({ value: next });
    if (!liveThreadId) return Promise.resolve(true);
    // `before` goes unused: a failure drops the hold rather than restoring one.
    return save({ threadId: liveThreadId, value: next, before: null });
  }, [liveThreadId, save]);

  const value: Override<F> | undefined = held ? held.value : liveThreadId ? rowValue : null as Override<F>;
  // A new thread's create names a pick, and otherwise nothing, so the thread
  // follows Settings.
  const toSend = !liveThreadId && held && held.value !== null
    ? held.value as Exclude<Override<F>, null>
    : undefined;

  return { value, toSend, set };
}
