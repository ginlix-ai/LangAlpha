import { useCallback, useEffect, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { resolveComposerModel } from '@/components/ui/chat-input.models';
import { useAllModels } from '@/hooks/useAllModels';
import { useModeDefaultModel } from '@/hooks/useModeDefaultModel';
import { useSeededModel } from '@/hooks/useSeededModel';
import type { ComposerMode } from '@/lib/modelPreferences';
import { queryKeys } from '@/lib/queryKeys';
import { userSessionStorage } from '@/lib/userStorage';
import type { Thread } from '@/types/api';
import { updateThread } from '../utils/api';
import { threadDetailQuery } from '../utils/threadQueries';

/** Per thread, for the session: one dismissal answers the offer for that
 *  thread, wherever the thread is open next. */
const OFFER_DISMISSED_PREFIX = 'thread-model-offer-dismissed:';

/**
 * One mutation identity per thread, so a settle and the turn-end refetch can
 * ask whether a pick on that thread is still in flight. Built here rather
 * than in `queryKeys`, which holds query keys only: mutation keys live in
 * their own cache, which query prefix invalidation never reaches.
 */
export function threadModelMutationKey(threadId: string) {
  return ['thread-model', threadId] as const;
}

/** A thread model that left the catalog, and the default now standing in. */
export interface RetiredModel {
  model: string;
  fallback: string;
}

/** A pick the user may make the account default for the mode. */
export interface DefaultModelOffer {
  model: string;
  defaultModel: string;
}

interface ThreadModelPick {
  threadId: string;
  model: string;
  /** What the composer showed before this pick, restored if it fails. */
  previous: string | null;
}

interface UseThreadModelOptions {
  /** The view's thread; `__default__` until the first send creates one. */
  threadId: string | null | undefined;
  mode: ComposerMode;
  /** Whether a turn is running. Its end is when the send's model reaches the row. */
  isLoading: boolean;
  /** The model a navigation's first message went out with, which the thread
   *  row does not carry until that send has stored it. */
  initialModel?: string | null;
}

/**
 * The model a chat view's thread runs on: what its composer shows, what the
 * next send names, and the one place a pick on it is saved.
 *
 * The thread row is the source. The model starts on the row's model while it
 * is reachable (else the mode default) and follows the row when it moves; a
 * pick holds in between and PATCHes the row, moving the cached detail first
 * so every view of the thread agrees before the server answers. A thread not
 * created yet has no row to write, so its pick is held here and goes out with
 * the first send, which stores it.
 *
 * A pick also raises the offer to make that model the account default, shown
 * while the composer still shows the pick and it differs from the default.
 */
export function useThreadModel({
  threadId,
  mode,
  isLoading,
  initialModel = null,
}: UseThreadModelOptions) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { catalogModelNames } = useAllModels();
  const defaultModel = useModeDefaultModel(mode);
  const liveThreadId = threadId && threadId !== '__default__' ? threadId : null;

  const { data: thread } = useQuery({
    ...threadDetailQuery(liveThreadId ?? ''),
    enabled: !!liveThreadId,
  });
  // A thread whose row has not been read (still loading, or the read failed)
  // seeds no model rather than the default: the composer names what it shows,
  // and the send would store the default over the model the thread holds.
  // With no model named, the server runs the thread's own.
  const unread = !!liveThreadId && !thread && !initialModel;
  const { seed, retired: retiredName } = unread
    ? { seed: null, retired: null }
    : resolveComposerModel(
      thread?.llm_model || initialModel || null,
      defaultModel,
      catalogModelNames,
    );
  const [model, setModel] = useSeededModel(seed);

  // A thread started from another page's composer opens with the pick made
  // there. That composer seeds from the default, so a model other than the
  // default is a pick like one made here; the gate below drops the default.
  const [offered, setOffered] = useState<string | null>(initialModel);
  const [dismissed, setDismissed] = useState(false);

  // One instance can move between threads (the market panel keeps its
  // composer across them), and a pick and its offer belong to the thread they
  // were made on. The seed alone cannot carry a move: two threads whose rows
  // are both unread seed the same null, and the pick would ride to the next
  // send there. A thread being created by its first send is not a move.
  const [heldThreadId, setHeldThreadId] = useState(liveThreadId);
  if (heldThreadId !== liveThreadId) {
    setHeldThreadId(liveThreadId);
    if (heldThreadId) {
      setModel(seed);
      setOffered(null);
      setDismissed(false);
    }
  }
  // Which thread the composer shows now, for a save that fails after a move.
  const shownThreadRef = useRef(liveThreadId);
  useEffect(() => {
    shownThreadRef.current = liveThreadId;
  }, [liveThreadId]);

  // What the server is known to hold per thread while picks on it are in
  // flight: the row before the first of them, then each one it accepts. A
  // failure restores this, not its own snapshot, which may be the optimistic
  // write of an earlier pick that failed too.
  const confirmedRef = useRef(new Map<string, string | null>());

  const { mutateAsync: saveModel } = useMutation({
    mutationKey: threadModelMutationKey(liveThreadId ?? ''),
    // PATCHes on one thread reach the server in the order they were picked.
    // Two in flight at once could otherwise land in either order, and the
    // row would keep the older pick while the composer shows the newer one.
    scope: { id: `thread-model:${liveThreadId ?? ''}` },
    mutationFn: ({ threadId: id, model: picked }: ThreadModelPick) => updateThread(id, { llm_model: picked }),
    onMutate: async ({ threadId: id, model: picked }: ThreadModelPick) => {
      const key = queryKeys.threads.detail(id);
      // Read before the await, so a second pick cannot slip in between the
      // count and the snapshot. This pick already counts, so 1 means no other
      // pick on the thread is in flight and the cached row is the server's.
      const previous = queryClient.getQueryData<Thread>(key)?.llm_model ?? null;
      if (queryClient.isMutating({ mutationKey: threadModelMutationKey(id) }) === 1) {
        confirmedRef.current.set(id, previous);
      }
      await queryClient.cancelQueries({ queryKey: key });
      queryClient.setQueryData<Thread>(key, (current) => (current ? { ...current, llm_model: picked } : current));
      return { previous };
    },
    onSuccess: (saved, { threadId: id, model: picked }) => {
      confirmedRef.current.set(id, saved?.llm_model ?? picked);
    },
    onError: (_error, { threadId: id, model: picked, previous }, context) => {
      // Only the last pick in flight speaks for the thread: an earlier one
      // failing behind a newer pick would restore a model the user has
      // already moved off. Inside onError this pick still counts, so 1 means
      // it is the last.
      if (queryClient.isMutating({ mutationKey: threadModelMutationKey(id) }) !== 1) return;
      // Only the model field rolls back; anything else a refetch brought in
      // since the optimistic write is newer than the snapshot. This pick's
      // own snapshot stands in only when the chain began in another mount.
      const confirmed = confirmedRef.current;
      const restored = confirmed.has(id) ? confirmed.get(id) ?? null : context?.previous ?? null;
      queryClient.setQueryData<Thread>(queryKeys.threads.detail(id), (current) => (
        current ? { ...current, llm_model: restored } : current
      ));
      // The row moving back carries the composer with it. With no row cached
      // there is nothing to move, so the held pick is put back directly,
      // unless the model has moved on since for another reason, the composer
      // having moved to another thread among them.
      if (shownThreadRef.current === id) {
        setModel((current) => (current === picked ? previous : current));
        setOffered(null);
      }
      toast({ description: t('chat.threadModel.pickFailed'), variant: 'destructive' });
    },
    onSettled: (saved, error, { threadId: id, model: picked }) => {
      if (queryClient.isMutating({ mutationKey: threadModelMutationKey(id) }) !== 1) return;
      confirmedRef.current.delete(id);
      const key = queryKeys.threads.detail(id);
      // A failure is read back: the save may have landed with its response
      // lost, and the rollback could only guess at what the server holds.
      // With no row cached (the cancel in onMutate can take a first load with
      // it) there is nothing to patch, so that row is read afresh too.
      if (error || !queryClient.getQueryData<Thread>(key)) {
        void queryClient.invalidateQueries({ queryKey: key });
        return;
      }
      const stored = saved?.llm_model ?? picked;
      queryClient.setQueryData<Thread>(key, (current) => (current ? { ...current, llm_model: stored } : current));
    },
  });

  /** Resolves false only when this pick's own save failed. */
  const pickModel = useCallback((picked: string): Promise<boolean> => {
    const previous = model;
    setModel(picked);
    const dismissedForThread = !!liveThreadId
      && userSessionStorage.getItem(OFFER_DISMISSED_PREFIX + liveThreadId) !== null;
    if (!dismissed && !dismissedForThread) setOffered(picked);
    if (!liveThreadId) return Promise.resolve(true);
    return saveModel({ threadId: liveThreadId, model: picked, previous }).then(() => true, () => false);
  }, [model, setModel, liveThreadId, dismissed, saveModel]);

  // A send stores the model it named on the row, so the cached detail is
  // behind once a turn ends. That includes a turn refused as `model_removed`:
  // the server cleared the dead model from the row, and the fresh copy is
  // what moves the composer to the default and says why. Skipped while a pick
  // is saving: that pick's own answer is newer than whatever the refetch
  // would read.
  const wasLoadingRef = useRef(isLoading);
  useEffect(() => {
    const ended = wasLoadingRef.current && !isLoading;
    wasLoadingRef.current = isLoading;
    if (!ended || !liveThreadId) return;
    if (queryClient.isMutating({ mutationKey: threadModelMutationKey(liveThreadId) }) > 0) return;
    void queryClient.invalidateQueries({ queryKey: queryKeys.threads.detail(liveThreadId) });
  }, [isLoading, liveThreadId, queryClient]);

  const dismissOffer = useCallback(() => {
    setDismissed(true);
    setOffered(null);
    if (liveThreadId) userSessionStorage.setItem(OFFER_DISMISSED_PREFIX + liveThreadId, '1');
  }, [liveThreadId]);

  // Named only once there is a default to name beside it, and never when the
  // default is the retired model too: the row would promise a move that the
  // composer cannot make.
  const retired: RetiredModel | null = retiredName && defaultModel && retiredName !== defaultModel
    ? { model: retiredName, fallback: defaultModel }
    : null;
  const offer: DefaultModelOffer | null = offered && offered === model && defaultModel && offered !== defaultModel
    ? { model: offered, defaultModel }
    : null;

  return { model, retired, offer, pickModel, dismissOffer };
}
