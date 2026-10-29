import { useCallback, useEffect, useRef, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { resolveComposerModel } from '@/components/ui/chat-input.models';
import type { SubscriptionProvider } from '@/api/oauth';
import { useAllModels } from '@/hooks/useAllModels';
import { useLapsedSignIn } from '@/hooks/useLapsedSignIn';
import { useDefaultModel } from '@/hooks/useSlotDefaultModels';
import { useSeededModel } from '@/hooks/useSeededModel';
import { modelLabel } from '@/lib/modelLabel';
import { queryKeys } from '@/lib/queryKeys';
import { threadFieldMutationKey } from '@/lib/threadFieldMutations';
import { userSessionStorage } from '@/lib/userStorage';
import { threadDetailQuery } from '../utils/threadQueries';
import { useThreadFieldSave } from './useThreadFieldSave';

/** Per thread, for the session: one dismissal answers the offer for that
 *  thread, wherever the thread is open next. */
const OFFER_DISMISSED_PREFIX = 'thread-model-offer-dismissed:';

/** A thread model that left the catalog, and the default now standing in. */
export interface RetiredModel {
  model: string;
  fallback: string;
}

/** The composer's model, on a subscription whose sign-in has to be renewed. */
export interface LapsedSignIn {
  model: string;
  provider: SubscriptionProvider;
}

/** A pick the user may make the account default. */
export interface DefaultModelOffer {
  model: string;
  defaultModel: string;
}

interface UseThreadModelOptions {
  /** The view's thread; `__default__` until the first send creates one. */
  threadId: string | null | undefined;
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
 * is reachable (else the account default) and follows the row when it moves; a
 * pick holds in between and is saved on the row (`useThreadFieldSave`). A
 * thread not created yet has no row to write, so its pick is held here and
 * goes out with the first send, which stores it.
 *
 * A pick also raises the offer to make that model the account default, shown
 * while the composer still shows the pick and it differs from the default.
 */
export function useThreadModel({
  threadId,
  isLoading,
  initialModel = null,
}: UseThreadModelOptions) {
  const { t } = useTranslation();
  const queryClient = useQueryClient();
  const { catalogModelNames, metadata } = useAllModels();
  const defaultModel = useDefaultModel();
  const lapsedOf = useLapsedSignIn();
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

  const saveModel = useThreadFieldSave({
    threadId: liveThreadId,
    field: 'llm_model',
    failedMessage: t('chat.threadModel.pickFailed'),
    // The row moving back carries the composer with it. With no row cached
    // there is nothing to move, so the held pick is put back directly, unless
    // the model has moved on since for another reason.
    onFailed: ({ value, before }) => {
      setModel((current) => (current === value ? before : current));
      setOffered(null);
    },
    // A switch away from a model whose provider compacted the thread into a
    // summary only it can read: that model writes it out for the new one
    // first, which the next send waits for.
    onSaved: (saved) => {
      const to = saved.llm_model || defaultModel;
      if (!saved.summary_handoff || !to) return;
      toast({ description: t('chat.threadModel.handoff', { model: modelLabel(to, metadata) }) });
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
    return saveModel({ threadId: liveThreadId, value: picked, before: previous });
  }, [model, setModel, liveThreadId, dismissed, saveModel]);

  // A send stores the model it named on the row, so the cached detail is
  // behind once a turn ends. That includes a turn refused as `model_removed`:
  // the server cleared the dead model from the row, and the fresh copy is
  // what moves the composer to the default and says why. While any field on
  // the thread is saving, a read now would land over that save, so the row is
  // only marked stale and the save's settle reads it.
  const wasLoadingRef = useRef(isLoading);
  useEffect(() => {
    const ended = wasLoadingRef.current && !isLoading;
    wasLoadingRef.current = isLoading;
    if (!ended || !liveThreadId) return;
    const saving = queryClient.isMutating({ mutationKey: threadFieldMutationKey(liveThreadId) }) > 0;
    void queryClient.invalidateQueries({
      queryKey: queryKeys.threads.detail(liveThreadId),
      ...(saving ? { refetchType: 'none' as const } : {}),
    });
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
  const lapsedProvider = lapsedOf(model);
  const lapsed: LapsedSignIn | null = model && lapsedProvider ? { model, provider: lapsedProvider } : null;
  const offer: DefaultModelOffer | null = offered && offered === model && defaultModel && offered !== defaultModel
    ? { model: offered, defaultModel }
    : null;

  return { model, retired, lapsed, offer, pickModel, dismissOffer };
}
