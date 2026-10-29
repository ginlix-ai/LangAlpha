/**
 * Create-first flow: pre-create the thread (POST /threads) so the real id
 * arrives one round-trip after send — the URL swap, the sidebar row, and the
 * auto-title all run without waiting on the run stream (a cold PTC sandbox can
 * hold the first SSE frame for seconds). Any failure falls back to the legacy
 * `__default__` door, where the server mints the id mid-stream.
 */
import type { QueryClient } from '@tanstack/react-query';
import { insertOptimisticThread } from '@/lib/threadListCache';
import type { Thread } from '@/types/api';
import { createThreadWithTitle } from '../utils/api';
import { bumpThreadNavOrder } from '../hooks/useNavigationData';

export interface EnsureThreadIdArgs {
  threadId: string;
  workspaceId: string;
  /** First user message — becomes the row's first_query_content + title seed. */
  message: string;
  /** The row's title, in place of one generated from `message`: a send that
   *  opens with no message of the user's has nothing fit to title it from. */
  title?: string;
  platform: string | null | undefined;
  /** The user's zone, which the title reads relative dates on. */
  timezone: string;
  /** The composer's subagents pick for the new thread; absent leaves the
   *  owner's default to the server. */
  subagentsAllowed?: boolean;
  /** The thread's own effort and fast mode, picked before it existed; absent
   *  leaves the thread following Settings. Not the turn's level, which the
   *  send carries on its own. */
  reasoningEffortOverride?: string;
  fastModeOverride?: boolean;
  queryClient: QueryClient;
  /** Kept in sync so in-flight closures see the real id immediately. */
  threadIdRef: { current: string };
  setThreadId: (threadId: string) => void;
  /** Re-checked after the round-trip — a stop during pre-create already
   *  finalized the UI, so starting the run would resurrect it. */
  wasStoppedRef: { current: boolean };
  /** The send's abort signal — stopWorkflow aborts it, so it catches a stop
   *  even when a subsequent send has already reset wasStoppedRef. */
  signal: AbortSignal;
}

export interface EnsureThreadIdResult {
  threadId: string;
  /** True when the caller must abandon the send (user stopped mid-create). */
  aborted: boolean;
}

export async function ensureThreadId({
  threadId,
  workspaceId,
  message,
  title,
  platform,
  timezone,
  subagentsAllowed,
  reasoningEffortOverride,
  fastModeOverride,
  queryClient,
  threadIdRef,
  setThreadId,
  wasStoppedRef,
  signal,
}: EnsureThreadIdArgs): Promise<EnsureThreadIdResult> {
  if (threadId && threadId !== '__default__') return { threadId, aborted: false };
  // A send with no words (attachments alone) and no title gives the row
  // nothing to be titled by, which the server refuses: the legacy door makes
  // that thread without the failed round-trip.
  if (!message.trim() && !title) return { threadId, aborted: signal.aborted || wasStoppedRef.current };

  let effectiveThreadId = threadId;
  try {
    const created = await createThreadWithTitle({
      workspaceId,
      firstQuery: message,
      title,
      platform,
      timezone,
      subagentsAllowed,
      reasoningEffort: reasoningEffortOverride,
      fastMode: fastModeOverride,
    });
    // Stop landed during the round-trip: adopting the id now (navigate,
    // optimistic row, nav bump) would resurrect the UI the stop finalized.
    // The durable row is harmless — it surfaces on the next list refetch.
    if (signal.aborted || wasStoppedRef.current) return { threadId, aborted: true };
    effectiveThreadId = created.thread_id;
    threadIdRef.current = created.thread_id;
    // Same flip the first SSE frame performs on the legacy path — drives
    // ChatView's navigate(replace) + reconcile invalidate, just earlier.
    setThreadId(created.thread_id);
    const nowIso = new Date().toISOString();
    insertOptimisticThread(queryClient, {
      thread_id: created.thread_id,
      workspace_id: created.workspace_id,
      thread_index: created.thread_index,
      current_status: 'in_progress',
      msg_type: created.msg_type,
      title: created.title,
      first_query_content: message,
      created_at: nowIso,
      updated_at: nowIso,
    } as Thread);
    bumpThreadNavOrder(created.workspace_id, created.thread_id);
  } catch (e) {
    console.warn('[Send] thread pre-create failed; using legacy new-thread flow', e);
  }
  return { threadId: effectiveThreadId, aborted: signal.aborted || wasStoppedRef.current };
}
