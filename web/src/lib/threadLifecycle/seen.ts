/**
 * Durable seen stamping: POST /threads/{id}/seen with the OBSERVED run_id —
 * the causal cursor (server refuses cross-thread / non-terminal runs and
 * never sweeps later settlements the client hasn't rendered).
 *
 * Fire-and-forget by design: the store's cursor is raised optimistically so
 * the dot clears instantly for this session; a lost POST self-heals on the
 * next open (thread-lifecycle v6 §3 failure matrix).
 */
import type { QueryClient } from '@tanstack/react-query';
import { api } from '@/api/client';
import { patchThreadSeen } from '@/lib/threadListCache';
import { applySeenCursors, raiseSeenToEffective } from './store';

/**
 * The lifecycle package's one QueryClient handle. Set by the feed mount and
 * read by every ambient caller (`markThreadSeen` is invoked from three call
 * sites that hold no client), so the feed client reads it back rather than
 * keeping a second copy.
 */
let queryClientRef: QueryClient | null = null;

export function setLifecycleQueryClient(qc: QueryClient): void {
  queryClientRef = qc;
}

export function getLifecycleQueryClient(): QueryClient | null {
  return queryClientRef;
}

const inflight = new Set<string>();

export function markThreadSeen(threadId: string, runId: string): void {
  if (!threadId || !runId) return;
  const key = `${threadId}:${runId}`;
  if (inflight.has(key)) return;
  inflight.add(key);
  raiseSeenToEffective(threadId);
  void (async () => {
    try {
      const { data } = await api.post<{
        last_seen_run_seq: number;
        latest_run_seq: number;
      }>(`/api/v1/threads/${threadId}/seen`, { run_id: runId });
      applySeenCursors(threadId, data);
      const qc = getLifecycleQueryClient();
      if (qc && typeof data?.last_seen_run_seq === 'number') {
        patchThreadSeen(qc, threadId, data.last_seen_run_seq);
      }
    } catch {
      // 409 seen_not_applicable (already-newer cursor / non-terminal) and
      // network loss are both fine — durable state self-heals on next open.
    } finally {
      inflight.delete(key);
    }
  })();
}
