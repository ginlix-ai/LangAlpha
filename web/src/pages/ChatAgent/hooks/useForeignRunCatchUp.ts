import { useEffect, useRef } from 'react';
import { useThreadFeedRunId } from '@/lib/threadLifecycle/store';

interface ForeignRunCatchUpOptions {
  threadId: string;
  isActive: boolean;
  /** The view is streaming or loading its history. */
  busy: boolean;
  awaitingReportBack: boolean;
  isOwnRun: (runId: string) => boolean;
  catchUp: () => Promise<void>;
}

/**
 * A run that starts on this thread from somewhere else (another tab, an
 * automation that waited for the last turn) is announced on the user feed
 * and brought in the way a re-shown view catches up. It is held while the
 * view is busy: a waiting automation starts the moment the turn settles,
 * often before this view's stream has closed. The view's own run is announced
 * too and can end before its announcement lands, when the turn watermark
 * (only a lower bound) could take the view for stale and reload it for
 * nothing, so a run this view streamed is passed over. So is a run while the
 * report-back watch is armed, since that watch attaches the thread's
 * report-back runs itself and a reload would race it.
 */
export function useForeignRunCatchUp({
  threadId,
  isActive,
  busy,
  awaitingReportBack,
  isOwnRun,
  catchUp,
}: ForeignRunCatchUpOptions): void {
  const feedRunId = useThreadFeedRunId(threadId);
  const lastFeedRunRef = useRef({ threadId, runId: feedRunId });
  const pendingFeedRunRef = useRef<string | null>(null);
  useEffect(() => {
    const last = lastFeedRunRef.current;
    lastFeedRunRef.current = { threadId, runId: feedRunId };
    // Hidden, the view catches up when it is shown again instead.
    if (last.threadId !== threadId || !isActive) pendingFeedRunRef.current = null;
    else if (feedRunId && feedRunId !== last.runId) pendingFeedRunRef.current = feedRunId;
    const pending = pendingFeedRunRef.current;
    if (!pending || busy) return;
    pendingFeedRunRef.current = null;
    if (awaitingReportBack || isOwnRun(pending)) return;
    void catchUp();
  }, [threadId, feedRunId, isActive, busy, awaitingReportBack, isOwnRun, catchUp]);
}
