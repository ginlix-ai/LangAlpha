import { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { getCompletedRowTitle } from '../toolDisplayConfig';

/** Screen-reader announcer for tool-call completions (carved out of ChatView,
 * 5.9c). Returns the string for ChatView's polite aria-live region. */
export function useToolCallAnnouncer(messages: unknown[]): string {
  const { t } = useTranslation();

  // --- Aria-live announcement for screen readers ---
  // String announced through a polite live region whenever a tool call
  // transitions from in-progress → completed/failed. Each completion is
  // queued and announced individually so a batch of completions in a single
  // SSE tick doesn't collapse to "only the last one" — screen readers
  // re-utter each one with a brief silence in between.
  const announcedToolCallIdsRef = useRef<Set<string>>(new Set());
  const announcementClearTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const announcementQueueRef = useRef<Array<{ label: string; failed: boolean }>>([]);
  const [recentlyCompletedAnnouncement, setRecentlyCompletedAnnouncement] = useState('');

  // Drain the announcement queue one item at a time. Each announcement is
  // displayed for 1500ms, followed by ~80ms of silence before the next so
  // screen readers treat each as a fresh utterance. Stable identity (no
  // deps) — uses tRef for fresh translations.
  const tRef = useRef(t);
  tRef.current = t;
  const pumpAnnouncements = useCallback(() => {
    if (announcementClearTimerRef.current) return;
    const next = announcementQueueRef.current.shift();
    if (!next) return;
    const currentT = tRef.current;
    const tail = next.failed
      ? currentT('chat.a11y.toolCallFailed', 'failed')
      : currentT('chat.a11y.toolCallCompleted', 'completed');
    setRecentlyCompletedAnnouncement(`${next.label} ${tail}`);
    announcementClearTimerRef.current = setTimeout(() => {
      announcementClearTimerRef.current = null;
      setRecentlyCompletedAnnouncement('');
      if (announcementQueueRef.current.length > 0) {
        setTimeout(pumpAnnouncements, 80);
      }
    }, 1500);
  }, []);

  // Aria-live announcements for tool call completion. Watches assistant
  // messages for tool-call processes that have transitioned out of
  // `isInProgress: true` and pushes a path-aware
  // "<verb> <object> completed/failed" string onto a queue that is drained
  // by `pumpAnnouncements`. Each tool-call id is announced at most once.
  useEffect(() => {
    const seen = announcedToolCallIdsRef.current;
    let enqueued = 0;

    for (const m of messages as unknown as Array<Record<string, unknown>>) {
      if (m?.role !== 'assistant') continue;
      const procs = m.toolCallProcesses as Record<string, Record<string, unknown>> | undefined;
      if (!procs) continue;
      for (const [id, proc] of Object.entries(procs)) {
        if (!proc) continue;
        if (proc.isInProgress) continue;
        // Only announce once per tool-call id.
        if (seen.has(id)) continue;
        // Only announce real terminal states (completed or failed). Skip
        // entries that haven't reached either yet.
        const isFailed = proc.isFailed === true;
        const isCompleted = proc.isComplete === true || proc.toolCallResult != null;
        if (!isFailed && !isCompleted) continue;
        seen.add(id);
        const toolName = (proc.toolName as string) || '';
        const toolCall = proc.toolCall as { args?: Record<string, unknown> } | undefined;
        const baseTitle = getCompletedRowTitle(toolName, toolCall, t);
        announcementQueueRef.current.push({ label: baseTitle, failed: isFailed });
        enqueued++;
      }
    }

    if (enqueued > 0) pumpAnnouncements();
  }, [messages, t, pumpAnnouncements]);

  // Clear announcement timer + queue on unmount.
  useEffect(() => {
    return () => {
      if (announcementClearTimerRef.current) {
        clearTimeout(announcementClearTimerRef.current);
        announcementClearTimerRef.current = null;
      }
      announcementQueueRef.current = [];
    };
  }, []);

  return recentlyCompletedAnnouncement;
}
