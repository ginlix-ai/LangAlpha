import { useEffect } from 'react';
import { whenIdle } from '@/lib/whenIdle';
import type { HistoryWindowSnapshot } from '../../session/history/historyWindow';
import type { HoldPlace } from '../../session/history/olderHistory';

// A busy main thread (a turn streaming) may leave no idle period for a while;
// the fill waits at most this long for one. A browser with no idle callback
// (Safari) waits a moment instead.
const IDLE = { timeout: 2000, fallbackMs: 200 };

/**
 * Fills in, behind first paint, the older turns a newest page stopped short of
 * on its bytes: one step each time the window is idle and the reader can see
 * the transcript, in idle time and never while the tab is hidden. A step that
 * starts a page moves the snapshot, and its landing moves it back to idle,
 * which asks for the next, as does the scroll trigger taking a page held
 * ahead; the window says when nothing more is owed (historyWindow.backfill).
 * A reload, a thread switch or a fork moves the snapshot too, which cancels a
 * step not yet started, and the window refuses one already on its way.
 */
export function useHistoryBackfill({
  snapshot,
  ready,
  backfill,
  place,
}: {
  snapshot: HistoryWindowSnapshot;
  /** The transcript is shown and the reader's place in it restored. */
  ready: boolean;
  backfill: (place?: HoldPlace) => Promise<void>;
  /** Holds the reader's place across a page put above them (useChatScroll). */
  place: HoldPlace;
}) {
  useEffect(() => {
    if (!ready || snapshot.status !== 'idle') return;
    let cancel: (() => void) | null = null;
    const schedule = () => {
      if (cancel || document.visibilityState === 'hidden') return;
      cancel = whenIdle(() => {
        cancel = null;
        // Hidden since it was scheduled: the return to the tab asks again.
        if (document.visibilityState === 'hidden') return;
        void backfill(place);
      }, IDLE);
    };
    document.addEventListener('visibilitychange', schedule);
    schedule();
    return () => {
      document.removeEventListener('visibilitychange', schedule);
      cancel?.();
    };
  }, [ready, snapshot, backfill, place]);
}
