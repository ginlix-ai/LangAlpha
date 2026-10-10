import { useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useLatestRef } from '@/hooks/useLatestRef';
import type { OlderHistoryStatus } from '../../session/history/historyWindow';

// How far above the viewport the next older page is asked for: about a screen,
// so a reader scrolling up at a steady pace finds it already there.
const OLDER_HISTORY_PREFETCH_PX = 600;

/**
 * The top of a paged transcript, which asks for the next older page as the
 * reader nears it. One height whatever it shows, so a page starting, landing
 * or failing never moves the transcript under the reader; absent at the start
 * of the thread and while the transcript is rebuilt.
 */
export function OlderHistoryRow({
  getRoot,
  status,
  canLoad,
  onLoad,
}: {
  /** The scrolling viewport the reader's distance from the top is measured in. */
  getRoot: () => HTMLElement | null;
  status: OlderHistoryStatus;
  /** A page may be asked for now: the window is idle and the reader's place
   *  in the transcript has been restored. */
  canLoad: boolean;
  onLoad: () => void;
}) {
  const { t } = useTranslation();
  const rowRef = useRef<HTMLDivElement>(null);
  const onLoadRef = useLatestRef(onLoad);

  // Observed afresh each time a page may be asked for, so a page too short to
  // carry the top out of range asks for the next one at once: an observer
  // reports where its target is when it starts, not only when that changes.
  useEffect(() => {
    const row = rowRef.current;
    const root = getRoot();
    if (!row || !root || !canLoad) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries.some((entry) => entry.isIntersecting)) onLoadRef.current();
      },
      { root, rootMargin: `${OLDER_HISTORY_PREFETCH_PX}px 0px 0px 0px` },
    );
    observer.observe(row);
    return () => observer.disconnect();
  }, [getRoot, canLoad, onLoadRef]);

  if (status === 'end' || status === 'reloading') return null;
  const label = t('chat.loadingOlderHistory');
  return (
    <div
      ref={rowRef}
      className="flex h-8 items-center justify-center gap-2 text-xs"
      style={{ color: 'var(--color-text-tertiary)' }}
    >
      {status === 'loading' ? (
        <>
          <Loader size={12} label={label} style={{ color: 'var(--color-accent-primary)' }} />
          <span aria-hidden="true">{label}</span>
        </>
      ) : status === 'failed' ? (
        <span role="alert" className="flex items-center gap-2">
          {t('chat.olderHistoryFailed')}
          <button
            type="button"
            className="underline-offset-2 hover:underline"
            style={{ color: 'var(--color-text-secondary)' }}
            onClick={onLoad}
          >
            {t('common.retry')}
          </button>
        </span>
      ) : null}
    </div>
  );
}
