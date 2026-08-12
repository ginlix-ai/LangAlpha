import { useTranslation } from 'react-i18next';
import { createDateFormatter } from '@/lib/format';
import PulseDot from '@/components/ui/pulse-dot';

// Locale-aware clock (matches the prior `toLocaleTimeString()` shape: h:m:s).
// The component calls useTranslation() so it re-renders on a locale switch.
const formatWatchTime = createDateFormatter({ timeStyle: 'medium' });

interface MarketWatchChipProps {
  /** Tickers the agent is currently live-stamping. Empty/undefined = watch off. */
  symbols?: string[] | null;
  /** Epoch-seconds timestamp of the last live stamp, when known (tooltip only). */
  lastUpdate?: number | null;
  /** When provided, the chip renders as a button that deep-links into the live
   *  watch detail (the RightPanel "Status" tab); omit for the read-only chip. */
  onClick?: () => void;
}

/**
 * Persistent "Watching NVDA, TSLA" chip. Renders only while the watched-symbols
 * list is non-empty — its absence IS the "watch off" signal. Purely
 * presentational; the watch state is owned by `useChatMessages` (GET-seeded on
 * thread load, overwritten by `market_watch_update` SSE events, refetched on
 * turn completion). With `onClick` it becomes a button that opens the Status tab.
 */
export default function MarketWatchChip({ symbols, lastUpdate, onClick }: MarketWatchChipProps) {
  const { t } = useTranslation();
  if (!symbols || symbols.length === 0) return null;

  const joined = symbols.join(', ');
  const title = lastUpdate
    ? t('chat.marketWatch.chipTitleUpdated', {
        time: formatWatchTime(lastUpdate * 1000),
        defaultValue: 'Live prices for watched tickers — updated {{time}}',
      })
    : t('chat.marketWatch.chipTitle', {
        defaultValue: 'Live prices for the tickers the agent is watching',
      });

  const dot = <PulseDot />;

  const label = t('chat.marketWatch.watching', {
    symbols: joined,
    defaultValue: 'Watching {{symbols}}',
  });

  // Clickable variant: a button that opens the Status tab. The polite live region
  // moves to the inner label span — a live region should not be the button itself.
  if (onClick) {
    return (
      <button
        type="button"
        onClick={onClick}
        title={title}
        aria-label={t('chat.marketWatch.chipAction', { defaultValue: 'Open live market watch' })}
        className="inline-flex items-center gap-2 self-start rounded-full px-3 py-1 text-xs cursor-pointer outline-none transition-colors bg-[var(--color-border-muted)] text-[var(--color-text-tertiary)] hover:bg-[var(--color-bg-elevated)] focus-visible:ring-2 focus-visible:ring-[var(--color-accent-primary)]"
      >
        {dot}
        <span role="status" aria-live="polite">
          {label}
        </span>
      </button>
    );
  }

  return (
    <div
      className="inline-flex items-center gap-2 self-start rounded-full px-3 py-1 text-xs"
      role="status"
      aria-live="polite"
      title={title}
      style={{
        color: 'var(--color-text-tertiary)',
        background: 'var(--color-border-muted)',
      }}
    >
      {dot}
      <span>{label}</span>
    </div>
  );
}
