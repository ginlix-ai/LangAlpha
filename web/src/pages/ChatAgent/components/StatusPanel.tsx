import type { ReactElement, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { mediumTime } from '@/lib/format';
import PulseDot from '@/components/ui/pulse-dot';
import type { MarketWatchState } from '../hooks/utils/streamEventHandlers';
import { useThreadWakeups } from '../hooks/useWakeups';
import { WakeupListRows } from './wakeups/WakeupList';
import { isPending } from './wakeupLabels';

const TERTIARY = { color: 'var(--color-text-tertiary)' as const };

// Locale-aware clock (matches the prior `toLocaleTimeString()` shape: h:m:s).

/** A titled panel section. Phase 1 renders only the market-watch section; later
 *  status sections (todos, skills, background tasks) append as siblings. The
 *  pulse marks a section with something live in it. */
function StatusSection({
  title,
  caption,
  live = true,
  children,
}: {
  title: string;
  caption?: string;
  live?: boolean;
  children: ReactNode;
}): ReactElement {
  return (
    <section className="mb-4">
      <div className="mb-2 flex items-center gap-2">
        {live && <PulseDot />}
        <span className="text-xs font-semibold uppercase tracking-wide" style={TERTIARY}>
          {title}
        </span>
        {caption && (
          <span className="ml-auto text-[0.6875rem]" style={TERTIARY}>
            {caption}
          </span>
        )}
      </div>
      {children}
    </section>
  );
}

/** The streamed quote block, treated as opaque preformatted text: the first line
 *  is the backend's "As of …" header (muted caption), the rest are quote rows
 *  with tabular figures. No parsing — prices are never pulled out of it. */
function QuoteBlock({ content }: { content: string }): ReactElement {
  const newlineIdx = content.indexOf('\n');
  const header = newlineIdx === -1 ? content : content.slice(0, newlineIdx);
  const rows = newlineIdx === -1 ? '' : content.slice(newlineIdx + 1);
  return (
    <div className="mt-3">
      <div className="text-[0.6875rem]" style={TERTIARY}>
        {header}
      </div>
      {rows && (
        <div
          className="mt-1.5 whitespace-pre-wrap wrap-break-word tabular-nums text-xs leading-relaxed"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {rows}
        </div>
      )}
    </div>
  );
}

export interface StatusPanelProps {
  marketWatch?: MarketWatchState | null;
  /** The thread whose wakeups the panel lists; absent, it lists none. */
  threadId?: string | null;
  /** False while the panel's chat is kept warm off screen, which holds the
   *  list without reading it. */
  active?: boolean;
}

/**
 * The file panel's Status tab body: the live market watch (symbols + the quote
 * block streamed by `market_watch_update` events), then the thread's wakeups:
 * pending ones and those that ended in the last day.
 * Structured so later status surfaces can be appended as more sections.
 */
export default function StatusPanel({ marketWatch, threadId = null, active = true }: StatusPanelProps): ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const wakeups = useThreadWakeups(threadId, active);
  const symbols = marketWatch?.symbols ?? [];
  const items = wakeups?.items ?? [];
  const content = marketWatch?.content;
  const timestamp = marketWatch?.timestamp;
  const pendingCount = items.filter(isPending).length;
  // Only a pending wakeup's time left moves.
  const now = useNow(60_000, pendingCount > 0);

  const updatedCaption =
    timestamp != null
      ? t('chat.marketWatch.panelUpdated', {
          time: mediumTime(timestamp * 1000, locale),
          defaultValue: 'Updated {{time}}',
        })
      : undefined;

  return (
    <div className="h-full overflow-y-auto px-4 py-4">
      {/* With only wakeups, the empty watch list would read as nothing watched. */}
      {(symbols.length > 0 || !wakeups || items.length === 0) && (
        <StatusSection
          title={t('chat.marketWatch.panelTitle', { defaultValue: 'Market watch' })}
          caption={updatedCaption}
        >
          {symbols.length === 0 ? (
            <p className="text-sm" style={TERTIARY}>
              {t('chat.marketWatch.panelEmpty', { defaultValue: 'No tickers are being watched.' })}
            </p>
          ) : (
            <>
              <div className="flex flex-wrap gap-1.5">
                {symbols.map((symbol) => (
                  <span
                    key={symbol}
                    className="inline-flex items-center rounded-md px-2 py-0.5 text-sm font-medium"
                    style={{
                      background: 'var(--color-bg-subtle)',
                      color: 'var(--color-text-primary)',
                    }}
                  >
                    {symbol}
                  </span>
                ))}
              </div>
              {content ? (
                <QuoteBlock content={content} />
              ) : (
                <p className="mt-3 text-xs" style={TERTIARY}>
                  {t('chat.marketWatch.panelWaiting', {
                    defaultValue: 'Live prices appear here while the agent is working.',
                  })}
                </p>
              )}
            </>
          )}
        </StatusSection>
      )}
      {wakeups && (
        <StatusSection title={t('chat.wakeups.panelTitle')} live={pendingCount > 0}>
          {items.length === 0 ? (
            <p className="text-sm" style={TERTIARY}>{t('chat.wakeups.panelEmpty')}</p>
          ) : (
            <ul className="-mx-2.5 divide-y divide-(--color-border-muted)">
              <WakeupListRows threadId={threadId} wakeups={items} now={now} userTimezone={wakeups.userTimezone} />
            </ul>
          )}
        </StatusSection>
      )}
    </div>
  );
}
