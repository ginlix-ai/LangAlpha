import type { ReactElement, ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import type { MarketWatchState } from '../hooks/utils/streamEventHandlers';

const TERTIARY = { color: 'var(--color-text-tertiary)' as const };

/** A small pulsing accent dot — the market-watch chip's two-span `animate-ping`
 *  idiom, reused as a "live" marker on a status section header. */
function LiveDot(): ReactElement {
  return (
    <span className="relative flex h-2 w-2" aria-hidden="true">
      <span
        className="animate-ping absolute inline-flex h-full w-full rounded-full opacity-75"
        style={{ background: 'var(--color-accent-primary)' }}
      />
      <span
        className="relative inline-flex h-2 w-2 rounded-full"
        style={{ background: 'var(--color-accent-primary)' }}
      />
    </span>
  );
}

/** A titled panel section. Phase 1 renders only the market-watch section; later
 *  status sections (todos, skills, background tasks) append as siblings. */
function StatusSection({
  title,
  caption,
  children,
}: {
  title: string;
  caption?: string;
  children: ReactNode;
}): ReactElement {
  return (
    <section className="mb-4">
      <div className="mb-2 flex items-center gap-2">
        <LiveDot />
        <span className="text-xs font-semibold uppercase tracking-wide" style={TERTIARY}>
          {title}
        </span>
        {caption && (
          <span className="ml-auto text-[11px]" style={TERTIARY}>
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
      <div className="text-[11px]" style={TERTIARY}>
        {header}
      </div>
      {rows && (
        <div
          className="mt-1.5 whitespace-pre-wrap break-words tabular-nums text-xs leading-relaxed"
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
}

/**
 * The RightPanel "Status" tab body. Phase 1 shows one section — the live market
 * watch (symbols + the quote block streamed by `market_watch_update` events).
 * Structured so later status surfaces can be appended as more sections.
 */
export default function StatusPanel({ marketWatch }: StatusPanelProps): ReactElement {
  const { t } = useTranslation();
  const symbols = marketWatch?.symbols ?? [];
  const content = marketWatch?.content;
  const timestamp = marketWatch?.timestamp;

  const updatedCaption =
    timestamp != null
      ? t('chat.marketWatch.panelUpdated', {
          time: new Date(timestamp * 1000).toLocaleTimeString(),
          defaultValue: 'Updated {{time}}',
        })
      : undefined;

  return (
    <div className="h-full overflow-y-auto px-4 py-4">
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
    </div>
  );
}
