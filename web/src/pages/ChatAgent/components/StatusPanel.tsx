import { useEffect, useRef, type ReactElement, type ReactNode } from 'react';
import { ArrowDown } from 'lucide-react';
import { scrollMotion } from '@/lib/reducedMotion';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import PulseDot from '@/components/ui/pulse-dot';
import { Sparkline } from '@/components/charts/Sparkline';
import { DASH, mediumTime } from '@/lib/format';
import { marketPhaseKey } from '@/lib/marketUtils';
import { moveTone, signedPercent } from '@/lib/priceMove';
import { quotePrice, useThreadQuoteLines, useThreadQuotes, type ThreadQuote } from './chatView/threadQuotesContext';
import { WakeupListRows } from './wakeups/WakeupList';
import { isPending } from './wakeupLabels';
import { AgentPlan } from './messageList/AgentPlan';
import { planHeading } from './messageList/requestPlans';
import { AnalystRow, TaskRow, TRAY_ROW } from './chatView/statusRows';
import { CommandListRows } from './commands/CommandListRows';
import { useThreadStatus, type StatusSectionId, type ThreadStatus } from './chatView/threadStatus';
import { useOpenDispatchedThread } from '../hooks/useOpenDispatchedThread';
import type { RosterAnalyst } from './chatView/useAnalystRoster';

const TERTIARY = { color: 'var(--color-text-tertiary)' as const };

/** A titled panel section, which a tray chip can bring to the top by its
 *  id. The pulse marks a section with something live in it. */
function StatusSection({
  id,
  title,
  caption,
  live = true,
  children,
}: {
  id: StatusSectionId;
  title: string;
  caption?: string;
  live?: boolean;
  children: ReactNode;
}): ReactElement {
  return (
    <section className="mb-5 scroll-mt-4" data-status-section={id}>
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

/** One watched ticker at its live price: today's line, the price and the
 *  day's move. The line is scaled to the day's own range, so a gap from the
 *  previous close (which the move states) does not flatten it. */
function WatchQuoteRow({ symbol, quote }: { symbol: string; quote: ThreadQuote | undefined }): ReactElement {
  const locale = useLocale();
  const tone = moveTone(quote?.pct);
  return (
    <li className="grid grid-cols-[minmax(0,1fr)_6rem_4.5rem_4rem] items-center gap-3 px-2 py-1.5">
      <span className="truncate text-xs font-semibold tabular-nums" style={{ color: 'var(--color-text-primary)' }}>{symbol}</span>
      <span className="h-6">
        {quote && quote.line.length > 1 && (
          <Sparkline values={quote.line} color={tone} width={96} height={24} stretch={false} fill dot strokeWidth={1.25} />
        )}
      </span>
      <span className="text-right text-xs tabular-nums" style={{ color: 'var(--color-text-primary)' }}>
        {quotePrice(quote, locale)}
      </span>
      <span className="text-right text-xs tabular-nums" style={{ color: tone }}>
        {quote?.pct != null ? signedPercent(quote.pct) : DASH}
      </span>
    </li>
  );
}

/**
 * The file panel's Status tab: what the thread is doing and whether it needs
 * the reader, in full where the tray above the composer only counts. What
 * waits on the reader comes first, then the plan, the work running now, the
 * market watch and wakeups, and the work that has ended. A section with
 * nothing in it is left out. Everything it shows is the chat view's
 * `ThreadStatus`, which the tray reads too; the tab opens only on a chat's
 * own thread, so there is always one.
 */
export default function StatusPanel(): ReactElement | null {
  const status = useThreadStatus();
  return status && <StatusPanelBody status={status} />;
}

function StatusPanelBody({ status }: { status: ThreadStatus }): ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const { allWakeups: wakeups, threadId, latestPlan: plan, work } = status;
  const marketWatch = status.contents?.watch ?? null;
  const symbols = marketWatch?.symbols ?? [];
  const quotes = useThreadQuotes();
  // The watch's rows draw each ticker's line.
  useThreadQuoteLines(symbols.length > 0);
  // Until a quote has a price, or where the market data has none, the agent's
  // own block is what there is: every watched symbol has an entry either way.
  // While only some have a price, the block stays under their rows for the rest.
  const priced = symbols.filter((symbol) => quotes.bySymbol[symbol]?.price != null);
  const quoted = priced.length > 0;
  const watchLive = symbols.some((symbol) => quotes.bySymbol[symbol]?.live);
  const items = wakeups?.items ?? [];
  const content = marketWatch?.content;
  const timestamp = marketWatch?.timestamp;
  const pendingCount = items.filter(isPending).length;
  // Only a pending wakeup's time left moves.
  const now = useNow(60_000, pendingCount > 0);

  const openDispatchedThread = useOpenDispatchedThread(status.from);
  const openAnalyst = (thread: RosterAnalyst) => openDispatchedThread(thread.threadId, thread.workspaceId);
  const { needs, working, done, counts } = work;
  // The watch and wakeups say they are empty only when nothing else is here,
  // so a thread with work going on does not read as idle beside it.
  const hasOther = counts.needs + counts.working + counts.done > 0 || !!plan;

  // A chip asked for a section: bring it to the top of the panel.
  const rootRef = useRef<HTMLDivElement>(null);
  const { focus } = status;
  useEffect(() => {
    if (!focus) return;
    if (focus.section === null) {
      rootRef.current?.scrollTo?.({ top: 0, behavior: scrollMotion() });
      return;
    }
    rootRef.current
      ?.querySelector(`[data-status-section="${focus.section}"]`)
      ?.scrollIntoView({ block: 'start', behavior: scrollMotion() });
  }, [focus]);

  const updatedCaption =
    timestamp != null
      ? t('chat.marketWatch.panelUpdated', {
          time: mediumTime(timestamp * 1000, locale),
          defaultValue: 'Updated {{time}}',
        })
      : undefined;

  return (
    <div ref={rootRef} className="h-full overflow-y-auto px-4 py-4">
      {counts.needs > 0 && (
        <StatusSection id="needs" title={t('chat.status.needsYou')} caption={String(counts.needs)}>
          <ul className="-mx-2.5 flex flex-col gap-0.5">
            {work.awaitingAnswer && (
              <li>
                <button type="button" className={TRAY_ROW} onClick={status.onJumpToLatest}>
                  <span className="min-w-0 flex-1 truncate text-[0.8125rem]" style={{ color: 'var(--color-text-primary)' }}>
                    {t('chat.status.awaitingAnswer')}
                  </span>
                  <ArrowDown aria-hidden="true" className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-quaternary)' }} />
                </button>
              </li>
            )}
            {needs.map((thread) => (
              <li key={thread.threadId}><AnalystRow thread={thread} onOpen={() => openAnalyst(thread)} /></li>
            ))}
          </ul>
        </StatusSection>
      )}
      {plan && (
        <StatusSection
          id="plan"
          title={t('chat.plan.label')}
          caption={planHeading(plan.steps, plan.state, t)}
          live={false}
        >
          <AgentPlan steps={plan.steps} state={plan.state} />
        </StatusSection>
      )}
      {counts.working > 0 && (
        <StatusSection id="working" title={t('chat.status.workingNow')} caption={String(counts.working)} live={false}>
          <ul className="-mx-2.5 flex flex-col gap-0.5">
            {working.tasks.map((row) => (
              <li key={row.id}><TaskRow row={row} onOpen={status.onOpenTask} /></li>
            ))}
            {working.analysts.map((thread) => (
              <li key={thread.threadId}><AnalystRow thread={thread} onOpen={() => openAnalyst(thread)} /></li>
            ))}
            {working.commands.length > 0 && <CommandListRows rows={working.commands} onOpen={status.onOpenCommand} />}
          </ul>
        </StatusSection>
      )}
      {/* With only wakeups, the empty watch list would read as nothing watched. */}
      {(symbols.length > 0 || (!hasOther && (!wakeups || items.length === 0))) && (
        <StatusSection
          id="watch"
          title={t('chat.marketWatch.panelTitle', { defaultValue: 'Market watch' })}
          caption={quoted ? (watchLive ? t('chat.marketWatch.live') : t(marketPhaseKey(quotes.phase))) : updatedCaption}
        >
          {symbols.length === 0 ? (
            <p className="text-sm" style={TERTIARY}>
              {t('chat.marketWatch.panelEmpty', { defaultValue: 'No tickers are being watched.' })}
            </p>
          ) : quoted ? (
            <>
              <ul className="-mx-2">
                {symbols.map((symbol) => <WatchQuoteRow key={symbol} symbol={symbol} quote={quotes.bySymbol[symbol]} />)}
              </ul>
              {content && priced.length < symbols.length && <QuoteBlock content={content} />}
            </>
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
      {wakeups && (items.length > 0 || !hasOther) && (
        <StatusSection id="wakeups" title={t('chat.wakeups.panelTitle')} live={pendingCount > 0}>
          {items.length === 0 ? (
            <p className="text-sm" style={TERTIARY}>{t('chat.wakeups.panelEmpty')}</p>
          ) : (
            <ul className="-mx-2.5 divide-y divide-(--color-border-muted)">
              <WakeupListRows threadId={threadId} wakeups={items} now={now} userTimezone={wakeups.userTimezone} />
            </ul>
          )}
        </StatusSection>
      )}
      {counts.done > 0 && (
        <StatusSection id="done" title={t('chat.status.done')} caption={String(counts.done)} live={false}>
          <ul className="-mx-2.5 flex flex-col gap-0.5">
            {done.tasks.map((row) => (
              <li key={row.id}><TaskRow row={row} onOpen={status.onOpenTask} /></li>
            ))}
            {done.analysts.map((thread) => (
              <li key={thread.threadId}><AnalystRow thread={thread} onOpen={() => openAnalyst(thread)} /></li>
            ))}
          </ul>
        </StatusSection>
      )}
    </div>
  );
}
