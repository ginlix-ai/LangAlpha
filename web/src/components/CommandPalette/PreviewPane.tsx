import { useEffect, useRef } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useTranslation } from 'react-i18next';
import { Sparkline } from '@/components/charts/Sparkline';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { timezoneForSymbol } from '@/lib/bars/exchanges';
import { sessionLineQuery } from '@/lib/bars/sessionQuery';
import { clockTime, formatCalendarMonthDay } from '@/lib/format';
import { flattenInline } from '@/lib/plainLine';
import { quoteKey, useQuotes } from '@/lib/quotes';
import { dateStrInTz } from '@/lib/utils';
import { matchSpans } from '@/lib/textMatch';
import { Highlighted, QuoteFigures } from './PaletteRow';
import { useSearchPreview } from './useServerSearch';
import { useNames } from './useVerbs';
import type { PreviewTarget } from './types';

interface PaneProps {
  /** What the selected row shows. */
  target: PreviewTarget | undefined;
  /** The words searched for, bolded where they appear. */
  words: readonly string[];
}

/**
 * The drilled-in view's preview of the selected row. Its width is fixed by
 * the palette, whatever it shows: a pane that resized as the arrows moved
 * would be worse than none.
 */
export function PreviewPane({ target, words }: PaneProps) {
  return (
    <aside className="command-palette-pane" aria-hidden>
      {target?.kind === 'ticker' ? (
        <TickerPreview symbol={target.symbol} />
      ) : target ? (
        <ConversationPreview target={target} words={words} />
      ) : null}
    </aside>
  );
}

/** A turn's text as the pane reads it: one paragraph per line, with Markdown's marks taken off. */
function plainTurn(markdown: string): string {
  return markdown
    .split('\n')
    .filter((line) => !/^\s*(```|~~~)/.test(line))
    .map((line) => flattenInline(line.replace(/^[ \t]*(?:>[ \t]*)*(?:#{1,6}[ \t]+)?/, '')))
    .join('\n')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

function ConversationPreview({ target, words }: { target: Exclude<PreviewTarget, { kind: 'ticker' }>; words: readonly string[] }) {
  const { t } = useTranslation();
  const names = useNames();
  const scrollRef = useRef<HTMLDivElement>(null);
  const { preview, loading, failure } = useSearchPreview(target);
  const side = target.kind === 'turn' ? target.side : null;
  const userText = preview ? plainTurn(preview.user_text) : '';
  const answerText = preview ? plainTurn(preview.answer_text) : '';

  // The words that matched are what the reader came for: the first of them
  // on the side that matched is brought into view.
  useEffect(() => {
    const scroller = scrollRef.current;
    if (!scroller) return;
    const mark = scroller.querySelector<HTMLElement>(`[data-side="${side ?? 'user'}"] mark`) ?? scroller.querySelector('mark');
    scroller.scrollTop = mark ? Math.max(0, mark.offsetTop - scroller.clientHeight / 3) : 0;
  }, [preview, side]);

  if (failure) {
    return (
      <p className="command-palette-pane-note">
        {failure === 'missing' ? t('palette.preview.missing') : failure === 'busy' ? t('palette.busy') : t('palette.preview.failed')}
      </p>
    );
  }
  if (loading || !preview) {
    return (
      <div className="command-palette-pane-loading">
        <span />
        <span />
        <span />
      </div>
    );
  }
  const agent = names.isHome(preview.workspace_id) ? t('agents.chiefOfStaff') : t('agents.analyst');
  return (
    <div ref={scrollRef} className="command-palette-pane-scroll">
      <div className="command-palette-pane-head">
        <div className="command-palette-pane-title">{preview.title || t('palette.untitledThread')}</div>
        <div className="command-palette-pane-meta">
          {[names.workspaceName(preview.workspace_id, preview.workspace_name), names.when(preview.turn_at)].filter(Boolean).join(' · ')}
        </div>
      </div>
      {userText && (
        <section data-side="user" className="command-palette-pane-turn">
          <div className="command-palette-pane-kicker">{t('palette.you')}</div>
          <p><Highlighted text={userText} spans={matchSpans(userText, words)} /></p>
        </section>
      )}
      {answerText && (
        <section data-side="agent" className="command-palette-pane-turn">
          <div className="command-palette-pane-kicker">{agent}</div>
          <p><Highlighted text={answerText} spans={matchSpans(answerText, words)} /></p>
        </section>
      )}
    </div>
  );
}

function TickerPreview({ symbol }: { symbol: string }) {
  const { quotes } = useQuotes([symbol], { refetchInterval: false });
  const quote = quotes[quoteKey(symbol)];
  return (
    <div className="command-palette-pane-scroll">
      <div className="command-palette-pane-ticker">
        <span className="command-palette-pane-symbol">{symbol}</span>
        {quote?.name && <span className="command-palette-pane-meta">{quote.name}</span>}
      </div>
      <QuoteFigures
        symbol={symbol}
        quote={quote}
        full
        className="command-palette-pane-price"
        fallback={
          <div className="command-palette-pane-loading">
            <span />
          </div>
        }
      />
      <SessionSparkline symbol={symbol} previousClose={quote?.previous_close ?? null} />
    </div>
  );
}

/** A time of day in the reader's clock that prints as `minutes` past midnight: a venue's hours in its own wall clock. */
const minuteOfDay = (minutes: number) => new Date(2000, 0, 1, 0, minutes);

/**
 * The latest session's prices across its hours, against the close before it:
 * today's while the venue trades or once it has closed for the day, the last
 * session's (captioned with its date) on a day it does not open. Nothing, and
 * no room kept, until there is a line to draw.
 */
function SessionSparkline({ symbol, previousClose }: { symbol: string; previousClose: number | null }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { data: line } = useQuery(sessionLineQuery(symbol));
  if (!line) return null;
  // The bars' own previous session is the close this line moved from; the
  // quote's stands in when the bars do not reach back to it.
  const baseline = line.previousClose ?? previousClose;
  const last = line.values[line.values.length - 1];
  const color = last >= (baseline ?? line.values[0]) ? 'var(--color-profit)' : 'var(--color-loss)';
  const today = line.date === dateStrInTz(now, timezoneForSymbol(symbol));
  return (
    <figure className="command-palette-pane-spark">
      <Sparkline values={line.values} at={line.at} baseline={baseline} color={color} height={46} />
      <figcaption>
        <span>{clockTime(minuteOfDay(line.opens), locale)}</span>
        <span>{today ? t('palette.preview.today') : formatCalendarMonthDay(line.date, locale)}</span>
        <span>{clockTime(minuteOfDay(line.closes), locale)}</span>
      </figcaption>
    </figure>
  );
}
