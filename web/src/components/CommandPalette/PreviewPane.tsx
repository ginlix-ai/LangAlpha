import { Suspense, lazy, useCallback, useMemo, useState, type ComponentType } from 'react';
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
import { useShown, type TurnBodyProps } from './turnBody';
import { useSearchPreview } from './useServerSearch';
import { useNames } from './useVerbs';
import type { PreviewTarget } from './types';

// The Markdown renderer (with its parser and highlighter) is the pane's to
// load, the first time it shows a turn, so the palette opens without it.
// Until it arrives the turn shows as plain text, and stays so if it cannot.
const MarkdownBody = lazy<ComponentType<TurnBodyProps>>(() => import('./PreviewMarkdown').catch(() => ({ default: PlainBody })));

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

/** A side of a turn as plain text, its matches marked: the pane's text until the Markdown renderer loads. */
function PlainBody(props: TurnBodyProps) {
  const { text, words } = props;
  useShown(props);
  const plain = plainTurn(text);
  return (
    <p className="command-palette-pane-plain">
      <Highlighted text={plain} spans={matchSpans(plain, words)} />
    </p>
  );
}

function ConversationPreview({ target, words }: { target: Exclude<PreviewTarget, { kind: 'ticker' }>; words: readonly string[] }) {
  const { t } = useTranslation();
  const names = useNames();
  // State, not a ref: on the commit that mounts the scroller the bodies'
  // layout effects run before its ref is set, and the render setting it
  // starts hands them a showMatch that reaches it.
  const [scroller, setScroller] = useState<HTMLDivElement | null>(null);
  const { preview, loading, failure } = useSearchPreview(target);
  const side = target.kind === 'turn' ? target.side : null;

  // The words that matched are what the reader came for: each body brings
  // the first of them on the side that matched into view as it draws.
  // Keyed on the words' text, as the bodies key their scroll: a render of
  // the palette that keeps them asks for no new scroll. (Search words hold
  // no spaces.)
  const wordsKey = words.join(' ');
  const matchWords = useMemo(() => (wordsKey ? wordsKey.split(' ') : []), [wordsKey]);
  const showMatch = useCallback(() => {
    if (scroller) scrollToMatch(scroller, side ?? 'user', matchWords);
  }, [scroller, side, matchWords]);

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
  const sides = [
    { side: 'user', kicker: t('palette.you'), text: preview.user_text },
    { side: 'agent', kicker: agent, text: preview.answer_text },
  ].filter(({ text }) => text.trim() !== '');
  return (
    <div ref={setScroller} className="command-palette-pane-scroll">
      <div className="command-palette-pane-head">
        <div className="command-palette-pane-title">{preview.title || t('palette.untitledThread')}</div>
        <div className="command-palette-pane-meta">
          {[names.workspaceName(preview.workspace_id, preview.workspace_name), names.when(preview.turn_at)].filter(Boolean).join(' · ')}
        </div>
      </div>
      {sides.map(({ side: which, kicker, text }) => (
        <section key={which} data-side={which} className="command-palette-pane-turn">
          <div className="command-palette-pane-kicker">{kicker}</div>
          <Suspense fallback={<PlainBody text={text} words={words} onShown={showMatch} />}>
            <MarkdownBody text={text} words={words} onShown={showMatch} />
          </Suspense>
        </section>
      ))}
    </div>
  );
}

/**
 * The first match in `root` a reader can see, in reading order: a mark the
 * search made (not one the text wrote), or a code block whose text holds a
 * word. Code carries no marks (its renderer reads the fence as a string), so
 * a match only in code is found by its block. A closed `<details>` shows
 * only its summary.
 */
function firstMatch(root: ParentNode, words: readonly string[]): Element | null {
  for (const el of root.querySelectorAll('mark[data-match], .command-palette-pane-plain mark, pre')) {
    if (el.closest('details:not([open])') && !el.closest('summary')) continue;
    if (el.tagName === 'MARK' || matchSpans(el.textContent ?? '', words).length > 0) return el;
  }
  return null;
}

/** Scrolls `scroller` to the first match on `side`, or on either side, a third of the way down; to the top without one. */
function scrollToMatch(scroller: HTMLElement, side: 'user' | 'agent', words: readonly string[]) {
  const matched = scroller.querySelector(`[data-side="${side}"]`);
  const match = (matched && firstMatch(matched, words)) ?? firstMatch(scroller, words);
  const top = match ? match.getBoundingClientRect().top - scroller.getBoundingClientRect().top - scroller.clientTop + scroller.scrollTop : 0;
  scroller.scrollTop = Math.max(0, top - scroller.clientHeight / 3);
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
