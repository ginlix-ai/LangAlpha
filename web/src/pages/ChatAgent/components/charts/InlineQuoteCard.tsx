import React from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney } from '@/lib/bars';
import { createDateFormatter } from '@/lib/format';
import { resolveFreshnessBadge } from '@/lib/freshness';
import { readQuotes, type InlineCardProps, type QuoteDisplay } from './cardReaders';
import { extendedHoursLabel, marketStatusLabel } from './cardWords';
import { FreshnessBadge } from './FreshnessBadge';
import { DualName, PriceHero } from './PriceHero';
import { TermCard, TermChange, TermRange, TermTag } from './TermCard';
import { percentMove } from './termChange';
import { useCardNarrow } from './termContext';
import { commonFreshness } from './termFreshness';
import { TermTable, type TermColumn } from './TermTable';

// User-local rendering of the retrieval instant: the tooltip on market-tz stamps.
const localTitleFormat = createDateFormatter({
  year: 'numeric', month: 'numeric', day: 'numeric',
  hour: 'numeric', minute: 'numeric', second: 'numeric',
});
function localTitle(asOfTs: number | undefined, locale: string): string | undefined {
  return asOfTs != null ? localTitleFormat(asOfTs, locale) : undefined;
}

// The badge's tooltip is where a row's venue stamp goes, so the plain stamp
// steps aside only when a badge renders and has a stamp to carry. A row with
// freshness but no badge, or a badge but no venue stamp, still shows its time.
function badgeCarriesStamp(d: QuoteDisplay): boolean {
  return !!d.asOfLocal && resolveFreshnessBadge(d) != null;
}

/** The regular session is the default reading; any other is worth a tag. */
function offSession(status: string | null | undefined): status is string {
  return !!status && status !== 'open';
}

function hasRange(d: QuoteDisplay): d is QuoteDisplay & { low: number; high: number; price: number } {
  return d.low != null && d.high != null && d.price != null && d.high > d.low;
}

/**
 * A second move beside a row's own: signed rather than arrowed, so the arrow
 * stays the row's change. Zero is gray and unsigned, as TermChange draws it.
 */
function SignedChange({ value }: { value: number }): React.ReactElement {
  const { dir, sign, size } = percentMove(value);
  return <span className="term-chg" data-dir={dir}>{`${sign}${size}`}</span>;
}

/**
 * Where a phone puts the block's time: on its edge a 23-character stamp
 * leaves the title no room, so it closes the card instead.
 */
function StampFooter({ children }: { children: React.ReactNode }): React.ReactElement {
  return <div className="term-t3 term-stamp-foot">{children}</div>;
}

function QuoteHero({
  d,
  asOf,
  asOfTs,
  onClick,
  link,
}: {
  d: QuoteDisplay;
  asOf?: string;
  asOfTs?: number;
  onClick?: () => void;
  /** At the frame's foot in the panel: the way to the symbol's own page. */
  link?: React.ReactNode;
}): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const money = (n: number): string => formatMoney(n, d.currency, locale);
  const stamp = badgeCarriesStamp(d) ? undefined : (d.asOfLocal ?? asOf);
  const stampEl = stamp && <span title={localTitle(asOfTs, locale)}>{stamp}</span>;
  const status = offSession(d.status) ? d.status : undefined;
  const ext = d.extPrice != null
    ? { label: extendedHoursLabel(t, d.status, 'long'), price: d.extPrice, change: d.extChange, changePct: d.extChangePct }
    : null;
  return (
    <TermCard
      title={d.symbol}
      sub={d.name && <DualName first={d.name} second={narrow ? null : d.secondaryName} />}
      meta={[status && <TermTag key="status">{marketStatusLabel(t, status)}</TermTag>, !narrow && stampEl]}
      onClick={onClick}
    >
      <PriceHero
        figures={{ ...d, ext }}
        freshness={{ tier: d.tier, freshness: d.freshness, asOfLocal: d.asOfLocal, printed: d.printed }}
        extAt="below"
      >
        {hasRange(d) && (
          <div className="term-line term-range-line">
            <span className="term-t3 term-label">{t('toolArtifact.low')}</span>
            <span>{money(d.low)}</span>
            <TermRange low={d.low} high={d.high} value={d.price} fill />
            <span className="term-t3 term-label">{t('toolArtifact.high')}</span>
            <span>{money(d.high)}</span>
          </div>
        )}
      </PriceHero>
      {narrow && stampEl && <StampFooter>{stampEl}</StampFooter>}
      {link && <div className="term-line term-links">{link}</div>}
    </TermCard>
  );
}

function QuoteTable({
  displays,
  asOf,
  asOfTs,
  allLive,
  onClick,
}: {
  displays: QuoteDisplay[];
  asOf?: string;
  asOfTs?: number;
  allLive: boolean;
  onClick?: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const { common, differs } = commonFreshness(displays);

  // The phone keeps sym, name, last and change; the rest needs the width.
  const extRows = displays.filter((d) => d.extPrice != null);
  const showExt = !narrow && extRows.length > 0;
  // One session across the extended prices names the column; a block mixing
  // pre-market and after-hours names each price instead.
  const oneSession = new Set(extRows.map((d) => d.status === 'early_trading')).size === 1;

  // A status every row shares is said once, in the header. Where rows differ,
  // each row off its regular session says its own, unless an extended price
  // on show already names the session; the tag takes the extended column's
  // empty cell when there is one, so the symbols stay narrow.
  const shared = new Set(displays.map((d) => d.status ?? '')).size === 1;
  const sharedStatus = shared && offSession(displays[0].status) ? displays[0].status : undefined;
  const rowStatus = (d: QuoteDisplay): string | undefined =>
    !shared && offSession(d.status) && !(showExt && d.extPrice != null) ? d.status : undefined;
  // A row's own badge carries its venue stamp in the tooltip; a row whose
  // freshness the header speaks for has no badge, so its stamp stays plain.
  const stampOf = (d: QuoteDisplay): string | undefined => (d.asOfLocal && !differs(d) ? d.asOfLocal : undefined);
  const showStamps = !narrow && displays.some(stampOf);
  const showName = displays.some((d) => d.name);
  const money = (n: number, d: QuoteDisplay): string => formatMoney(n, d.currency, locale);

  // The backend stamps a block in the clock its rows share; a mixed block has
  // none, so the reader's own clock stands in.
  const blockStamp = asOf ?? localTitle(asOfTs, locale);
  const stampEl = blockStamp && <span title={asOf ? localTitle(asOfTs, locale) : undefined}>{blockStamp}</span>;
  // A closed venue's rows read "Last close" already; the status would say it twice.
  const sharedTag = sharedStatus === 'closed' && common && resolveFreshnessBadge(common)?.state === 'eod'
    ? undefined
    : sharedStatus;
  const statusTag = (status: string) => <TermTag>{marketStatusLabel(t, status)}</TermTag>;

  const columns: Array<TermColumn<QuoteDisplay>> = [
    {
      key: 'sym',
      label: t('toolArtifact.termHead.sym'),
      fill: 'clip',
      className: 'term-code',
      cell: (d) => {
        const status = rowStatus(d);
        return (
          <>
            <span className="term-strong">{d.symbol}</span>
            {/* Only a row whose freshness differs from the header's keeps a
                mark, or one whose venue time has no column to sit in: the
                mark's tooltip carries it. */}
            {(differs(d) || (!showStamps && !!d.asOfLocal)) && (
              <FreshnessBadge tier={d.tier} freshness={d.freshness} asOfLocal={d.asOfLocal} printed={d.printed} compact className="ml-[2px]" />
            )}
            {status && !showExt && <span className="term-aside">{statusTag(status)}</span>}
          </>
        );
      },
    },
    {
      key: 'name',
      label: t('toolArtifact.termHead.name'),
      fill: 'clip',
      className: 'term-t3',
      show: showName,
      cell: (d) => d.name && <DualName first={d.name} second={narrow ? null : d.secondaryName} />,
    },
    {
      key: 'ext',
      label: oneSession ? extendedHoursLabel(t, extRows[0]?.status, 'long') : t('toolArtifact.termHead.extended'),
      num: true,
      className: 'term-t3',
      show: showExt,
      cell: (d) => {
        if (d.extPrice == null) {
          const status = rowStatus(d);
          return status && statusTag(status);
        }
        return (
          <>
            {!oneSession && <span className="term-label">{extendedHoursLabel(t, d.status, 'short')} </span>}
            <span>{money(d.extPrice, d)}</span>{' '}
            {d.extChangePct != null && <SignedChange value={d.extChangePct} />}
          </>
        );
      },
    },
    {
      key: 'range',
      label: t('toolArtifact.termHead.dayRange'),
      num: true,
      show: !narrow && displays.some(hasRange),
      cell: (d) => <TermRange low={d.low} high={d.high} value={d.price} />,
    },
    {
      key: 'asOf',
      label: t('toolArtifact.termHead.asOf'),
      num: true,
      className: 'term-t3',
      show: showStamps,
      // A time the provider did not print is when the row was fetched.
      title: (d) => {
        const stamp = stampOf(d);
        if (!stamp) return undefined;
        return d.printed ? localTitle(asOfTs, locale) : t('toolArtifact.retrievedAt', { time: stamp });
      },
      cell: stampOf,
    },
    { key: 'last', label: t('toolArtifact.termHead.last'), num: true, cell: (d) => (d.price != null ? money(d.price, d) : '') },
    { key: 'change', label: t('toolArtifact.termHead.change'), num: true, cell: (d) => <TermChange value={d.changePct} /> },
  ];

  return (
    <TermCard
      title={allLive ? t('toolArtifact.liveQuotes') : t('toolArtifact.quotes')}
      meta={[
        sharedTag && statusTag(sharedTag),
        common && <FreshnessBadge key="fresh" tier={common.tier} freshness={common.freshness} source={common.source} />,
        !narrow && stampEl,
      ]}
      onClick={onClick}
    >
      <TermTable rows={displays} columns={columns} rowKey={(d, i) => `${d.symbol}-${i}`} />
      {narrow && stampEl && <StampFooter>{stampEl}</StampFooter>}
    </TermCard>
  );
}

/**
 * Adaptive card for the `get_quote` tool: one symbol renders a hero (big
 * price, day-range strip, stats); two or more render a table with a range
 * strip each. Extended-hours moves split onto their own line or column when
 * the provider supplies them.
 */
export function InlineQuoteCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { displays, asOf, asOfTs, allLive } = readQuotes(artifact);
  if (!displays.length) return null;
  if (displays.length === 1) {
    return <QuoteHero d={displays[0]} asOf={asOf} asOfTs={asOfTs} onClick={onClick} />;
  }
  return <QuoteTable displays={displays} asOf={asOf} asOfTs={asOfTs} allLive={allLive} onClick={onClick} />;
}

/**
 * The quote panel's frames: the card's table when there are several symbols,
 * then each symbol a size up with the figures the table has no room for.
 * `link` names each symbol's way out; the panel module owns the route.
 */
export function QuoteSections({
  artifact,
  link,
}: {
  artifact: Record<string, unknown>;
  link: (symbol: string, isIndex: boolean) => React.ReactNode;
}): React.ReactElement | null {
  const { displays, asOf, asOfTs, allLive } = readQuotes(artifact);
  if (!displays.length) return null;
  return (
    <>
      {displays.length > 1 && <QuoteTable displays={displays} asOf={asOf} asOfTs={asOfTs} allLive={allLive} />}
      {displays.map((d, i) => (
        <QuoteHero
          key={`${d.symbol}-${i}`}
          d={d}
          // The table above says the block's time once; a frame keeps only its own venue's.
          asOf={displays.length > 1 ? undefined : asOf}
          asOfTs={asOfTs}
          link={link(d.symbol, d.isIndex)}
        />
      ))}
    </>
  );
}
