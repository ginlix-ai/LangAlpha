import React from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney } from '@/lib/bars';
import { compactNumber } from '@/lib/format';
import { cn } from '@/lib/utils';
import { FreshnessBadge, type FreshnessBadgeProps } from './FreshnessBadge';
import { TermChange, TermKV } from './TermCard';
import { useCardNarrow } from './termContext';

/** Both spellings of a name: the second, where the width has room for it, after a space. */
export function DualName({ first, second }: { first: React.ReactNode; second?: string | null }): React.ReactElement {
  return (
    <>
      <span>{first}</span>
      {second && <span className="term-aside">{second}</span>}
    </>
  );
}

/** A price as a quote reads it, every figure already worked out. */
export interface PriceFigures {
  /** What the money prints in; null for an index, whose level is points. */
  currency: string | null;
  price?: number | null;
  change?: number | null;
  changePct?: number | null;
  open?: number | null;
  prevClose?: number | null;
  volume?: number | null;
  /** The extended session's last trade and its move off the close. */
  ext?: { label: string; price: number; change?: number | null; changePct?: number | null } | null;
}

type Pair = readonly [label: React.ReactNode, value: React.ReactNode];

/**
 * A quote's price, big, with its move and how current it is; the extended
 * session's; and the session's open, close and volume in columns. The quote
 * card and the company card both lead with it. The quote card sets the
 * extended session on a line of its own `below` the price; the company card at
 * the price line's `end`, or on its own line where a phone has no room.
 */
export function PriceHero({
  figures,
  freshness,
  extAt,
  stats = [],
  children,
}: {
  figures: PriceFigures;
  freshness: FreshnessBadgeProps;
  extAt: 'below' | 'end';
  /** The figures after the session's own: a company's size and earnings. */
  stats?: ReadonlyArray<Pair | null | false | undefined>;
  /** What reads off the price before the session's figures: its day's range. */
  children?: React.ReactNode;
}): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const { currency, ext } = figures;
  const money = (n: number): string => formatMoney(n, currency, locale);
  const signed = (n: number): string => formatMoney(n, currency, locale, { signed: true });
  const pairs = [
    figures.open != null && ([t('toolArtifact.open'), money(figures.open)] as const),
    figures.prevClose != null && ([t('toolArtifact.prevClose'), money(figures.prevClose)] as const),
    // In the reader's locale: a Chinese reader counts in 万 and 亿.
    figures.volume != null && ([t('toolArtifact.volume'), compactNumber(figures.volume, locale)] as const),
    ...stats,
  ].filter((p) => !!p);
  const extAside = ext && extAt === 'end' && (
    <span className={cn('term-t3', !narrow && 'term-push')}>
      <span className="term-label">{ext.label}</span>{' '}
      <span className="term-ink">{money(ext.price)}</span>{' '}
      <TermChange value={ext.changePct} />
      {ext.change != null && <>{' '}<span className="term-t2">{signed(ext.change)}</span></>}
    </span>
  );
  return (
    <>
      <div className="term-line">
        {figures.price != null && <span className="term-big term-hero">{money(figures.price)}</span>}
        <TermChange value={figures.changePct} />
        {figures.change != null && <span className="term-t2">{signed(figures.change)}</span>}
        <FreshnessBadge {...freshness} />
        {!narrow && extAside}
      </div>
      {narrow && extAside && <div className="term-line">{extAside}</div>}
      {ext && extAt === 'below' && (
        <div className="term-line">
          <span>
            <span className="term-t3 term-label">{ext.label}</span>{' '}
            <span>{money(ext.price)}</span>
          </span>
          <TermChange value={ext.changePct} />
          {ext.change != null && <span className="term-t2">{signed(ext.change)}</span>}
        </div>
      )}
      {children}
      {pairs.length > 0 && (
        <div className="term-figures">
          {/* Three to a row needs the desktop width; a phone's labels would cut their values. */}
          <TermKV pairs={pairs} cols={narrow ? 2 : 3} />
        </div>
      )}
    </>
  );
}
