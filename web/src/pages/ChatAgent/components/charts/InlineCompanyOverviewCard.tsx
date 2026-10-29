import React from 'react';
import { useTranslation } from 'react-i18next';
import { formatMoney } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { fixed2 } from '@/lib/format';
import { asQuoteTier } from '@/lib/freshness';
import { deriveOverviewQuote, type CompanyOverviewArtifact } from '@/lib/quotes/overview';
import type { InlineCardProps } from './cardReaders';
import { extendedHoursLabel, marketStatusLabel } from './cardWords';
import { DualName, PriceHero } from './PriceHero';
import { TermCard, TermRange, TermTag } from './TermCard';
import { useCardNarrow } from './termContext';

export function InlineCompanyOverviewCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const narrow = useCardNarrow();
  // A declared wire shape, as the panel and Market View take it: lib/quotes/overview reads it.
  const overview = artifact as CompanyOverviewArtifact;
  if (!overview.quote) return null;

  const symbol = overview.symbol || '';
  const { marketStatus, dualName } = deriveOverviewQuote(overview, symbol);
  // The regular session is the default reading; any other is worth a tag.
  const status = marketStatus && marketStatus !== 'open' ? marketStatus : null;

  return (
    <TermCard
      title={dualName.primary}
      keep={narrow && dualName.named ? 'sub' : 'title'}
      // The ticker first: the shortest and most useful of the three, the
      // second spelling after it gives way before it does. A phone has room
      // for no second spelling, and there the ticker keeps every letter while
      // a long name gives way.
      sub={dualName.named ? <DualName first={symbol} second={narrow ? null : dualName.secondary} /> : undefined}
      meta={status && <TermTag>{marketStatusLabel(t, status)}</TermTag>}
      onClick={onClick}
    >
      <OverviewQuote overview={overview} />
    </TermCard>
  );
}

/**
 * A company's quote as the cards read it: the price and its move, the
 * extended session, the session's figures, and where the price sits in its
 * day's and year's range. The card's body, and the detail panel's first
 * section, so the two read the same.
 */
export function OverviewQuote({ overview }: { overview: CompanyOverviewArtifact }): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  const { quote } = overview;
  if (!quote) return null;

  const q = deriveOverviewQuote(overview, overview.symbol || '');
  const money = (n: number): string => formatMoney(n, q.currency, locale);
  const ranges = [
    quote.dayLow != null && quote.dayHigh != null &&
      ({ label: t('toolArtifact.termRange.day'), low: quote.dayLow, high: quote.dayHigh }),
    quote.yearLow != null && quote.yearHigh != null &&
      ({ label: t('toolArtifact.termRange.week52'), low: quote.yearLow, high: quote.yearHigh }),
  ].filter((r) => !!r);

  return (
    <>
      <PriceHero
        figures={{
          currency: q.currency,
          price: q.displayPrice,
          change: q.displayChange,
          changePct: q.displayChangePct,
          open: quote.open,
          prevClose: quote.previousClose,
          volume: quote.volume,
          ext: q.hasExtPrice && q.extPrice != null
            ? { label: extendedHoursLabel(t, q.marketStatus ?? undefined, 'long'), price: q.extPrice, change: q.extDiff, changePct: q.extDiffPct }
            : null,
        }}
        freshness={{
          tier: asQuoteTier(quote.tier),
          freshness: quote.freshness,
          source: quote.source,
          asOfLocal: quote.as_of_local,
          printed: quote.printed,
        }}
        extAt="end"
        stats={[
          quote.marketCap != null &&
            ([t('toolArtifact.marketCap'), formatMoney(quote.marketCap, q.currency, locale, { compact: true })] as const),
          quote.pe != null && ([t('toolArtifact.peRatio'), fixed2(quote.pe, locale)] as const),
          quote.eps != null && ([t('toolArtifact.eps'), money(quote.eps)] as const),
        ]}
      />
      {ranges.length > 0 && (
        // One grid for both lines, so the strips start and end together.
        <div className="term-ranges">
          {ranges.map((r) => (
            <React.Fragment key={r.label}>
              <span className="term-t3 term-label">{r.label}</span>
              <span className="term-num">{money(r.low)}</span>
              {q.displayPrice != null && r.high > r.low ? (
                <TermRange low={r.low} high={r.high} value={q.displayPrice} fill />
              ) : (
                <span />
              )}
              <span>{money(r.high)}</span>
            </React.Fragment>
          ))}
        </div>
      )}
    </>
  );
}
