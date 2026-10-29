import React, { memo, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { compactNumberFixed2, grouped } from '@/lib/format';
import { deriveOverviewQuote, type CompanyOverviewArtifact, type ShortInterest } from '@/lib/quotes/overview';
import { useLocale } from '@/hooks/useLocale';
import { CashFlowChart, EarningsSurpriseChart, MarginsChart, QuarterlyRevenueChart, RevenueBreakdownChart } from './CompanyCharts';
import { ratingCounts } from './cardReaders';
import { formatPct, marketStatusLabel, performancePoints } from './cardWords';
import { OverviewQuote } from './InlineCompanyOverviewCard';
import { MarketLink } from './marketLinks';
import { TermBars, TermCard, TermKV, TermTag } from './TermCard';
import { TermPanelHead } from './TermPanel';

/** Defers rendering until the element scrolls into view (with 200px pre-load margin) */
function LazyChart({ height, children, root }: { height: number; children: React.ReactNode; root?: React.RefObject<HTMLDivElement | null> }): React.ReactElement {
  const ref = useRef<HTMLDivElement>(null);
  const [visible, setVisible] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => { if (entry.isIntersecting) { setVisible(true); observer.disconnect(); } },
      { root: root?.current ?? null, rootMargin: '200px' },
    );
    observer.observe(el);
    return () => observer.disconnect();
  // eslint-disable-next-line react-hooks/exhaustive-deps -- root is a stable ref object, observer is one-shot
  }, []);

  if (!visible) return <div ref={ref} style={{ minHeight: height, width: '100%' }} />;
  return <>{children}</>;
}

interface CompanyOverviewCardProps {
  data: CompanyOverviewArtifact;
  scrollContainerRef?: React.RefObject<HTMLDivElement | null>;
}

export const CompanyOverviewCard = memo(function CompanyOverviewCard({ data, scrollContainerRef }: CompanyOverviewCardProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const {
    quote, performance, analystRatings,
    revenueByProduct, revenueByGeo,
    quarterlyFundamentals, earningsSurprises, cashFlow,
    float: floatObj, shortInterest, shortVolume: shortVolumeObj,
  } = data;
  const symbol = data.symbol || '';
  const { marketStatus, statementCurrency, earningsCurrency, dualName } = deriveOverviewQuote(data, symbol);
  // The regular session is the default reading; any other is worth a tag.
  const status = marketStatus && marketStatus !== 'open' ? marketStatus : null;

  // Float / short interest / short volume
  const hasFloat = floatObj && typeof floatObj === 'object' && floatObj.free_float != null;
  // shortInterest: single object (new) or array (legacy backward compat)
  const latestSI: ShortInterest | null = Array.isArray(shortInterest)
    ? (shortInterest.length ? shortInterest[shortInterest.length - 1] : null)
    : (shortInterest || null);
  const hasSI = latestSI && latestSI.short_interest != null;
  const siPctOfFloat = (hasSI && hasFloat && floatObj!.free_float)
    ? (latestSI!.short_interest! / floatObj!.free_float * 100) : null;
  const hasSV = shortVolumeObj && typeof shortVolumeObj === 'object' && shortVolumeObj.short_volume_ratio != null;
  const performanceRows = performancePoints(performance).map((d) => ({ label: d.name, value: d.value, text: formatPct(d.value) }));

  return (
    <>
      <TermPanelHead
        title={dualName.named ? symbol : dualName.primary}
        sub={dualName.named ? [dualName.primary, dualName.secondary].filter(Boolean).join(' · ') : undefined}
        aside={<MarketLink symbol={data.symbol} />}
      />

      {/* The card this panel opened from, a size up. */}
      {quote && (
        <TermCard
          title={t('toolArtifact.quote')}
          meta={status ? <TermTag>{marketStatusLabel(t, status)}</TermTag> : undefined}
        >
          <OverviewQuote overview={data} />
        </TermCard>
      )}

      {(hasFloat || hasSI || hasSV) && (
        <TermCard title={t('toolArtifact.shareStructure', 'Share Structure')}>
          <TermKV
            cols={2}
            pairs={[
              hasFloat && [t('toolArtifact.float', 'Float'), compactNumberFixed2(floatObj!.free_float!, locale)],
              hasFloat && floatObj!.free_float_percent != null &&
                [t('toolArtifact.floatPct', 'Float %'), `${floatObj!.free_float_percent.toFixed(1)}%`],
              hasSI && [
                `${t('toolArtifact.shortInterest', 'Short Interest')}${latestSI!.settlement_date ? ` (${latestSI!.settlement_date})` : ''}`,
                grouped(latestSI!.short_interest!, locale),
              ],
              siPctOfFloat != null && [t('toolArtifact.shortPctFloat', 'SI % of Float'), `${siPctOfFloat.toFixed(2)}%`],
              latestSI?.days_to_cover != null && [t('toolArtifact.daysToCover', 'Days to Cover'), latestSI.days_to_cover.toFixed(2)],
              hasSV && [
                `${t('toolArtifact.shortVolRatio', 'Short Vol Ratio')}${shortVolumeObj!.date ? ` (${shortVolumeObj!.date})` : ''}`,
                `${shortVolumeObj!.short_volume_ratio!.toFixed(1)}%`,
              ],
            ]}
          />
        </TermCard>
      )}

      {/* A period's move drawn as the cards draw a sector's. */}
      {performanceRows.length > 0 && (
        <TermCard title={t('toolArtifact.pricePerformance')}>
          <TermBars rows={performanceRows} />
        </TermCard>
      )}
      {analystRatings && <RatingsBars ratings={analystRatings} />}

      {/* Quarterly Revenue & Net Income + Profit Margins (same data source) */}
      {!!quarterlyFundamentals?.length && (
        <>
          <LazyChart height={250} root={scrollContainerRef}>
            <QuarterlyRevenueChart data={quarterlyFundamentals} currency={statementCurrency} />
          </LazyChart>
          <LazyChart height={250} root={scrollContainerRef}>
            <MarginsChart data={quarterlyFundamentals} />
          </LazyChart>
        </>
      )}

      {!!earningsSurprises?.length && (
        <LazyChart height={250} root={scrollContainerRef}>
          <EarningsSurpriseChart data={earningsSurprises} currency={earningsCurrency} />
        </LazyChart>
      )}

      {!!cashFlow?.length && (
        <LazyChart height={250} root={scrollContainerRef}>
          <CashFlowChart data={cashFlow} currency={statementCurrency} />
        </LazyChart>
      )}

      {(revenueByProduct && Object.keys(revenueByProduct).length > 0) ||
       (revenueByGeo && Object.keys(revenueByGeo).length > 0) ? (
        <LazyChart height={220} root={scrollContainerRef}>
          <RevenueBreakdownChart revenueByProduct={revenueByProduct} revenueByGeo={revenueByGeo} currency={statementCurrency} />
        </LazyChart>
      ) : null}
    </>
  );
});

const RATING_TONE: Record<string, 'up' | 'down' | undefined> = {
  strongBuy: 'up', buy: 'up', sell: 'down', strongSell: 'down',
};

/** Every rating counted, a zero too: none rate it a sell is news. Nothing to
 *  draw when no one rates it. */
function RatingsBars({ ratings }: { ratings: Record<string, unknown> }): React.ReactElement | null {
  const { t } = useTranslation();
  const rows = ratingCounts(ratings);
  const total = rows.reduce((s, r) => s + Math.max(r.value, 0), 0);
  if (!total) return null;
  const max = Math.max(...rows.map((r) => r.value)) || 1;
  const consensus = ratings.consensus as string | undefined;
  return (
    <TermCard
      title={t('toolArtifact.analystRatings')}
      meta={
        <span style={{ display: 'inline-flex', alignItems: 'baseline', gap: '1.25ch' }}>
          {consensus && <TermTag>{consensus}</TermTag>}
          <span>{t('toolArtifact.nRatings', { count: total })}</span>
        </span>
      }
    >
      <div className="term-hbars">
        {rows.map((r) => (
          <React.Fragment key={r.key}>
            <span className="term-hbars-label">{t(`toolArtifact.ratings.${r.key}`)}</span>
            <span className="term-hbars-track">
              <span
                className="term-hbar"
                data-tone={RATING_TONE[r.key]}
                data-weak={r.key === 'buy' || r.key === 'sell' ? '' : undefined}
                style={{ width: `${(r.value / max) * 100}%` }}
              />
            </span>
            <span className="term-num">{r.value}</span>
            <span className="term-num term-t3">{Math.round((r.value / total) * 100)}%</span>
          </React.Fragment>
        ))}
      </div>
    </TermCard>
  );
}
