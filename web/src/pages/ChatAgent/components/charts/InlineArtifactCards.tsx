import React, { useMemo, useState } from 'react';
import {
  AreaChart, Area, BarChart, Bar, XAxis, YAxis, Cell,
  LabelList,
} from 'recharts';
import { useTranslation } from 'react-i18next';
import { dateStrInTz } from '@/lib/utils';
import { formatMoney, quoteCurrency, resolveCurrency, timezoneForSymbol } from '@/lib/bars';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { grouped2, signedFixed2 } from '@/lib/format';
import { deriveOverviewQuote, type CompanyOverviewArtifact } from '@/lib/quotes/overview';
import { InlineAutomationCard } from './InlineAutomationCards';
import { InlinePreviewCard } from './InlinePreviewCard';
import { InlineChartAnnotationCard } from './InlineChartAnnotationCard';
import { InlineQuoteCard } from './InlineQuoteCard';
import { OrderReceiptCard } from '../mcp/OrderReceiptCard';
import {
  GREEN,
  RED,
  TEXT_COLOR,
  CARD_BORDER,
  SIZES_MOBILE,
  SIZES_DESKTOP,
  cardStyle,
  mobileCardStyle,
  formatPct,
  formatCompactNumber,
  MARKET_STATUS_COLORS,
  extendedHoursLabel,
  marketStatusLabel,
  unwrapMarketOverview,
  type InlineCardProps,
} from './inlineCardsShared';
import { readTypedTicker } from '@/lib/marketUtils';
import { FreshnessBadge } from './FreshnessBadge';
import { asQuoteTier } from '@/lib/freshness';
import type { Freshness } from '@/types/market';

export const INLINE_ARTIFACT_TOOLS = new Set([
  'get_daily_prices',
  'get_stock_daily_prices',
  'get_company_overview',
  'get_quote',
  // Composite tool, renders InlineMarketOverviewCard, which nests the legacy
  // indices + sector cards. Requires the `market_overview` entry in
  // INLINE_ARTIFACT_MAP (ActivityBlock.tsx / MessageList.tsx). Unlike the other
  // cards, InlineMarketOverviewCard never returns null, a completed
  // get_market_overview call routed here always renders (the unknown-region
  // error path falls back to a minimal region card rather than nothing).
  'get_market_overview',
  // Legacy names (pre-consolidation), kept for SSE replay of historical threads
  'get_market_indices',
  'get_sector_performance',
  'get_sec_filing',
  'screen_stocks',
  'check_automations',
  'create_automation',
  'GetPreviewUrl',
  'WebSearch',
  'draw_chart_annotation',
]);

// ─── Helpers ────────────────────────────────────────────────────────

function downsample<T>(arr: T[] | null | undefined, maxPoints = 60): T[] | null | undefined {
  if (!arr || arr.length <= maxPoints) return arr;
  const step = arr.length / maxPoints;
  const result: T[] = [];
  for (let i = 0; i < maxPoints; i++) {
    result.push(arr[Math.floor(i * step)]);
  }
  // Always include last point
  if (result[result.length - 1] !== arr[arr.length - 1]) {
    result.push(arr[arr.length - 1]);
  }
  return result;
}

const ABBREVIATIONS: Record<string, string> = {
  'Consumer Cyclical': 'Cons. Cyclical',
  'Consumer Defensive': 'Cons. Defensive',
  'Communication Services': 'Comm. Services',
  'Financial Services': 'Financial Svcs',
  'Basic Materials': 'Basic Materials',
  'Real Estate': 'Real Estate',
};

function abbreviateSector(name: string): string {
  return ABBREVIATIONS[name] || name;
}

/** Recharts makes its chart surface a tab stop -- `tabindex="0"`,
 *  `role="application"` -- so arrow keys can walk a tooltip's active index.
 *  Neither card below renders a <Tooltip>, so the stop navigates nothing: a
 *  Tab press spent on a picture, announced to assistive tech as an
 *  application. (The stray focus ring these charts used to draw came from
 *  elsewhere and is fixed in styles/tokens.css, not by this flag.) */
const NO_KEYBOARD_LAYER = false;

// ─── InlineStockPriceCard ───────────────────────────────────────────

export function InlineStockPriceCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const { symbol, ohlcv, stats, source, freshness, price_currency: priceCurrency } = (artifact || {}) as {
    symbol?: string;
    ohlcv?: Record<string, unknown>[];
    stats?: Record<string, unknown>;
    /** ISO currency of the series; older artifacts fall back to the suffix. */
    price_currency?: string;
    /** Provider that filled `ohlcv`; absent on pre-contract artifacts. */
    source?: string;
    /** Measured freshness of `ohlcv`, the series this card draws. */
    freshness?: Freshness;
  };

  const sparkData = useMemo(() => {
    if (!ohlcv?.length) return [];
    return (downsample(ohlcv) as Record<string, unknown>[]).map((d) => ({ close: d.close as number }));
  }, [ohlcv]);
  const code = quoteCurrency(priceCurrency, symbol);

  if (!ohlcv?.length) return null;

  const lastClose = ohlcv[ohlcv.length - 1]?.close as number | undefined;
  // On the venue's calendar: a Shanghai or Hong Kong daily bar is stamped at
  // local midnight, which is the previous afternoon in New York.
  const venueTz = timezoneForSymbol(symbol);
  const formatDateLabel = (val: unknown): string => {
    if (typeof val === 'number') return dateStrInTz(val, venueTz);
    return (val as string) || '';
  };
  const firstDate = formatDateLabel(ohlcv[0]?.time ?? ohlcv[0]?.date);
  const lastDate = formatDateLabel(ohlcv[ohlcv.length - 1]?.time ?? ohlcv[ohlcv.length - 1]?.date);
  const changePct = stats?.period_change_pct as number | undefined;
  const isPositive = (changePct ?? 0) >= 0;
  const color = isPositive ? GREEN : RED;
  const gradientId = `sparkGrad-${symbol || 'stock'}`;

  // Period label from date range
  const periodLabel = firstDate && lastDate ? `${firstDate}, ${lastDate}` : '';


  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header row: symbol + price + change. Wraps at phone width, where a
          CN¥ price, the badge and the period outgrow one line. */}
      <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'baseline', columnGap: sz.gap, rowGap: 2, marginBottom: 2 }}>
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? '0.8125rem' : '0.9375rem' }}>{symbol}</span>
        {lastClose != null && (
          <span style={{ color: 'var(--color-text-primary)', fontSize: isMobile ? '0.8125rem' : '0.9375rem', fontWeight: 600 }}>{formatMoney(lastClose, code, locale)}</span>
        )}
        {changePct != null && (
          <span style={{ color, fontSize: isMobile ? '0.6875rem' : '0.8125rem', fontWeight: 600 }}>
            {formatPct(changePct)}
          </span>
        )}
        {/* Publisher and lag of the plotted series live behind this pill; the
            caption it replaces read as a second stats row. */}
        {freshness && <FreshnessBadge freshness={freshness} source={source} chartSymbol={symbol ?? ''} />}
        <span style={{ marginLeft: 'auto', fontSize: sz.labelFs, color: TEXT_COLOR }}>
          {periodLabel}
        </span>
      </div>

      {/* Sparkline */}
      <AreaChart
        responsive
        width="100%"
        height={isMobile ? 48 : 64}
        data={sparkData}
        margin={{ top: 4, right: 2, bottom: 2, left: 2 }}
        accessibilityLayer={NO_KEYBOARD_LAYER}
      >
        <defs>
          <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor={color} stopOpacity={0.3} />
            <stop offset="100%" stopColor={color} stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <YAxis type="number" domain={['dataMin', 'dataMax']} hide />
        <Area
          type="monotone"
          dataKey="close"
          stroke={color}
          strokeWidth={1.5}
          fill={`url(#${gradientId})`}
          dot={false}
          isAnimationActive={false}
        />
      </AreaChart>

      {/* Footer stats */}
      {stats && (
        <div
          style={{
            display: 'flex',
            gap: isMobile ? 8 : 12,
            marginTop: sz.moreMt,
            fontSize: sz.labelFs,
            color: TEXT_COLOR,
            flexWrap: 'wrap',
          }}
        >
          {(stats.period_high as number | undefined) != null && (
            <span>{t('toolArtifact.high')}: {formatMoney(stats.period_high as number, code, locale)}</span>
          )}
          {(stats.period_low as number | undefined) != null && (
            <span>{t('toolArtifact.low')}: {formatMoney(stats.period_low as number, code, locale)}</span>
          )}
          {(stats.avg_volume as number | undefined) != null && (
            <span>{t('toolArtifact.vol')}: {formatCompactNumber(stats.avg_volume as number)}</span>
          )}
        </div>
      )}
    </div>
  );
}

// ─── InlineCompanyOverviewCard ───────────────────────────────────────

export function InlineCompanyOverviewCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const overview = (artifact || {}) as CompanyOverviewArtifact;
  const { quote } = overview;
  if (!quote) return null;

  const symbol = overview.symbol || '';
  const {
    displayPrice, displayChange, displayChangePct,
    marketStatus, extPrice, extDiff, extDiffPct, hasExtPrice,
    currency, dualName,
  } = deriveOverviewQuote(overview, symbol);
  const money = (n: number | null | undefined): string => formatMoney(n, currency, locale);
  const changeColor = (displayChange ?? 0) >= 0 ? GREEN : RED;

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Company name + symbol + market status */}
      <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: sz.sectionMb, flexWrap: 'wrap' }}>
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? '0.875rem' : '1rem', minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {dualName.primary}
        </span>
        {/* The second spelling only where the row has room, as the quote card does. */}
        {dualName.secondary && !isMobile && (
          <span style={{ fontSize: '0.8125rem', color: TEXT_COLOR, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{dualName.secondary}</span>
        )}
        {dualName.named && (
          <span style={{ fontSize: isMobile ? '0.6875rem' : '0.8125rem', color: TEXT_COLOR, flexShrink: 0 }}>{symbol}</span>
        )}
        {marketStatus && (
          <span style={{
            fontSize: sz.badgeFs, fontWeight: 600, padding: '1px 6px', borderRadius: 4,
            color: MARKET_STATUS_COLORS[marketStatus] || TEXT_COLOR,
            border: `1px solid ${MARKET_STATUS_COLORS[marketStatus] || TEXT_COLOR}`,
            whiteSpace: 'nowrap', flexShrink: 0,
          }}>
            {marketStatusLabel(t, marketStatus)}
          </span>
        )}
      </div>

      {/* Regular close price + change; wraps as the quote hero does, so a narrow
          card moves the badge down a line instead of overflowing. */}
      <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'baseline', columnGap: isMobile ? 8 : 10, rowGap: 2, marginBottom: hasExtPrice ? 2 : sz.filingMb }}>
        {displayPrice != null && (
          <span style={{ fontSize: isMobile ? '1.125rem' : '1.375rem', fontWeight: 700, color: 'var(--color-text-primary)' }}>
            {money(displayPrice)}
          </span>
        )}
        {displayChange != null && (
          <span style={{ fontSize: isMobile ? '0.75rem' : '0.875rem', color: changeColor, fontWeight: 500 }}>
            {formatMoney(displayChange, currency, locale, { signed: true })}
            {displayChangePct != null && ` (${signedFixed2(displayChangePct, locale)}%)`}
          </span>
        )}
        <FreshnessBadge
          tier={asQuoteTier(quote.tier)}
          freshness={quote.freshness}
          source={quote.source}
          asOfLocal={quote.as_of_local}
          printed={quote.printed}
        />
      </div>

      {/* Extended-hours price */}
      {hasExtPrice && (
        <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: sz.filingMb, fontSize: isMobile ? '0.6875rem' : '0.8125rem' }}>
          <span style={{ color: TEXT_COLOR }}>
            {extendedHoursLabel(t, marketStatus ?? undefined, 'long')}
          </span>
          <span style={{ fontWeight: 600, color: 'var(--color-text-primary)' }}>
            {money(extPrice)}
          </span>
          <span style={{ color: extDiff >= 0 ? GREEN : RED, fontWeight: 500 }}>
            {formatMoney(extDiff, currency, locale, { signed: true })} ({signedFixed2(extDiffPct, locale)}%)
          </span>
        </div>
      )}

      {/* Key stats grid */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: sz.gridGap,
          fontSize: sz.rowFs,
          color: TEXT_COLOR,
        }}
      >
        {quote.open != null && (
          <QuoteRow label={t('toolArtifact.open')} value={money(quote.open)} />
        )}
        {quote.previousClose != null && (
          <QuoteRow label={t('toolArtifact.prevClose')} value={money(quote.previousClose)} />
        )}
        {quote.dayLow != null && quote.dayHigh != null && (
          <QuoteRow label={t('toolArtifact.dayRange')} value={`${money(quote.dayLow)} - ${money(quote.dayHigh)}`} />
        )}
        {quote.yearLow != null && quote.yearHigh != null && (
          <QuoteRow label={t('toolArtifact.52wRange')} value={`${money(quote.yearLow)} - ${money(quote.yearHigh)}`} />
        )}
        {quote.volume != null && (
          <QuoteRow label={t('toolArtifact.volume')} value={formatCompactNumber(quote.volume)} />
        )}
        {quote.marketCap != null && (
          <QuoteRow label={t('toolArtifact.marketCap')} value={formatMoney(quote.marketCap, currency, locale, { compact: true })} />
        )}
      </div>
    </div>
  );
}

interface QuoteRowProps {
  label: string;
  value: string;
}

function QuoteRow({ label, value }: QuoteRowProps): React.ReactElement {
  const isMobile = useIsMobile();
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: isMobile ? SIZES_MOBILE.rowPad : SIZES_DESKTOP.rowPad }}>
      <span style={{ opacity: 0.7 }}>{label}</span>
      <span style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}>{value}</span>
    </div>
  );
}

// ─── InlineMarketIndicesCard ────────────────────────────────────────

export function InlineMarketIndicesCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const indices = (artifact as Record<string, unknown> | undefined)?.indices as Record<string, Record<string, unknown>> | undefined;
  if (!indices || Object.keys(indices).length === 0) return null;

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      <div style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: sz.headerFs, marginBottom: isMobile ? 4 : 8 }}>
        {t('toolArtifact.marketIndices')}
      </div>
      <div style={{ display: 'flex', flexDirection: 'column', gap: sz.listGap }}>
        {Object.entries(indices).map(([sym, data]) => {
          const ohlcv = data.ohlcv as Record<string, unknown>[] | undefined;
          const lastClose = ohlcv?.[ohlcv.length - 1]?.close as number | undefined;
          const stats = data.stats as Record<string, unknown> | undefined;
          const changePct = stats?.period_change_pct as number | undefined;
          const color = (changePct ?? 0) >= 0 ? GREEN : RED;
          return (
            <div
              key={sym}
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                padding: sz.rowPad,
                fontSize: sz.rowFs,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, minWidth: 0 }}>
                <FreshnessBadge
                  tier={asQuoteTier(data.tier)}
                  freshness={data.freshness as Freshness | undefined}
                  source={data.source as string | undefined}
                  compact
                />
                <span style={{ color: TEXT_COLOR, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', minWidth: 0 }}>{(data.name as string) || sym}</span>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, flexShrink: 0 }}>
                {lastClose != null && (
                  <span style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}>
                    {grouped2(lastClose, locale)}
                  </span>
                )}
                {changePct != null && (
                  <span style={{ color, fontWeight: 500, minWidth: sz.changeMinW, textAlign: 'right' }}>
                    {formatPct(changePct)}
                  </span>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ─── InlineSectorPerformanceCard ────────────────────────────────────

export function InlineSectorPerformanceCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const sectors = (artifact as Record<string, unknown> | undefined)?.sectors as Record<string, unknown>[] | undefined;
  if (!sectors?.length) return null;

  const chartData = sectors
    .slice()
    .sort((a, b) => ((b.changePercentage as number) || 0) - ((a.changePercentage as number) || 0))
    .map((s) => ({
      name: abbreviateSector((s.sector as string) || 'N/A'),
      value: (s.changePercentage as number) || 0,
      fill: ((s.changePercentage as number) || 0) >= 0 ? GREEN : RED,
      label: formatPct((s.changePercentage as number) || 0),
    }));

  const barHeight = isMobile ? 18 : 22;
  const chartHeight = Math.min(chartData.length * barHeight + 20, isMobile ? 220 : 280);

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      <div style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: sz.headerFs, marginBottom: sz.sectionMb }}>
        {t('toolArtifact.sectorPerformance')}
      </div>
      <BarChart
        responsive
        width="100%"
        height={chartHeight}
        data={chartData}
        layout="vertical"
        margin={{ left: 0, right: isMobile ? 40 : 50, top: 0, bottom: 0 }}
        accessibilityLayer={NO_KEYBOARD_LAYER}
      >
        <XAxis type="number" hide />
        <YAxis
          type="category"
          dataKey="name"
          width={isMobile ? 80 : 100}
          tick={{ fill: TEXT_COLOR, fontSize: sz.chartFs }}
          axisLine={false}
          tickLine={false}
        />
        <Bar dataKey="value" radius={[0, 3, 3, 0]} barSize={isMobile ? 11 : 14} isAnimationActive={false}>
          {chartData.map((entry, i) => (
            <Cell key={i} fill={entry.fill} />
          ))}
          <LabelList
            dataKey="label"
            position="right"
            style={{ fill: TEXT_COLOR, fontSize: sz.chartFs }}
          />
        </Bar>
      </BarChart>
    </div>
  );
}

// ─── InlineMarketOverviewCard ───────────────────────────────────────

/**
 * Composite card for the consolidated `get_market_overview` tool. It nests the
 * legacy market_indices / sector_performance artifacts (carried verbatim under
 * `indices` / `sectors`) into their existing cards. InlineMarketOverviewCard
 * must never return null, a completed get_market_overview call routed here
 * would otherwise render nothing, so when neither nested card has data it
 * falls back to a minimal region card.
 */
export function InlineMarketOverviewCard({ artifact, onClick }: InlineCardProps): React.ReactElement {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;

  // hasIndices / hasSectors mirror the nested cards' own null conditions, so the
  // composite knows whether either will render content (and the fallback is needed).
  const { indicesArtifact, sectorsArtifact, hasIndices, hasSectors } = unwrapMarketOverview(artifact);

  if (hasIndices || hasSectors) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', gap: sz.gap }}>
        {hasIndices && <InlineMarketIndicesCard artifact={indicesArtifact} onClick={onClick} />}
        {hasSectors && <InlineSectorPerformanceCard artifact={sectorsArtifact} onClick={onClick} />}
      </div>
    );
  }

  // Error / degenerate path (e.g. unknown region): keep the call visible.
  const region = (artifact as Record<string, unknown> | undefined)?.region as string | undefined;
  const regionLabel = region ? region.toUpperCase() : '';

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap }}>
        <span style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: sz.headerFs }}>
          {t('toolArtifact.marketOverview')}
        </span>
        {regionLabel && (
          <span style={{ fontSize: sz.labelFs, color: TEXT_COLOR }}>{regionLabel}</span>
        )}
      </div>
    </div>
  );
}

// ─── InlineStockScreenerCard ──────────────────────────────────────

export function InlineStockScreenerCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const { results = [], filters = {}, count = 0 } = (artifact || {}) as {
    results?: Record<string, unknown>[];
    filters?: Record<string, unknown>;
    count?: number;
  };
  if (!(results as Record<string, unknown>[]).length) return null;

  const top5 = (results as Record<string, unknown>[]).slice(0, 5);
  const remaining = (count as number) - top5.length;

  // Build compact filter tags
  const filterTags = Object.entries(filters as Record<string, unknown>).map(([k, v]) => `${k}: ${v}`);

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header: title + count badge */}
      <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, marginBottom: sz.sectionMb }}>
        <span style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: sz.headerFs }}>
          {t('toolArtifact.stockScreener')}
        </span>
        <span
          style={{
            fontSize: sz.labelFs,
            color: TEXT_COLOR,
            backgroundColor: 'var(--color-bg-surface)',
            padding: '1px 6px',
            borderRadius: 10,
          }}
        >
          {t('toolArtifact.nResults', { count })}
        </span>
      </div>

      {/* Filter tags */}
      {filterTags.length > 0 && (
        <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginBottom: sz.filingMb }}>
          {filterTags.slice(0, 4).map((tag, i) => (
            <span
              key={i}
              style={{
                fontSize: sz.badgeFs,
                padding: '1px 6px',
                borderRadius: 10,
                backgroundColor: 'var(--color-accent-soft)',
                color: 'var(--color-text-tertiary)',
                border: '1px solid var(--color-border-muted)',
                whiteSpace: 'nowrap',
              }}
            >
              {tag}
            </span>
          ))}
          {filterTags.length > 4 && (
            <span style={{ fontSize: sz.badgeFs, color: TEXT_COLOR }}>+{filterTags.length - 4}</span>
          )}
        </div>
      )}

      {/* Top 5 results */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: isMobile ? 1 : 3 }}>
        {top5.map((stock, i) => {
          const change = stock.changes as number | undefined;
          const changeColor = change != null ? (change >= 0 ? GREEN : RED) : TEXT_COLOR;
          return (
            <div
              key={i}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: sz.gap,
                fontSize: sz.rowFs,
                padding: isMobile ? '1px 0' : '2px 0',
              }}
            >
              <span style={{ color: 'var(--color-text-primary)', fontWeight: 600, flexShrink: 0 }}>
                {stock.symbol as string}
              </span>
              <span
                style={{
                  color: TEXT_COLOR,
                  flex: 1,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {stock.companyName as string}
              </span>
              <span style={{ color: 'var(--color-text-primary)', fontWeight: 500, flexShrink: 0 }}>
                {(stock.price as number | undefined) != null
                  ? formatMoney(stock.price as number, resolveCurrency(null, stock.symbol as string | undefined), locale)
                  : 'N/A'}
              </span>
              {!isMobile && (
                <span style={{ color: TEXT_COLOR, fontSize: '0.6875rem', flexShrink: 0, textAlign: 'right' }}>
                  {(stock.marketCap as number | undefined) != null ? formatCompactNumber(stock.marketCap as number) : ''}
                </span>
              )}
              <span style={{ color: changeColor, fontWeight: 500, flexShrink: 0, minWidth: sz.changeMinW, textAlign: 'right' }}>
                {change != null ? formatPct(change) : ''}
              </span>
            </div>
          );
        })}
      </div>

      {/* +N more */}
      {remaining > 0 && (
        <div style={{ marginTop: sz.moreMt, fontSize: sz.labelFs, color: TEXT_COLOR }}>
          {t('toolArtifact.nMoreStocks', { count: remaining })}
        </div>
      )}
    </div>
  );
}

// ─── InlineSecFilingCard ───────────────────────────────────────────

const ACCENT = 'var(--color-accent-primary)';
const MAX_INLINE_8K = 3;

export function InlineSecFilingCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  if (!artifact || artifact.type !== 'sec_filing') return null;

  if (artifact.filing_type === '8-K') {
    return <Inline8KCard artifact={artifact} onClick={onClick} />;
  }

  return <InlineAnnualQuarterlyCard artifact={artifact} onClick={onClick} />;
}

interface InlineFilingCardProps {
  artifact: Record<string, unknown>;
  onClick?: () => void;
}

function InlineAnnualQuarterlyCard({ artifact, onClick }: InlineFilingCardProps): React.ReactElement {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const { symbol, filing_type, filing_date, period_end, cik, sections_extracted, source_url, has_earnings_call, recent_8k_count } = artifact as {
    symbol?: string;
    filing_type?: string;
    filing_date?: string;
    period_end?: string;
    cik?: string;
    sections_extracted?: number;
    source_url?: string;
    has_earnings_call?: boolean;
    recent_8k_count?: number;
  };

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header: symbol badge + filing type */}
      <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, marginBottom: sz.filingMb }}>
        <span
          style={{
            fontSize: sz.labelFs,
            fontWeight: 700,
            padding: '2px 6px',
            borderRadius: 4,
            backgroundColor: 'var(--color-accent-soft)',
            color: ACCENT,
          }}
        >
          {symbol}
        </span>
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? '0.75rem' : '0.875rem' }}>
          {t('toolArtifact.filing', { type: filing_type })}
        </span>
      </div>

      {/* Metadata grid */}
      <div
        style={{
          display: 'grid',
          gridTemplateColumns: '1fr 1fr',
          gap: sz.gridGap,
          fontSize: sz.rowFs,
          color: TEXT_COLOR,
        }}
      >
        {filing_date && <QuoteRow label={t('toolArtifact.filingDate')} value={filing_date} />}
        {period_end && <QuoteRow label={t('toolArtifact.periodEnd')} value={period_end} />}
        {cik && <QuoteRow label={t('toolArtifact.cik')} value={cik} />}
        {sections_extracted != null && <QuoteRow label={t('toolArtifact.sections')} value={String(sections_extracted)} />}
        {has_earnings_call && <QuoteRow label={t('toolArtifact.earningsCall')} value={t('toolArtifact.included')} />}
        {recent_8k_count != null && <QuoteRow label={t('toolArtifact.recent8Ks')} value={String(recent_8k_count)} />}
      </div>

      {/* EDGAR link */}
      {source_url && (
        <div style={{ marginTop: sz.filingMb }}>
          <a
            href={source_url}
            target="_blank"
            rel="noopener noreferrer"
            onClick={(e) => e.stopPropagation()}
            style={{ fontSize: sz.labelFs, color: ACCENT, textDecoration: 'none' }}
          >
            {t('toolArtifact.viewOnEdgar')}
          </a>
        </div>
      )}
    </div>
  );
}

function Inline8KCard({ artifact, onClick }: InlineFilingCardProps): React.ReactElement {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const { symbol, filing_count, filings = [] } = artifact as {
    symbol?: string;
    filing_count?: number;
    filings?: Record<string, unknown>[];
  };
  const shown = (filings as Record<string, unknown>[]).slice(0, MAX_INLINE_8K);
  const remaining = (filing_count as number) - shown.length;

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header: symbol badge + title + count */}
      <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, marginBottom: sz.filingMb }}>
        <span
          style={{
            fontSize: sz.labelFs,
            fontWeight: 700,
            padding: '2px 6px',
            borderRadius: 4,
            backgroundColor: 'var(--color-accent-soft)',
            color: ACCENT,
          }}
        >
          {symbol}
        </span>
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? '0.75rem' : '0.875rem' }}>{t('toolArtifact.8kFilings')}</span>
        <span
          style={{
            fontSize: sz.labelFs,
            color: TEXT_COLOR,
            backgroundColor: 'var(--color-bg-surface)',
            padding: '1px 6px',
            borderRadius: 10,
          }}
        >
          {filing_count}
        </span>
      </div>

      {/* Compact filing list */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: sz.listGap }}>
        {shown.map((f, i) => (
          <div
            key={i}
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: sz.gap,
              fontSize: sz.rowFs,
              padding: sz.rowPad,
            }}
          >
            <span style={{ color: 'var(--color-text-primary)', fontWeight: 500, flexShrink: 0 }}>{f.filing_date as string}</span>
            <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', flex: 1, overflow: 'hidden' }}>
              {((f.items as string[]) || []).slice(0, 2).map((item, j) => (
                <span
                  key={j}
                  style={{
                    fontSize: sz.badgeFs,
                    padding: '1px 6px',
                    borderRadius: 10,
                    backgroundColor: 'var(--color-accent-soft)',
                    color: 'var(--color-text-tertiary)',
                    border: '1px solid var(--color-border-muted)',
                    whiteSpace: 'nowrap',
                  }}
                >
                  {item}
                </span>
              ))}
              {((f.items as string[]) || []).length > 2 && (
                <span style={{ fontSize: sz.badgeFs, color: TEXT_COLOR }}>+{(f.items as string[]).length - 2}</span>
              )}
            </div>
            {!!f.has_press_release && (
              <span style={{ fontSize: sz.badgeFs, color: GREEN, flexShrink: 0 }}>PR</span>
            )}
          </div>
        ))}
      </div>

      {/* +N more */}
      {remaining > 0 && (
        <div style={{ marginTop: sz.moreMt, fontSize: sz.labelFs, color: TEXT_COLOR }}>
          {t('toolArtifact.nMoreFilings', { count: remaining })}
        </div>
      )}
    </div>
  );
}

// ─── Shared favicon helper ──────────────────────────────────────────

/** Build a Google favicon service URL for the given domain. Returns '' if domain is empty. */
export function googleFaviconUrl(domain: string): string {
  return domain ? `https://www.google.com/s2/favicons?domain=${domain}&sz=32` : '';
}

/** Favicon <img> with onError fallback to a monogram span. */
export function FaviconImg({ src, domain, size = 14 }: { src: string; domain: string; size?: number }): React.ReactElement {
  const [failed, setFailed] = useState(false);

  if (failed || !src) {
    return (
      <span
        style={{
          width: size,
          height: size,
          borderRadius: 2,
          backgroundColor: 'var(--color-bg-surface)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: Math.round(size * 0.64),
          fontWeight: 600,
          color: TEXT_COLOR,
          flexShrink: 0,
        }}
      >
        {(domain || '?')[0].toUpperCase()}
      </span>
    );
  }

  return (
    <img
      src={src}
      alt=""
      width={size}
      height={size}
      style={{ borderRadius: 2, flexShrink: 0 }}
      onError={() => setFailed(true)}
    />
  );
}

// ─── InlineWebSearchCard ──────────────────────────────────────────

interface WebSearchResult {
  title?: string;
  url?: string;
  favicon?: string;
  snippet?: string;
}

/** Extract domain from a URL, stripping www. prefix. */
function extractDomain(url: string): string {
  try {
    return new URL(url).hostname.replace(/^www\./, '');
  } catch {
    return '';
  }
}

/** Resolve favicon URL: use provided value or fall back to Google favicon service. */
function resolveFavicon(result: WebSearchResult): string {
  if (result.favicon) return result.favicon;
  return googleFaviconUrl(extractDomain(result.url || ''));
}

const MAX_INLINE_RESULTS = 4;

export function InlineWebSearchCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;

  const query = (artifact?.query as string) || '';
  const results = (artifact?.results as WebSearchResult[] | undefined) || [];

  if (!results.length) return null;

  const shown = results.slice(0, MAX_INLINE_RESULTS);
  const remaining = results.length - shown.length;

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header: search icon + query + count badge */}
      <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, marginBottom: sz.sectionMb }}>
        <svg
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          strokeLinecap="round"
          strokeLinejoin="round"
          style={{ width: isMobile ? 13 : 15, height: isMobile ? 13 : 15, flexShrink: 0, color: TEXT_COLOR }}
        >
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>
        <span
          style={{
            fontWeight: 600,
            color: 'var(--color-text-primary)',
            fontSize: sz.headerFs,
            flex: 1,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {query}
        </span>
        <span
          style={{
            fontSize: sz.labelFs,
            color: TEXT_COLOR,
            backgroundColor: 'var(--color-bg-surface)',
            padding: '1px 6px',
            borderRadius: 10,
            flexShrink: 0,
          }}
        >
          {results.length} results
        </span>
      </div>

      {/* Result rows */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: sz.listGap }}>
        {shown.map((result, i) => {
          const domain = extractDomain(result.url || '');
          const faviconSrc = resolveFavicon(result);
          return (
            <div
              key={i}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: sz.gap,
                fontSize: sz.rowFs,
                padding: sz.rowPad,
              }}
            >
              <FaviconImg src={faviconSrc} domain={domain} />
              <span
                style={{
                  color: 'var(--color-text-primary)',
                  fontWeight: 500,
                  flex: 1,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}
              >
                {result.title || domain}
              </span>
              <span
                style={{
                  color: TEXT_COLOR,
                  fontSize: sz.labelFs,
                  flexShrink: 0,
                  opacity: 0.7,
                }}
              >
                {domain}
              </span>
            </div>
          );
        })}
      </div>

      {/* +N more results */}
      {remaining > 0 && (
        <div style={{ marginTop: sz.moreMt, fontSize: sz.labelFs, color: TEXT_COLOR }}>
          +{remaining} more results
        </div>
      )}
    </div>
  );
}

// ─── Artifact-type dispatch ─────────────────────────────────────────

/**
 * Maps an artifact `type` to its inline card component. Single source of truth
 * for both the activity timeline (ActivityBlock) and the message list
 * (MessageList), a new inline card is registered here (plus its tool-name gate
 * in INLINE_ARTIFACT_TOOLS) rather than in each surface separately.
 */
export const INLINE_ARTIFACT_MAP: Record<
  string,
  React.ComponentType<{ artifact: Record<string, unknown>; onClick?: () => void }>
> = {
  stock_prices: InlineStockPriceCard,
  company_overview: InlineCompanyOverviewCard,
  quote: InlineQuoteCard,
  market_indices: InlineMarketIndicesCard,
  sector_performance: InlineSectorPerformanceCard,
  market_overview: InlineMarketOverviewCard,
  sec_filing: InlineSecFilingCard,
  stock_screener: InlineStockScreenerCard,
  automations: InlineAutomationCard,
  preview_url: InlinePreviewCard,
  web_search: InlineWebSearchCard,
  chart_annotation: InlineChartAnnotationCard,
  order_receipt: OrderReceiptCard,
};

/**
 * The compact cards whose data is a price, and so read better as the live
 * chart. An overview is prose and fundamentals: the chart is a neighbour of
 * that, not a bigger view of it, so its card opens the result itself.
 */
const CHART_CARD_TYPES = new Set(['quote', 'stock_prices']);

/** The symbol whose live chart a card opens, or null for a card about no one stock. */
export function chartSymbolOf(artifact: Record<string, unknown> | null | undefined): string | null {
  if (!artifact || !CHART_CARD_TYPES.has(artifact.type as string)) return null;
  // A symbol no chart tab would take falls back to the result, rather than
  // opening the panel on nothing.
  return typeof artifact.symbol === 'string' ? readTypedTicker(artifact.symbol) : null;
}

/**
 * What a click on a card does: a card about one stock opens that stock's live
 * chart, and every other card falls back to the raw tool result. Either way the
 * tool-call row beside the card stays the way to the result itself.
 */
export function openCardTarget(
  artifact: Record<string, unknown> | null | undefined,
  onOpenChart: ((spec: { symbol: string }) => void) | null | undefined,
  fallback: () => void,
): void {
  const symbol = chartSymbolOf(artifact);
  if (symbol && onOpenChart) onOpenChart({ symbol });
  else fallback();
}

/**
 * Whether a completed tool call has an artifact this build draws a card for.
 *
 * The artifact's own type is asked first, which is what lets a tool nobody can
 * enumerate register a card: a direct MCP tool is named
 * `mcp__<server>__<tool>`, one name per user per connection, so the name set
 * below could never hold it. The name set stays because it is also the gate a
 * tool passes before its artifact has arrived.
 */
export function isInlineArtifactReady(
  toolName: string | null | undefined,
  artifact: unknown,
): boolean {
  if (!artifact || typeof artifact !== 'object') return false;
  const type = (artifact as { type?: unknown }).type;
  if (typeof type === 'string' && INLINE_ARTIFACT_MAP[type]) return true;
  return INLINE_ARTIFACT_TOOLS.has(toolName || '');
}

