import React, { useMemo, useRef, useState, useEffect } from 'react';
import {
  AreaChart, Area, BarChart, Bar, XAxis, YAxis, Cell,
  LabelList,
} from 'recharts';
import { useTranslation } from 'react-i18next';
import { utcMsToETDate } from '@/lib/utils';
import { useIsMobile } from '@/hooks/useIsMobile';

// ─── Constants ──────────────────────────────────────────────────────

const GREEN = 'var(--color-profit)';
const RED = 'var(--color-loss)';
const TEXT_COLOR = 'var(--color-text-tertiary)';
const CARD_BG = 'var(--color-bg-tool-card)';
const CARD_BORDER = 'var(--color-border-muted)';

export const INLINE_ARTIFACT_TOOLS = new Set([
  'get_stock_daily_prices',
  'get_company_overview',
  'get_market_indices',
  'get_sector_performance',
  'get_sec_filing',
  'screen_stocks',
  'check_automations',
  'create_automation',
  'GetPreviewUrl',
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

function formatCompactNumber(num: number | null | undefined): string {
  if (num == null) return 'N/A';
  if (Math.abs(num) >= 1e9) return `${(num / 1e9).toFixed(1)}B`;
  if (Math.abs(num) >= 1e6) return `${(num / 1e6).toFixed(1)}M`;
  if (Math.abs(num) >= 1e3) return `${(num / 1e3).toFixed(1)}K`;
  return typeof num === 'number' ? num.toFixed(2) : String(num);
}

function formatPct(val: number | null | undefined): string {
  if (val == null) return 'N/A';
  const sign = val >= 0 ? '+' : '';
  return `${sign}${val.toFixed(2)}%`;
}

const cardStyle: React.CSSProperties = {
  background: CARD_BG,
  border: `1px solid ${CARD_BORDER}`,
  borderRadius: 8,
  padding: '12px 14px',
  cursor: 'pointer',
  transition: 'border-color 0.15s',
  outline: 'none',
  WebkitTapHighlightColor: 'transparent',
  userSelect: 'none',
};

const mobileCardStyle: React.CSSProperties = {
  ...cardStyle,
  padding: '8px 10px',
  borderRadius: 6,
};

/** Shared mobile / desktop sizing tokens for inline cards */
const SIZES_MOBILE = {
  gap: 6, listGap: 2, gridGap: '1px 12px',
  headerFs: 12, rowFs: 11, labelFs: 10, badgeFs: 9,
  rowPad: '2px 0', sectionMb: 4, filingMb: 6,
  moreMt: 2, changeMinW: 48,
} as const;
const SIZES_DESKTOP = {
  gap: 8, listGap: 4, gridGap: '2px 20px',
  headerFs: 13, rowFs: 12, labelFs: 11, badgeFs: 10,
  rowPad: '3px 0', sectionMb: 6, filingMb: 8,
  moreMt: 4, changeMinW: 55,
} as const;

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

/** Measures container width via ResizeObserver and passes it to children as a render prop.
 *  Replaces ResponsiveContainer to avoid the Recharts width(-1) warning on first render. */
function MeasuredContainer({ height, children }: { height: number; children: (width: number) => React.ReactNode }): React.ReactElement {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([entry]) => {
      const w = Math.round(entry.contentRect.width);
      setWidth((prev) => (prev !== w ? w : prev));
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  return (
    <div ref={ref} style={{ width: '100%', height, minWidth: 1 }}>
      {width > 0 && children(width)}
    </div>
  );
}

// ─── InlineStockPriceCard ───────────────────────────────────────────

interface InlineCardProps {
  artifact: Record<string, unknown> | null | undefined;
  onClick?: () => void;
}

export function InlineStockPriceCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const { symbol, ohlcv, stats } = (artifact || {}) as {
    symbol?: string;
    ohlcv?: Record<string, unknown>[];
    stats?: Record<string, unknown>;
  };

  const sparkData = useMemo(() => {
    if (!ohlcv?.length) return [];
    return (downsample(ohlcv) as Record<string, unknown>[]).map((d) => ({ close: d.close as number }));
  }, [ohlcv]);

  if (!ohlcv?.length) return null;

  const lastClose = ohlcv[ohlcv.length - 1]?.close as number | undefined;
  const formatDateLabel = (val: unknown): string => {
    if (typeof val === 'number') return utcMsToETDate(val);
    return (val as string) || '';
  };
  const firstDate = formatDateLabel(ohlcv[0]?.time ?? ohlcv[0]?.date);
  const lastDate = formatDateLabel(ohlcv[ohlcv.length - 1]?.time ?? ohlcv[ohlcv.length - 1]?.date);
  const changePct = stats?.period_change_pct as number | undefined;
  const isPositive = (changePct ?? 0) >= 0;
  const color = isPositive ? GREEN : RED;
  const gradientId = `sparkGrad-${symbol || 'stock'}`;

  // Period label from date range
  const periodLabel = firstDate && lastDate ? `${firstDate} — ${lastDate}` : '';

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Header row: symbol + price + change */}
      <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: 2 }}>
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? 13 : 15 }}>{symbol}</span>
        {lastClose != null && (
          <span style={{ color: 'var(--color-text-primary)', fontSize: isMobile ? 13 : 15, fontWeight: 600 }}>${lastClose.toFixed(2)}</span>
        )}
        {changePct != null && (
          <span style={{ color, fontSize: isMobile ? 11 : 13, fontWeight: 600 }}>
            {formatPct(changePct)}
          </span>
        )}
        <span style={{ marginLeft: 'auto', fontSize: sz.labelFs, color: TEXT_COLOR }}>
          {periodLabel}
        </span>
      </div>

      {/* Sparkline */}
      <MeasuredContainer height={isMobile ? 48 : 64}>
        {(w) => (
          <AreaChart width={w} height={isMobile ? 48 : 64} data={sparkData} margin={{ top: 4, right: 2, bottom: 2, left: 2 }}>
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
        )}
      </MeasuredContainer>

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
            <span>{t('toolArtifact.high')}: ${(stats.period_high as number).toFixed(2)}</span>
          )}
          {(stats.period_low as number | undefined) != null && (
            <span>{t('toolArtifact.low')}: ${(stats.period_low as number).toFixed(2)}</span>
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

function formatMarketCap(num: number | null | undefined): string {
  if (num == null) return 'N/A';
  if (Math.abs(num) >= 1e12) return `$${(num / 1e12).toFixed(2)}T`;
  if (Math.abs(num) >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
  if (Math.abs(num) >= 1e6) return `$${(num / 1e6).toFixed(2)}M`;
  return `$${num.toLocaleString()}`;
}

const MARKET_STATUS_LABELS: Record<string, string> = {
  early_trading: 'Pre-Market',
  open: 'Regular',
  late_trading: 'After-Hours',
  closed: 'Closed',
};
const MARKET_STATUS_COLORS: Record<string, string> = {
  early_trading: '#f59e0b',
  open: GREEN,
  late_trading: '#3b82f6',
  closed: TEXT_COLOR,
};

export function InlineCompanyOverviewCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const { symbol, name, quote } = (artifact || {}) as {
    symbol?: string;
    name?: string;
    quote?: Record<string, unknown>;
  };
  if (!quote) return null;

  // Resolve display price: snapshot -> regularClose, FMP fallback -> price
  const displayPrice = (quote.regularClose ?? quote.price) as number | undefined;
  const displayChange = (quote.regularChange ?? quote.change) as number | undefined;
  const displayChangePct = (quote.regularChangePct ?? quote.changePct) as number | undefined;
  const changeColor = (displayChange ?? 0) >= 0 ? GREEN : RED;

  // Extended hours
  const marketStatus = quote.marketStatus as string | undefined;
  const isExtended = marketStatus === 'early_trading' || marketStatus === 'late_trading';
  const extPrice = quote.lastTradePrice as number | undefined;
  const hasExtPrice = isExtended && extPrice != null && displayPrice != null && extPrice !== displayPrice;
  const extDiff = hasExtPrice ? extPrice - displayPrice : 0;
  const extDiffPct = hasExtPrice && displayPrice ? (extDiff / displayPrice * 100) : 0;

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {/* Company name + symbol + market status */}
      <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: sz.sectionMb, flexWrap: 'wrap' }}>
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? 14 : 16, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
          {name || symbol}
        </span>
        {name && (
          <span style={{ fontSize: isMobile ? 11 : 13, color: TEXT_COLOR, flexShrink: 0 }}>{symbol}</span>
        )}
        {marketStatus && (
          <span style={{
            fontSize: sz.badgeFs, fontWeight: 600, padding: '1px 6px', borderRadius: 4,
            color: MARKET_STATUS_COLORS[marketStatus] || TEXT_COLOR,
            border: `1px solid ${MARKET_STATUS_COLORS[marketStatus] || TEXT_COLOR}`,
            whiteSpace: 'nowrap', flexShrink: 0,
          }}>
            {MARKET_STATUS_LABELS[marketStatus] || marketStatus}
          </span>
        )}
      </div>

      {/* Regular close price + change */}
      <div style={{ display: 'flex', alignItems: 'baseline', gap: isMobile ? 8 : 10, marginBottom: hasExtPrice ? 2 : sz.filingMb }}>
        {displayPrice != null && (
          <span style={{ fontSize: isMobile ? 18 : 22, fontWeight: 700, color: 'var(--color-text-primary)' }}>
            ${displayPrice.toFixed(2)}
          </span>
        )}
        {displayChange != null && (
          <span style={{ fontSize: isMobile ? 12 : 14, color: changeColor, fontWeight: 500 }}>
            {displayChange >= 0 ? '+' : ''}{displayChange.toFixed(2)} ({displayChangePct?.toFixed(2)}%)
          </span>
        )}
      </div>

      {/* Extended-hours price */}
      {hasExtPrice && (
        <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: sz.filingMb, fontSize: isMobile ? 11 : 13 }}>
          <span style={{ color: TEXT_COLOR }}>
            {marketStatus === 'early_trading' ? 'Pre-Mkt' : 'After-Hrs'}
          </span>
          <span style={{ fontWeight: 600, color: 'var(--color-text-primary)' }}>
            ${extPrice.toFixed(2)}
          </span>
          <span style={{ color: extDiff >= 0 ? GREEN : RED, fontWeight: 500 }}>
            {extDiff >= 0 ? '+' : ''}{extDiff.toFixed(2)} ({extDiffPct >= 0 ? '+' : ''}{extDiffPct.toFixed(2)}%)
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
        {(quote.open as number | undefined) != null && (
          <QuoteRow label={t('toolArtifact.open')} value={`$${(quote.open as number).toFixed(2)}`} />
        )}
        {(quote.previousClose as number | undefined) != null && (
          <QuoteRow label={t('toolArtifact.prevClose')} value={`$${(quote.previousClose as number).toFixed(2)}`} />
        )}
        {(quote.dayLow as number | undefined) != null && (quote.dayHigh as number | undefined) != null && (
          <QuoteRow label={t('toolArtifact.dayRange')} value={`$${(quote.dayLow as number).toFixed(2)} - $${(quote.dayHigh as number).toFixed(2)}`} />
        )}
        {(quote.yearLow as number | undefined) != null && (quote.yearHigh as number | undefined) != null && (
          <QuoteRow label={t('toolArtifact.52wRange')} value={`$${(quote.yearLow as number).toFixed(2)} - $${(quote.yearHigh as number).toFixed(2)}`} />
        )}
        {(quote.volume as number | undefined) != null && (
          <QuoteRow label={t('toolArtifact.volume')} value={formatCompactNumber(quote.volume as number)} />
        )}
        {(quote.marketCap as number | undefined) != null && (
          <QuoteRow label={t('toolArtifact.marketCap')} value={formatMarketCap(quote.marketCap as number)} />
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
              <span style={{ color: TEXT_COLOR, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', minWidth: 0 }}>{(data.name as string) || sym}</span>
              <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, flexShrink: 0 }}>
                {lastClose != null && (
                  <span style={{ color: 'var(--color-text-primary)', fontWeight: 500 }}>
                    {lastClose.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
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
    .sort((a, b) => ((b.changesPercentage as number) || 0) - ((a.changesPercentage as number) || 0))
    .map((s) => ({
      name: abbreviateSector((s.sector as string) || 'N/A'),
      value: (s.changesPercentage as number) || 0,
      fill: ((s.changesPercentage as number) || 0) >= 0 ? GREEN : RED,
      label: formatPct((s.changesPercentage as number) || 0),
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
      <MeasuredContainer height={chartHeight}>
        {(w) => (
          <BarChart
            width={w}
            height={chartHeight}
            data={chartData}
            layout="vertical"
            margin={{ left: 0, right: isMobile ? 40 : 50, top: 0, bottom: 0 }}
          >
            <XAxis type="number" hide />
            <YAxis
              type="category"
              dataKey="name"
              width={isMobile ? 80 : 100}
              tick={{ fill: TEXT_COLOR, fontSize: sz.badgeFs }}
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
                style={{ fill: TEXT_COLOR, fontSize: sz.badgeFs }}
              />
            </Bar>
          </BarChart>
        )}
      </MeasuredContainer>
    </div>
  );
}

// ─── InlineStockScreenerCard ──────────────────────────────────────

export function InlineStockScreenerCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
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
                {(stock.price as number | undefined) != null ? `$${(stock.price as number).toFixed(2)}` : 'N/A'}
              </span>
              {!isMobile && (
                <span style={{ color: TEXT_COLOR, fontSize: 11, flexShrink: 0, textAlign: 'right' }}>
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
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? 12 : 14 }}>
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
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? 12 : 14 }}>{t('toolArtifact.8kFilings')}</span>
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
