import React from 'react';
import { useTranslation } from 'react-i18next';
import { useIsMobile } from '@/hooks/useIsMobile';
import { createFormatter, createDateFormatter } from '@/lib/format';
import {
  GREEN,
  RED,
  TEXT_COLOR,
  CARD_BG,
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
  type InlineCardProps,
} from './inlineCardsShared';

// ─── InlineQuoteCard ────────────────────────────────────────────────

interface QuoteDisplay {
  symbol: string;
  name?: string;
  price?: number;
  change?: number;
  changePct?: number;
  low?: number;
  high?: number;
  open?: number;
  prevClose?: number;
  volume?: number;
  status?: string;
  /** Venue-local retrieval clock ('15:32:05 HKT'); only set for non-US listings. */
  asOfLocal?: string;
  extPrice?: number;
  extChange?: number;
  extChangePct?: number;
}

/**
 * Decompose one unified snapshot (snake_case) into display fields. During
 * pre/after-hours the main price is the regular close and the extended move
 * splits onto ext* fields — mirroring InlineCompanyOverviewCard. On the FMP
 * fallback the extended fields are None, so the blended change shows instead.
 */
function toQuoteDisplay(q: Record<string, unknown>): QuoteDisplay {
  const num = (v: unknown): number | undefined => (typeof v === 'number' ? v : undefined);
  const status = q.market_status as string | undefined;
  const isExtended = status === 'early_trading' || status === 'late_trading';
  const regularClose = num(q.regular_close);
  const lastTrade = num(q.last_trade_price);
  const d: QuoteDisplay = {
    symbol: (q.symbol as string) || '?',
    name: q.name as string | undefined,
    low: num(q.low),
    high: num(q.high),
    open: num(q.open),
    prevClose: num(q.previous_close),
    volume: num(q.volume),
    status,
    asOfLocal: typeof q.as_of_local === 'string' ? q.as_of_local : undefined,
  };
  if (isExtended && regularClose != null) {
    d.price = regularClose;
    d.change = num(q.regular_trading_change) ?? num(q.change);
    d.changePct = num(q.regular_trading_change_percent) ?? num(q.change_percent);
    if (lastTrade != null && lastTrade !== regularClose) {
      const prefix = status === 'early_trading' ? 'early' : 'late';
      d.extPrice = lastTrade;
      d.extChange = num(q[`${prefix}_trading_change`]);
      d.extChangePct = num(q[`${prefix}_trading_change_percent`]);
    }
  } else {
    d.price = lastTrade ?? num(q.price);
    d.change = num(q.change);
    d.changePct = num(q.change_percent);
  }
  return d;
}

const TABULAR: React.CSSProperties = { fontVariantNumeric: 'tabular-nums' };

// No currency symbol — quotes can be non-USD and the artifact has no currency
// field. Locale-aware via lib/format so it re-renders on a locale switch.
const quotePriceFormat = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });
function fmtQuotePrice(price: number): string {
  return quotePriceFormat(price);
}

function fmtSigned(value: number): string {
  return `${value >= 0 ? '+' : ''}${value.toFixed(2)}`;
}

/** Day-range strip: low→high track with the price marker, clamped to bounds. */
function QuoteRangeStrip({ d, hero }: { d: QuoteDisplay; hero?: boolean }): React.ReactElement | null {
  if (d.low == null || d.high == null || d.price == null || d.high <= d.low) return null;
  const pos = Math.min(1, Math.max(0, (d.price - d.low) / (d.high - d.low)));
  const color = (d.changePct ?? 0) >= 0 ? GREEN : RED;
  return (
    <span
      style={{
        position: 'relative',
        display: 'inline-block',
        width: hero ? 'auto' : 72,
        flex: hero ? 1 : undefined,
        flexShrink: hero ? undefined : 0,
        height: hero ? 4 : 3,
        borderRadius: 2,
        background: CARD_BORDER,
      }}
    >
      <span
        style={{
          position: 'absolute',
          top: '50%',
          left: `${pos * 100}%`,
          width: 7,
          height: 7,
          borderRadius: '50%',
          transform: 'translate(-50%, -50%)',
          background: color,
          boxShadow: `0 0 0 2px ${CARD_BG}`,
        }}
      />
    </span>
  );
}

// User-local rendering of the retrieval instant — tooltip on market-tz stamps.
// Locale-aware via lib/format (mirrors the app default numeric date+time).
const localTitleFormat = createDateFormatter({
  year: 'numeric', month: 'numeric', day: 'numeric',
  hour: 'numeric', minute: 'numeric', second: 'numeric',
});
function localTitle(asOfTs?: number): string | undefined {
  return asOfTs != null ? localTitleFormat(asOfTs) : undefined;
}

function QuoteHero({ d, asOf, asOfTs }: { d: QuoteDisplay; asOf?: string; asOfTs?: number }): React.ReactElement {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const color = (d.changePct ?? 0) >= 0 ? GREEN : RED;
  const extColor = MARKET_STATUS_COLORS[d.status || ''] || TEXT_COLOR;
  return (
    <>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: 4, flexWrap: 'wrap' }}>
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? 13 : 15 }}>{d.symbol}</span>
        {d.name && (
          <span style={{ fontSize: sz.rowFs, color: TEXT_COLOR, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {d.name}
          </span>
        )}
        {d.status && d.status !== 'open' && (
          <span style={{
            fontSize: sz.badgeFs, fontWeight: 600, padding: '1px 6px', borderRadius: 4,
            color: MARKET_STATUS_COLORS[d.status] || TEXT_COLOR,
            border: `1px solid ${MARKET_STATUS_COLORS[d.status] || TEXT_COLOR}`,
            whiteSpace: 'nowrap', flexShrink: 0,
          }}>
            {marketStatusLabel(t, d.status)}
          </span>
        )}
        {(d.asOfLocal ?? asOf) && (
          <span
            title={localTitle(asOfTs)}
            style={{ marginLeft: 'auto', fontSize: sz.labelFs, color: TEXT_COLOR, ...TABULAR }}
          >
            {d.asOfLocal ?? asOf}
          </span>
        )}
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: isMobile ? 8 : 10, marginBottom: d.extPrice != null ? 2 : sz.sectionMb }}>
        {d.price != null && (
          <span style={{ fontSize: isMobile ? 18 : 22, fontWeight: 700, color: 'var(--color-text-primary)', ...TABULAR }}>
            {fmtQuotePrice(d.price)}
          </span>
        )}
        {d.changePct != null && (
          <span style={{ fontSize: isMobile ? 12 : 14, color, fontWeight: 500, ...TABULAR }}>
            {d.change != null ? `${fmtSigned(d.change)} (${formatPct(d.changePct)})` : formatPct(d.changePct)}
          </span>
        )}
      </div>
      {d.extPrice != null && (
        <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: sz.sectionMb, fontSize: sz.rowFs }}>
          <span style={{ color: extColor, fontWeight: 600, fontSize: sz.labelFs }}>
            {extendedHoursLabel(t, d.status, 'long')}
          </span>
          <span style={{ fontWeight: 600, color: 'var(--color-text-primary)', ...TABULAR }}>{fmtQuotePrice(d.extPrice)}</span>
          {d.extChangePct != null && (
            <span style={{ color: (d.extChangePct >= 0 ? GREEN : RED), fontWeight: 500, ...TABULAR }}>
              {d.extChange != null ? `${fmtSigned(d.extChange)} (${formatPct(d.extChangePct)})` : formatPct(d.extChangePct)}
            </span>
          )}
        </div>
      )}
      {d.low != null && d.high != null && d.price != null && d.high > d.low && (
        <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, marginBottom: sz.sectionMb }}>
          <span style={{ fontSize: sz.labelFs, color: TEXT_COLOR, ...TABULAR }}>
            {t('toolArtifact.low')} {fmtQuotePrice(d.low)}
          </span>
          <QuoteRangeStrip d={d} hero />
          <span style={{ fontSize: sz.labelFs, color: TEXT_COLOR, ...TABULAR }}>
            {t('toolArtifact.high')} {fmtQuotePrice(d.high)}
          </span>
        </div>
      )}
      <div style={{ display: 'flex', gap: isMobile ? 8 : 14, fontSize: sz.labelFs, color: TEXT_COLOR, flexWrap: 'wrap' }}>
        {d.open != null && (
          <span>{t('toolArtifact.open')} <b style={{ fontWeight: 500, color: 'var(--color-text-secondary)', ...TABULAR }}>{fmtQuotePrice(d.open)}</b></span>
        )}
        {d.prevClose != null && (
          <span>{t('toolArtifact.prevClose')} <b style={{ fontWeight: 500, color: 'var(--color-text-secondary)', ...TABULAR }}>{fmtQuotePrice(d.prevClose)}</b></span>
        )}
        {d.volume != null && (
          <span>{t('toolArtifact.vol')} <b style={{ fontWeight: 500, color: 'var(--color-text-secondary)', ...TABULAR }}>{formatCompactNumber(d.volume)}</b></span>
        )}
      </div>
    </>
  );
}

/**
 * Adaptive card for the `get_quote` tool: one symbol renders a hero (big
 * price, labeled day-range strip, stats line); two or more render compact
 * rows with a range strip each. Extended-hours moves split onto their own
 * line/chip when the provider supplies them.
 */
export function InlineQuoteCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const { quotes, as_of: asOf, as_of_ts: asOfTs } = (artifact || {}) as {
    quotes?: Record<string, unknown>[];
    as_of?: string;
    as_of_ts?: number;
  };
  if (!quotes?.length) return null;
  const displays = quotes.map(toQuoteDisplay);

  return (
    <div
      style={isMobile ? mobileCardStyle : cardStyle}
      onClick={onClick}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = CARD_BORDER)}
    >
      {displays.length === 1 ? (
        <QuoteHero d={displays[0]} asOf={asOf} asOfTs={asOfTs} />
      ) : (
        <>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: isMobile ? 4 : 8 }}>
            <span style={{ fontWeight: 600, color: 'var(--color-text-primary)', fontSize: sz.headerFs }}>
              {t('toolArtifact.liveQuotes')}
            </span>
            {asOf && (
              <span
                title={localTitle(asOfTs)}
                style={{ marginLeft: 'auto', fontSize: sz.labelFs, color: TEXT_COLOR, ...TABULAR }}
              >
                {asOf}
              </span>
            )}
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: sz.listGap }}>
            {displays.map((d, i) => {
              const color = (d.changePct ?? 0) >= 0 ? GREEN : RED;
              return (
                <div
                  key={`${d.symbol}-${i}`}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'space-between',
                    gap: sz.gap,
                    padding: sz.rowPad,
                    fontSize: sz.rowFs,
                  }}
                >
                  <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, minWidth: 0 }}>
                    <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', flexShrink: 0 }}>{d.symbol}</span>
                    {d.name && !isMobile && (
                      <span style={{ color: TEXT_COLOR, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', minWidth: 0 }}>
                        {d.name}
                      </span>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, flexShrink: 0 }}>
                    {d.asOfLocal && !isMobile && (
                      <span title={localTitle(asOfTs)} style={{ fontSize: sz.badgeFs, color: TEXT_COLOR, ...TABULAR }}>
                        {d.asOfLocal}
                      </span>
                    )}
                    {d.extPrice != null && !isMobile ? (
                      <span style={{ fontSize: sz.badgeFs, color: MARKET_STATUS_COLORS[d.status || ''] || TEXT_COLOR, ...TABULAR }}>
                        {extendedHoursLabel(t, d.status, 'short')} {fmtQuotePrice(d.extPrice)}
                        {d.extChangePct != null ? ` ${formatPct(d.extChangePct)}` : ''}
                      </span>
                    ) : d.status && d.status !== 'open' ? (
                      <span style={{ fontSize: sz.badgeFs, color: MARKET_STATUS_COLORS[d.status] || TEXT_COLOR }}>
                        {marketStatusLabel(t, d.status)}
                      </span>
                    ) : null}
                    <QuoteRangeStrip d={d} />
                    {d.price != null && (
                      <span style={{ color: 'var(--color-text-primary)', fontWeight: 500, ...TABULAR }}>
                        {fmtQuotePrice(d.price)}
                      </span>
                    )}
                    {d.changePct != null && (
                      <span style={{ color, fontWeight: 500, minWidth: sz.changeMinW, textAlign: 'right', ...TABULAR }}>
                        {formatPct(d.changePct)}
                      </span>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </>
      )}
    </div>
  );
}
