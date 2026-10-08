import React from 'react';
import { useTranslation } from 'react-i18next';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useLocale } from '@/hooks/useLocale';
import { createDateFormatter } from '@/lib/format';
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
import { FreshnessBadge } from './FreshnessBadge';
import { asQuoteTier, hasFreshnessFields, isLiveRow, resolveFreshnessBadge } from '@/lib/freshness';
import { formatMoney, isIndexListing } from '@/lib/bars';
import { resolveDualName } from '@/lib/displayName';
import type { Freshness, QuoteTier } from '@/types/market';

// ─── InlineQuoteCard ────────────────────────────────────────────────

interface QuoteDisplay {
  symbol: string;
  /** Local-first display name (贵州茅台 before Kweichow Moutai). */
  name?: string;
  /** The other spelling, shown only where it adds information. */
  secondaryName?: string;
  price?: number;
  change?: number;
  changePct?: number;
  low?: number;
  high?: number;
  open?: number;
  prevClose?: number;
  volume?: number;
  status?: string;
  /** Venue-local clock ('2026-07-14 15:32:05 HKT'), set only for non-US
   *  listings: the provider's print time when it gave one, else the time the
   *  row was retrieved. */
  asOfLocal?: string;
  /** True when `asOfLocal` is the print time, false when it is the retrieval clock. */
  printed?: boolean;
  extPrice?: number;
  extChange?: number;
  extChangePct?: number;
  /** ISO code the row is quoted in; absent on pre-contract artifacts and for
   *  an index, whose level is points. */
  currency?: string;
  /** Freshness tier the filling provider declares; absent on pre-contract artifacts. */
  tier?: QuoteTier | null;
  /** Freshness measured for this quote at response time. */
  freshness?: Freshness | null;
}

/**
 * Decompose one unified snapshot (snake_case) into display fields. During
 * pre/after-hours, and once the venue has closed, the main price is the regular
 * close and the extended move splits onto ext* fields — mirroring
 * InlineCompanyOverviewCard, whose badge names that price the last close. On
 * the FMP fallback the extended fields are None, so the blended change shows.
 */
function toQuoteDisplay(q: Record<string, unknown>): QuoteDisplay {
  const num = (v: unknown): number | undefined => (typeof v === 'number' ? v : undefined);
  const status = q.market_status as string | undefined;
  const leadsWithClose = status === 'early_trading' || status === 'late_trading' || status === 'closed';
  const regularClose = num(q.regular_close);
  const lastTrade = num(q.last_trade_price);
  const str = (v: unknown): string | undefined => (typeof v === 'string' ? v : undefined);
  const symbol = (q.symbol as string) || '?';
  const dual = resolveDualName({ name: str(q.name), nameLocal: str(q.name_local), nameEn: str(q.name_en) }, symbol);
  // The venue stamps an index with its currency (CNY for 000300.SH), but a level is points.
  const isIndex = isIndexListing(symbol, str(q.asset_class));
  const d: QuoteDisplay = {
    symbol,
    name: dual.named ? dual.primary : undefined,
    secondaryName: dual.secondary ?? undefined,
    low: num(q.low),
    high: num(q.high),
    open: num(q.open),
    prevClose: num(q.previous_close),
    volume: num(q.volume),
    status,
    asOfLocal: typeof q.as_of_local === 'string' ? q.as_of_local : undefined,
    printed: typeof q.as_of === 'number' && q.as_of > 0,
    currency: !isIndex && typeof q.currency === 'string' && q.currency ? q.currency : undefined,
    tier: asQuoteTier(q.tier),
    freshness: (q.freshness as Freshness | undefined) ?? undefined,
  };
  if (leadsWithClose && regularClose != null) {
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

// A row that carries its currency prints it; one without, a pre-contract
// artifact's, prints the bare figure as it always did, and an index level is
// bare whatever its venue stamps. The caller passes `useLocale()`.
function fmtQuotePrice(price: number, locale: string, currency?: string): string {
  return formatMoney(price, currency ?? null, locale);
}

function fmtSigned(value: number, locale: string, currency?: string): string {
  return formatMoney(value, currency ?? null, locale, { signed: true });
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
function localTitle(asOfTs: number | undefined, locale: string): string | undefined {
  return asOfTs != null ? localTitleFormat(asOfTs, locale) : undefined;
}

// The badge's tooltip is where a row's venue stamp goes, so the plain stamp
// steps aside only when a badge renders and has a stamp to carry. A row with
// freshness but no badge, or a badge but no venue stamp, still shows its time.
function badgeCarriesStamp(d: QuoteDisplay): boolean {
  return !!d.asOfLocal && resolveFreshnessBadge(d) != null;
}

function QuoteHero({ d, asOf, asOfTs }: { d: QuoteDisplay; asOf?: string; asOfTs?: number }): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const color = (d.changePct ?? 0) >= 0 ? GREEN : RED;
  const extColor = MARKET_STATUS_COLORS[d.status || ''] || TEXT_COLOR;
  return (
    <>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: 4, flexWrap: 'wrap' }}>
        <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', fontSize: isMobile ? '0.8125rem' : '0.9375rem' }}>{d.symbol}</span>
        {d.name && (
          <span style={{ fontSize: sz.rowFs, color: TEXT_COLOR, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {d.name}
            {d.secondaryName && !isMobile && <span style={{ marginLeft: 6 }}>{d.secondaryName}</span>}
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
        {!badgeCarriesStamp(d) && (d.asOfLocal ?? asOf) && (
          <span
            title={localTitle(asOfTs, locale)}
            style={{ marginLeft: 'auto', fontSize: sz.labelFs, color: TEXT_COLOR, ...TABULAR }}
          >
            {d.asOfLocal ?? asOf}
          </span>
        )}
      </div>
      {/* At phone width a CN¥ price, its signed change and the badge outgrow one
          line; the change drops below rather than running past the card. */}
      <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'baseline', columnGap: isMobile ? 8 : 10, rowGap: 2, marginBottom: d.extPrice != null ? 2 : sz.sectionMb }}>
        {d.price != null && (
          <span style={{ fontSize: isMobile ? '1.125rem' : '1.375rem', fontWeight: 700, color: 'var(--color-text-primary)', ...TABULAR }}>
            {fmtQuotePrice(d.price, locale, d.currency)}
          </span>
        )}
        {d.changePct != null && (
          <span style={{ fontSize: isMobile ? '0.75rem' : '0.875rem', color, fontWeight: 500, ...TABULAR }}>
            {d.change != null ? `${fmtSigned(d.change, locale, d.currency)} (${formatPct(d.changePct)})` : formatPct(d.changePct)}
          </span>
        )}
        <FreshnessBadge tier={d.tier} freshness={d.freshness} asOfLocal={d.asOfLocal} printed={d.printed} />
      </div>
      {d.extPrice != null && (
        <div style={{ display: 'flex', alignItems: 'baseline', gap: sz.gap, marginBottom: sz.sectionMb, fontSize: sz.rowFs }}>
          <span style={{ color: extColor, fontWeight: 600, fontSize: sz.labelFs }}>
            {extendedHoursLabel(t, d.status, 'long')}
          </span>
          <span style={{ fontWeight: 600, color: 'var(--color-text-primary)', ...TABULAR }}>{fmtQuotePrice(d.extPrice, locale, d.currency)}</span>
          {d.extChangePct != null && (
            <span style={{ color: (d.extChangePct >= 0 ? GREEN : RED), fontWeight: 500, ...TABULAR }}>
              {d.extChange != null ? `${fmtSigned(d.extChange, locale, d.currency)} (${formatPct(d.extChangePct)})` : formatPct(d.extChangePct)}
            </span>
          )}
        </div>
      )}
      {d.low != null && d.high != null && d.price != null && d.high > d.low && (
        <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, marginBottom: sz.sectionMb }}>
          <span style={{ fontSize: sz.labelFs, color: TEXT_COLOR, ...TABULAR }}>
            {t('toolArtifact.low')} {fmtQuotePrice(d.low, locale, d.currency)}
          </span>
          <QuoteRangeStrip d={d} hero />
          <span style={{ fontSize: sz.labelFs, color: TEXT_COLOR, ...TABULAR }}>
            {t('toolArtifact.high')} {fmtQuotePrice(d.high, locale, d.currency)}
          </span>
        </div>
      )}
      <div style={{ display: 'flex', gap: isMobile ? 8 : 14, fontSize: sz.labelFs, color: TEXT_COLOR, flexWrap: 'wrap' }}>
        {d.open != null && (
          <span>{t('toolArtifact.open')} <b style={{ fontWeight: 500, color: 'var(--color-text-secondary)', ...TABULAR }}>{fmtQuotePrice(d.open, locale, d.currency)}</b></span>
        )}
        {d.prevClose != null && (
          <span>{t('toolArtifact.prevClose')} <b style={{ fontWeight: 500, color: 'var(--color-text-secondary)', ...TABULAR }}>{fmtQuotePrice(d.prevClose, locale, d.currency)}</b></span>
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
  const locale = useLocale();
  const isMobile = useIsMobile();
  const sz = isMobile ? SIZES_MOBILE : SIZES_DESKTOP;
  const { quotes, as_of: asOf, as_of_ts: asOfTs, all_realtime: allRealtime } = (artifact || {}) as {
    quotes?: Record<string, unknown>[];
    as_of?: string;
    as_of_ts?: number;
    all_realtime?: boolean;
  };
  if (!quotes?.length) return null;
  const displays = quotes.map(toQuoteDisplay);

  // "Live Quotes" is a claim, so make it only when every row supports it. A
  // pre-contract artifact carries no freshness at all and keeps the old
  // heading; anything else has to earn it row by row.
  const anyFreshness = displays.some(hasFreshnessFields);
  const allLive =
    !anyFreshness ||
    (allRealtime !== false && displays.every((d) => !hasFreshnessFields(d) || isLiveRow(d)));

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
              {allLive ? t('toolArtifact.liveQuotes') : t('toolArtifact.quotes')}
            </span>
            {/* The backend stamps a block in the clock its rows share; a mixed
                block has none, so the reader's own clock stands in. */}
            {(asOf ?? asOfTs) != null && (
              <span
                title={asOf ? localTitle(asOfTs, locale) : undefined}
                style={{ marginLeft: 'auto', fontSize: sz.labelFs, color: TEXT_COLOR, ...TABULAR }}
              >
                {asOf ?? localTitle(asOfTs, locale)}
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
                    <FreshnessBadge tier={d.tier} freshness={d.freshness} asOfLocal={d.asOfLocal} printed={d.printed} compact />
                    <span style={{ fontWeight: 700, color: 'var(--color-text-primary)', flexShrink: 0 }}>{d.symbol}</span>
                    {d.name && !isMobile && (
                      <span style={{ color: TEXT_COLOR, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', minWidth: 0 }}>
                        {d.name}
                        {d.secondaryName && <span style={{ marginLeft: 6 }}>{d.secondaryName}</span>}
                      </span>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: sz.gap, flexShrink: 0 }}>
                    {d.asOfLocal && !isMobile && !badgeCarriesStamp(d) && (
                      <span title={localTitle(asOfTs, locale)} style={{ fontSize: sz.badgeFs, color: TEXT_COLOR, ...TABULAR }}>
                        {d.asOfLocal}
                      </span>
                    )}
                    {d.extPrice != null && !isMobile ? (
                      <span style={{ fontSize: sz.badgeFs, color: MARKET_STATUS_COLORS[d.status || ''] || TEXT_COLOR, ...TABULAR }}>
                        {extendedHoursLabel(t, d.status, 'short')} {fmtQuotePrice(d.extPrice, locale, d.currency)}
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
                        {fmtQuotePrice(d.price, locale, d.currency)}
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
