import React, { useState, useEffect, useId } from 'react';
import { Info, List, ChevronDown } from 'lucide-react';
import { SymbolSwitcher } from './SymbolSwitcher';
import { HeaderPill } from './HeaderPill';
import { ExtendedHoursPair } from './ExtendedHoursPair';
import './StockHeader.css';
import type { StockSearchHit } from '@/lib/marketUtils';
import { compactNumberFixed2, createDateFormatter, fixed2, signedFixed2 } from '@/lib/format';
import { DASH, fixed2OrDash as fmt, quoteDotState, quoteSessionText, quoteSourceText, type StockQuoteModel } from '../hooks/useStockQuoteModel';
import { venueLabelForSymbol } from '@/lib/bars';
import { chartFreshnessParts, freshnessSpecText, venueTime } from '@/lib/freshness';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import type { Freshness } from '@/types/market';
import type { ConnectionStatus, DataLevel } from '../hooks/useMarketDataWS';

interface ChartMeta {
  dateRange?: { from: string; to: string };
  dataPoints?: number;
  [key: string]: unknown;
}

interface StockHeaderProps {
  symbol: string;
  /** Derived once by the host (`useStockQuoteModel`), shared with the legend strips. */
  quote: StockQuoteModel;
  chartMeta: ChartMeta | null;
  onToggleOverview: () => void;
  onOpenWatchlist?: () => void;
  wsStatus: ConnectionStatus;
  wsHasData?: boolean;
  wsDataLevel?: DataLevel;
  ginlixDataEnabled?: boolean;
  /** Makes the ticker a control: clicking it opens a search, and a pick lands here. */
  onSwitchSymbol?: (symbol: string, hit?: StockSearchHit) => void;
  /** A host's own buttons, beside Company Overview. */
  headerActions?: React.ReactNode;
  /** Measured freshness of the chart's bars — a different surface, often a
   *  different provider, from the quote row's. */
  chartFreshness?: Freshness | null;
}

const tickTimeFormat = createDateFormatter({ hour: '2-digit', minute: '2-digit', second: '2-digit' });

function venueStatusLabel(sym: string, statusLabel: string): string {
  const venue = venueLabelForSymbol(sym);
  return venue ? `${venue} ${statusLabel}` : statusLabel;
}

/** Which `marketView.header.ws` phrase describes the socket. */
function wsStatusKey(status: ConnectionStatus, hasData: boolean, level: DataLevel): string {
  if (status === 'connected') {
    if (!hasData) return 'connectedNoData';
    return level === 'second' ? 'connectedSecond' : 'connectedMinute';
  }
  if (status === 'disabled') return 'unavailable';
  return status === 'reconnecting' ? 'reconnecting' : 'disconnected';
}

const StockHeader = ({ symbol, quote: q, chartMeta: _chartMeta, onToggleOverview, onOpenWatchlist, wsStatus, wsHasData = false, wsDataLevel = null, ginlixDataEnabled: _ginlixDataEnabled = true, onSwitchSymbol, headerActions, chartFreshness = null }: StockHeaderProps) => {
  const { t } = useTranslation();
  const locale = useLocale();
  // Which venue day is today decides whether a stamp carries its date.
  const now = useNow();
  const {
    headline, status, tickAt, changePercent,
    previousClose, open, high, low, fiftyTwoWeekHigh, fiftyTwoWeekLow, averageVolume, shownVolume, volumeIsAverage,
    displayName, displaySecondaryName, displayExchange, ext, currency,
  } = q;
  const hasDayRange = high != null && low != null;

  // Two surfaces, measured independently: the quote and the chart routinely come
  // from different providers and disagree, which is the whole reason both lines
  // exist. The decision itself is lib/freshness's, shared with the inline
  // artifact badge; with nothing to state we never fall back to "Realtime".
  const quoteLabel = freshnessSpecText(t, q.quoteBadge);
  const quoteLine = [
    quoteSourceText(t, q),
    quoteLabel ?? t('marketView.header.quoteUnknownTier'),
    venueTime(q.asOf, symbol, locale, now),
  ].filter(Boolean).join(' · ');

  const chartParts = chartFreshness ? chartFreshnessParts(t, chartFreshness, symbol, locale, now) : null;
  const chartLine = chartParts
    ? [chartParts.source, chartParts.state, chartParts.lastBar].filter(Boolean).join(' · ')
    : null;
  const isMobile = useIsMobile();
  const [metricsCollapsed, setMetricsCollapsed] = useState(false);
  const sourceDetailId = useId();

  const [tickTime, setTickTime] = useState<Date | null>(null);
  useEffect(() => {
    if (tickAt) setTickTime(new Date(tickAt));
  }, [tickAt]);

  const formatTickTime = (date: Date | null): string | null => {
    if (!date) return null;
    return tickTimeFormat(date, locale);
  };

  const watchlistBtn = onOpenWatchlist ? (
    <button className="stock-metrics-watchlist-pill" onClick={onOpenWatchlist}>
      <List size={13} />
      {t('marketView.header.watchlist')}
    </button>
  ) : null;

  return (
    <div className={`stock-header${isMobile && metricsCollapsed ? ' stock-header--compact' : ''}`}>
      <div className="stock-header-top">
        <div className="stock-header-identity">
          <div className="stock-title">
            {onSwitchSymbol ? (
              <SymbolSwitcher symbol={symbol} onPick={onSwitchSymbol} />
            ) : (
              <span className="stock-symbol">{symbol}</span>
            )}
            {/* One truncating unit: the two names read as one phrase and take a
                single ellipsis, rather than each shrinking to its own min-content. */}
            {q.hasName && (
              <span className="stock-names" title={[displayName, displaySecondaryName].filter(Boolean).join(' ')}>
                <span className="stock-name">{displayName}</span>
                {displaySecondaryName && <span className="stock-name-secondary">{displaySecondaryName}</span>}
              </span>
            )}
            {displayExchange && <span className="stock-exchange">{displayExchange}</span>}
            {/* A tab stop so the source lines open by keyboard and by tap, not
                only under a hovering pointer; they describe the session label. */}
            <span className="stock-data-source stock-data-source--inline" tabIndex={0} aria-describedby={sourceDetailId}>
              <span className={`data-source-dot data-source-dot--${quoteDotState(q)}`} />
              <span className="data-source-label">
                {status === 'live' ? quoteSessionText(t, q) : venueStatusLabel(symbol, quoteSessionText(t, q))}
              </span>
              {status === 'live' && tickTime && <span className="data-source-time">{formatTickTime(tickTime)}</span>}
              <span className="data-source-tooltip" role="tooltip" id={sourceDetailId}>
                <span>{t('marketView.header.quote')}: {quoteLine}</span>
                {chartLine && <span>{t('marketView.header.chart')}: {chartLine}</span>}
                <span>{t('marketView.header.ws.label', { status: t(`marketView.header.ws.${wsStatusKey(wsStatus, wsHasData, wsDataLevel)}`) })}</span>
              </span>
            </span>
          </div>
          <div className="stock-header-actions">
            <HeaderPill onClick={onToggleOverview}>
              <Info size={13} />
              {t('marketView.companyOverview')}
            </HeaderPill>
            {headerActions}
          </div>
        </div>
        <div className="stock-price-section">
          {/* In an extended session the headline is the official close, stable
              across refreshes and intervals; the session move rides beneath it
              against its own anchor, in session color. */}
          {/* Figures print bare; the code names their currency once, here. An
              index level is points and names none. */}
          <div className={`stock-price ${headline.tone}`}>
            {fmt(headline.price, locale)}
            {headline.price != null && currency && <span className="stock-price-currency">{currency}</span>}
          </div>
          {(headline.change != null && headline.pct != null) ? (
            <div className={`stock-change ${headline.tone}`}>
              {signedFixed2(headline.change, locale)} {signedFixed2(headline.pct, locale)}%
            </div>
          ) : !ext && (
            <div className={`stock-change ${headline.tone}`}>{DASH}</div>
          )}
          {ext && (
            <ExtendedHoursPair
              ext={ext}
              iconSize={13}
              className="stock-extended-hours"
              style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: '0.8125rem' }}
            />
          )}
        </div>
      </div>

      {isMobile && (
        <div className="stock-metrics-toggle-row">
          <button
            className="stock-metrics-toggle"
            onClick={() => setMetricsCollapsed(c => !c)}
            aria-expanded={!metricsCollapsed}
          >
            <span>{metricsCollapsed ? t('marketView.header.showMetrics') : t('marketView.header.hideMetrics')}</span>
            <ChevronDown size={14} className={`stock-metrics-toggle-icon${metricsCollapsed ? '' : ' stock-metrics-toggle-icon--open'}`} />
          </button>
          {watchlistBtn}
        </div>
      )}
      <div
        className={`stock-metrics-wrapper${isMobile && metricsCollapsed ? ' stock-metrics-wrapper--collapsed' : ''}`}
      >
        <div className="stock-metrics">
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.prevClose')}</span>
            <span className="metric-value">{fmt(previousClose, locale)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.open')}
              <span className="metrics-discrepancy-hint" title={t('marketView.header.openHint')}>!</span>
            </span>
            <span className="metric-value">{fmt(open, locale)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.low')}</span>
            <span className="metric-value">{fmt(low, locale)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.high')}</span>
            <span className="metric-value">{fmt(high, locale)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.high52w')}</span>
            <span className="metric-value">{fmt(fiftyTwoWeekHigh, locale)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.low52w')}</span>
            <span className="metric-value">{fmt(fiftyTwoWeekLow, locale)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.avgVolume3m')}</span>
            <span className="metric-value">
              {averageVolume != null ? compactNumberFixed2(averageVolume, locale) : DASH}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{volumeIsAverage ? t('marketView.header.avgVolume3m') : t('marketView.header.volume')}</span>
            <span className="metric-value">
              {shownVolume != null ? compactNumberFixed2(shownVolume, locale) : DASH}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.dayRange')}</span>
            <span className="metric-value">
              {hasDayRange ? `${fixed2(low, locale)} – ${fixed2(high, locale)}` : DASH}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{ext ? t('marketView.header.changePctExt') : t('marketView.header.changePct')}</span>
            <span className={`metric-value ${(changePercent ?? 0) < 0 ? 'negative' : 'positive'}`}>
              {changePercent != null ? `${signedFixed2(changePercent, locale)}%` : DASH}
            </span>
          </div>
          {!isMobile && onOpenWatchlist && (
            <div className="metric-item metric-item--watchlist">
              {watchlistBtn}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default React.memo(StockHeader);
