import React, { useState, useEffect, useId } from 'react';
import { Info, List, ChevronDown } from 'lucide-react';
import { SymbolSwitcher } from './SymbolSwitcher';
import { HeaderPill } from './HeaderPill';
import { ExtendedHoursPair } from './ExtendedHoursPair';
import './StockHeader.css';
import type { StockSearchHit } from '@/lib/marketUtils';
import { DASH, compactNumberFixed2, createDateFormatter, signedFixed2 } from '@/lib/format';
import { priceOrDash, quoteDotState, quoteSessionText, type StockQuoteModel } from '../hooks/useStockQuoteModel';
import { fixedUpTo, venueLabelForSymbol } from '@/lib/bars';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useQuoteSourceLines } from '../hooks/useQuoteSourceLines';
import type { Freshness } from '@/types/market';
import type { ConnectionStatus, DataLevel } from '../hooks/useMarketDataWS';

interface StockHeaderProps {
  symbol: string;
  /** Derived once by the host (`useStockQuoteModel`), shared with the legend strips. */
  quote: StockQuoteModel;
  onToggleOverview: () => void;
  onOpenWatchlist?: () => void;
  wsStatus: ConnectionStatus;
  wsHasData?: boolean;
  wsDataLevel?: DataLevel;
  /** Makes the ticker a control: clicking it opens a search, and a pick lands here. */
  onSwitchSymbol?: (symbol: string, hit?: StockSearchHit) => void;
  /** Measured freshness of the chart's bars — a different surface, often a
   *  different provider, from the quote row's. */
  chartFreshness?: Freshness | null;
}

const tickTimeFormat = createDateFormatter({ hour: '2-digit', minute: '2-digit', second: '2-digit' });

function venueStatusLabel(sym: string, statusLabel: string): string {
  const venue = venueLabelForSymbol(sym);
  return venue ? `${venue} ${statusLabel}` : statusLabel;
}

/** The phone's stock header: on a desktop the ticker, quote and figures ride the chart's toolbar (`useChartLegendSlots`). */
const StockHeader = ({ symbol, quote: q, onToggleOverview, onOpenWatchlist, wsStatus, wsHasData = false, wsDataLevel = null, onSwitchSymbol, chartFreshness = null }: StockHeaderProps) => {
  const { t } = useTranslation();
  const locale = useLocale();
  const {
    headline, status, tickAt, changePercent,
    previousClose, open, high, low, fiftyTwoWeekHigh, fiftyTwoWeekLow, averageVolume, shownVolume, volumeIsAverage,
    displayName, displaySecondaryName, displayExchange, ext, currency, places,
  } = q;
  const fmt = (n: number | null | undefined): string => priceOrDash(n, places, locale);
  const hasDayRange = high != null && low != null;
  const sourceLines = useQuoteSourceLines({ symbol, quote: q, wsStatus, wsHasData, wsDataLevel, chartFreshness });
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

  return (
    <div className={`stock-header rule-b${metricsCollapsed ? ' stock-header--compact' : ''}`}>
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
                {sourceLines.map((line) => <span key={line}>{line}</span>)}
              </span>
            </span>
          </div>
          <div className="stock-header-actions">
            <HeaderPill onClick={onToggleOverview}>
              <Info size={13} />
              {t('marketView.companyOverview')}
            </HeaderPill>
          </div>
        </div>
        <div className="stock-price-section">
          {/* In an extended session the headline is the official close, stable
              across refreshes and intervals; the session move rides beneath it
              against its own anchor, in session color. */}
          {/* Figures print bare; the code names their currency once, here. An
              index level is points and names none. */}
          <div className={`stock-price ${headline.tone}`}>
            {fmt(headline.price)}
            {headline.price != null && currency && <span className="stock-price-currency">{currency}</span>}
          </div>
          {(headline.change != null && headline.pct != null) ? (
            <div className={`stock-change ${headline.tone}`}>
              {fixedUpTo(headline.change, places, locale, { signed: true })} {signedFixed2(headline.pct, locale)}%
            </div>
          ) : !ext && (
            <div className={`stock-change ${headline.tone}`}>{DASH}</div>
          )}
          {ext && (
            <ExtendedHoursPair
              ext={ext}
              places={places}
              iconSize={13}
              className="stock-extended-hours"
              style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: '0.8125rem' }}
            />
          )}
        </div>
      </div>

      <div className="stock-metrics-toggle-row">
        <button
          className="stock-metrics-toggle tap-target"
          onClick={() => setMetricsCollapsed(c => !c)}
          aria-expanded={!metricsCollapsed}
        >
          <span>{metricsCollapsed ? t('marketView.header.showMetrics') : t('marketView.header.hideMetrics')}</span>
          <ChevronDown size={14} className={`stock-metrics-toggle-icon${metricsCollapsed ? '' : ' stock-metrics-toggle-icon--open'}`} />
        </button>
        {onOpenWatchlist && (
          <button className="stock-metrics-watchlist-pill tap-target" onClick={onOpenWatchlist}>
            <List size={13} />
            {t('marketView.header.watchlist')}
          </button>
        )}
      </div>
      <div
        className={`stock-metrics-wrapper${metricsCollapsed ? ' stock-metrics-wrapper--collapsed' : ''}`}
      >
        <div className="stock-metrics rule-t">
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.prevClose')}</span>
            <span className="metric-value">{fmt(previousClose)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.open')}
              <span className="metrics-discrepancy-hint" title={t('marketView.header.openHint')}>!</span>
            </span>
            <span className="metric-value">{fmt(open)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.low')}</span>
            <span className="metric-value">{fmt(low)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.high')}</span>
            <span className="metric-value">{fmt(high)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.high52w')}</span>
            <span className="metric-value">{fmt(fiftyTwoWeekHigh)}</span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{t('marketView.header.low52w')}</span>
            <span className="metric-value">{fmt(fiftyTwoWeekLow)}</span>
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
              {hasDayRange ? `${fmt(low)} – ${fmt(high)}` : DASH}
            </span>
          </div>
          <div className="metric-item">
            <span className="metric-label">{ext ? t('marketView.header.changePctExt') : t('marketView.header.changePct')}</span>
            <span className={`metric-value ${(changePercent ?? 0) < 0 ? 'negative' : 'positive'}`}>
              {changePercent != null ? `${signedFixed2(changePercent, locale)}%` : DASH}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default React.memo(StockHeader);
