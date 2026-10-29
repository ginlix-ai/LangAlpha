/**
 * The header as two thin strips instead of a block, for a host that keeps
 * its height for the chart. `LegendLead` is the ticker, price and session in
 * toolbar-sized type, meant to sit at the start of the chart's toolbar row;
 * `LegendStats` is the day's figures as one mono string, meant for the row
 * beneath. Both print the quote model StockHeader prints, so the numbers
 * cannot disagree between the two presentations.
 */

import React from 'react';
import { useTranslation } from 'react-i18next';
import { SymbolSwitcher } from './SymbolSwitcher';
import { ExtendedHoursPair } from './ExtendedHoursPair';
import { DASH, fixed2OrDash as fmt, quoteDotState, quoteSessionText, quoteSourceText, type StockQuoteModel } from '../hooks/useStockQuoteModel';
import { headlineChange } from './legendLeadShape';
import { compactNumberFixed2, signedFixed2 } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import type { StockSearchHit } from '@/lib/marketUtils';
import './StockLegendStrip.css';

export interface LegendLeadProps {
  symbol: string;
  quote: StockQuoteModel;
  /** Makes the ticker a control: a pick lands here. */
  onSwitchSymbol?: (symbol: string, hit?: StockSearchHit) => void;
}

export function LegendLead({ symbol, quote: q, onSwitchSymbol }: LegendLeadProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const { headline } = q;

  // Inline content on purpose: the chart's lead slot lays it on one line and
  // cuts it with an ellipsis when the row is short of room.
  return (
    <span className="legend-lead" title={[q.displayName, q.displaySecondaryName].filter(Boolean).join(' ')}>
      {onSwitchSymbol ? (
        <SymbolSwitcher symbol={symbol} onPick={onSwitchSymbol} />
      ) : (
        <span className="legend-lead-symbol">{symbol}</span>
      )}
      <span className={`legend-lead-price ${headline.tone}`}>{fmt(headline.price, locale)}</span>
      {headline.price != null && q.currency && <span className="legend-lead-currency">{q.currency}</span>}
      <span className={`legend-lead-change ${headline.tone}`}>{headlineChange(headline, locale)}</span>
      {q.ext && <ExtendedHoursPair ext={q.ext} iconSize={11} className="legend-lead-ext" />}
      <span className={`legend-lead-status legend-lead-status--${quoteDotState(q)}`} title={t('marketView.quote.source', { label: quoteSourceText(t, q) })}>
        <span className="legend-lead-dot" />
        {quoteSessionText(t, q)}
      </span>
    </span>
  );
}

export function LegendStats({ quote: q }: { quote: StockQuoteModel }): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const cells: Array<{ k: string; v: string; tone?: string }> = [
    { k: t('marketView.quote.open'), v: fmt(q.open, locale) },
    { k: t('marketView.quote.high'), v: fmt(q.high, locale) },
    { k: t('marketView.quote.low'), v: fmt(q.low, locale) },
    // The candle ends at the regular close: in an extended session the live
    // price belongs to the lead, and C is the settled close the headline shows.
    { k: t('marketView.quote.close'), v: fmt(q.headline.price, locale) },
    { k: t(q.volumeIsAverage ? 'marketView.quote.avgVolume3m' : 'marketView.quote.volume'), v: q.shownVolume != null ? compactNumberFixed2(q.shownVolume, locale) : DASH },
    { k: t('marketView.quote.prevClose'), v: fmt(q.previousClose, locale) },
    { k: t('marketView.quote.range52w'), v: q.fiftyTwoWeekLow != null && q.fiftyTwoWeekHigh != null ? `${fmt(q.fiftyTwoWeekLow, locale)}–${fmt(q.fiftyTwoWeekHigh, locale)}` : DASH },
    { k: t(q.ext ? 'marketView.quote.changeExt' : 'marketView.quote.change'), v: q.changePercent != null ? `${signedFixed2(q.changePercent, locale)}%` : DASH, tone: q.tone },
  ];
  return (
    <div className="legend-stats">
      {cells.map(({ k, v, tone }) => (
        <span className="legend-stat" key={k}>
          <span className="legend-stat-key">{k}</span>
          <span className={`legend-stat-value${tone ? ` ${tone}` : ''}`}>{v}</span>
        </span>
      ))}
    </div>
  );
}
