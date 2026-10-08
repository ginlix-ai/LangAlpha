import React from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import {
  PerformanceBarChart,
  AnalystRatingsChart,
  QuarterlyRevenueChart,
  MarginsChart,
  EarningsSurpriseChart,
  CashFlowChart,
  RevenueBreakdownChart,
} from '../../ChatAgent/components/charts/MarketDataCharts';
import { formatMoney } from '@/lib/bars';
import { compactNumberFixed2, fixed2, signedFixed2 } from '@/lib/format';
import { deriveOverviewQuote, type CompanyOverviewArtifact } from '@/lib/quotes/overview';
import './CompanyOverviewPanel.css';

const GREEN = 'var(--color-profit)';
const RED = 'var(--color-loss)';
const TEXT_COLOR = 'var(--color-text-secondary)';
const NAME_CLIP: React.CSSProperties = { minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' };


interface QuoteStatProps {
  label: string;
  value: string;
}

interface QuoteSummaryProps {
  data: CompanyOverviewArtifact;
  symbol: string;
}

interface CompanyOverviewPanelProps {
  symbol: string;
  visible: boolean;
  onClose: () => void;
  data: CompanyOverviewArtifact | null;
  loading: boolean;
}

function QuoteStat({ label, value }: QuoteStatProps) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '2px 0' }}>
      <span style={{ fontSize: '0.75rem', color: TEXT_COLOR, opacity: 0.7 }}>{label}</span>
      <span style={{ fontSize: '0.75rem', color: 'var(--color-text-primary)' }}>{value}</span>
    </div>
  );
}

const FUNDAMENTAL_KEYS = [
  'performance', 'analystRatings', 'quarterlyFundamentals', 'earningsSurprises',
  'cashFlow', 'revenueByProduct', 'revenueByGeo',
] as const;

function hasFundamentals(data: CompanyOverviewArtifact): boolean {
  return FUNDAMENTAL_KEYS.some((key) => {
    const value = data[key];
    if (value == null) return false;
    if (Array.isArray(value)) return value.length > 0;
    return typeof value !== 'object' || Object.keys(value as object).length > 0;
  });
}

function QuoteSummary({ data, symbol }: QuoteSummaryProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { quote } = data;
  if (!quote) return null;

  const { displayPrice, displayChange, displayChangePct, currency, dualName } = deriveOverviewQuote(data, symbol);
  const money = (n: number): string => formatMoney(n, currency, locale);

  return (
    <div style={{ marginBottom: 16 }}>
      {/* Two long names can outrun the panel, so both shrink to an ellipsis and
          the ticker, the shortest and most useful of the three, never does. */}
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 8, minWidth: 0 }}>
        <span title={dualName.primary} style={{ ...NAME_CLIP, fontSize: '1.125rem', fontWeight: 700, color: 'var(--color-text-primary)' }}>
          {dualName.primary}
        </span>
        {dualName.secondary && (
          <span title={dualName.secondary} style={{ ...NAME_CLIP, fontSize: '0.8125rem', color: TEXT_COLOR }}>{dualName.secondary}</span>
        )}
        {dualName.named && <span style={{ flexShrink: 0, fontSize: '0.8125rem', color: TEXT_COLOR }}>{symbol}</span>}
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 10 }}>
        <span style={{ fontSize: '1.375rem', fontWeight: 700, color: 'var(--color-text-primary)' }}>
          {formatMoney(displayPrice, currency, locale)}
        </span>
        {displayChange != null && (
          <span style={{ fontSize: '0.8125rem', color: displayChange >= 0 ? GREEN : RED }}>
            {formatMoney(displayChange, currency, locale, { signed: true })}
            {displayChangePct != null && ` (${signedFixed2(displayChangePct, locale)}%)`}
          </span>
        )}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0 16px' }}>
        {quote.open != null && <QuoteStat label={t('toolArtifact.open')} value={money(quote.open)} />}
        {quote.previousClose != null && <QuoteStat label={t('toolArtifact.prevClose')} value={money(quote.previousClose)} />}
        {quote.dayLow != null && quote.dayHigh != null && (
          <QuoteStat label={t('toolArtifact.dayRange')} value={`${money(quote.dayLow)} - ${money(quote.dayHigh)}`} />
        )}
        {quote.yearLow != null && quote.yearHigh != null && (
          <QuoteStat label={t('toolArtifact.52wRange')} value={`${money(quote.yearLow)} - ${money(quote.yearHigh)}`} />
        )}
        {quote.volume != null && <QuoteStat label={t('toolArtifact.volume')} value={compactNumberFixed2(quote.volume, locale)} />}
        {quote.marketCap != null && <QuoteStat label={t('toolArtifact.marketCap')} value={formatMoney(quote.marketCap, currency, locale, { compact: true })} />}
        {quote.pe != null && <QuoteStat label={t('toolArtifact.peRatio')} value={fixed2(quote.pe, locale)} />}
        {quote.eps != null && <QuoteStat label={t('toolArtifact.eps')} value={money(quote.eps)} />}
      </div>
    </div>
  );
}

export default function CompanyOverviewPanel({ symbol, visible, onClose, data, loading }: CompanyOverviewPanelProps) {
  const { t } = useTranslation();
  if (!visible) return null;

  const shownSymbol = data?.symbol || symbol;
  // Revenue, earnings and cash flow are in the issuer's reporting currency.
  const { isIndex, statementCurrency, earningsCurrency } = deriveOverviewQuote(data ?? {}, shownSymbol);
  // A payload with nothing to draw is as empty as no payload; an index still
  // says why its fundamentals are missing.
  const empty = !data || (!isIndex && !data.quote && !hasFundamentals(data));
  const error = empty && !loading ? t('marketView.overview.noData') : null;

  return (
    <div className="company-overview-panel">
      <div className="company-overview-header">
        <h3>{t('marketView.companyOverview')}</h3>
        <button className="company-overview-close" onClick={onClose} aria-label={t('common.close')}>
          <X size={16} />
        </button>
      </div>

      {loading && (
        <div className="company-overview-loading">
          <span aria-hidden="true" className="shrink-0">
            <Loader size={16} className="text-current" />
          </span>
          {t('common.loading')}
        </div>
      )}

      {error && !loading && (
        <div className="company-overview-error">{error}</div>
      )}

      {data && !empty && !loading && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          <QuoteSummary data={data} symbol={shownSymbol} />
          {isIndex && <div className="company-overview-note">{t('marketView.overview.indexNoFundamentals')}</div>}
          <PerformanceBarChart performance={data.performance} />
          <AnalystRatingsChart ratings={data.analystRatings} />
          <QuarterlyRevenueChart data={data.quarterlyFundamentals} currency={statementCurrency} />
          <MarginsChart data={data.quarterlyFundamentals} />
          <EarningsSurpriseChart data={data.earningsSurprises} currency={earningsCurrency} />
          <CashFlowChart data={data.cashFlow} currency={statementCurrency} />
          <RevenueBreakdownChart revenueByProduct={data.revenueByProduct} revenueByGeo={data.revenueByGeo} currency={statementCurrency} />
        </div>
      )}
    </div>
  );
}
