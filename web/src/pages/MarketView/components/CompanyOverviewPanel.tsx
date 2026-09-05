import React from 'react';
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
import './CompanyOverviewPanel.css';

const GREEN = 'var(--color-profit)';
const RED = 'var(--color-loss)';
const TEXT_COLOR = 'var(--color-text-secondary)';

interface QuoteData {
  price?: number;
  change?: number;
  changePct?: number;
  open?: number;
  previousClose?: number;
  dayLow?: number;
  dayHigh?: number;
  yearLow?: number;
  yearHigh?: number;
  volume?: number;
  marketCap?: number;
  pe?: number;
  eps?: number;
}

// TODO: type properly once MarketDataCharts exports its prop types
interface OverviewData {
  symbol?: string;
  name?: string;
  quote?: QuoteData;
  performance?: unknown;
  analystRatings?: unknown;
  quarterlyFundamentals?: unknown;
  earningsSurprises?: unknown;
  cashFlow?: unknown;
  revenueByProduct?: unknown;
  revenueByGeo?: unknown;
  [key: string]: unknown;
}

interface QuoteStatProps {
  label: string;
  value: string;
}

interface QuoteSummaryProps {
  data: OverviewData;
}

interface CompanyOverviewPanelProps {
  symbol: string;
  visible: boolean;
  onClose: () => void;
  data: OverviewData | null;
  loading: boolean;
}

const formatNumber = (num: number | null | undefined): string => {
  if (num == null) return 'N/A';
  if (Math.abs(num) >= 1e12) return `$${(num / 1e12).toFixed(2)}T`;
  if (Math.abs(num) >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
  if (Math.abs(num) >= 1e6) return `$${(num / 1e6).toFixed(2)}M`;
  if (Math.abs(num) >= 1e3) return `$${(num / 1e3).toFixed(1)}K`;
  return typeof num === 'number' ? `$${num.toFixed(2)}` : String(num);
};

function QuoteStat({ label, value }: QuoteStatProps) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '2px 0' }}>
      <span style={{ fontSize: '0.75rem', color: TEXT_COLOR, opacity: 0.7 }}>{label}</span>
      <span style={{ fontSize: '0.75rem', color: 'var(--color-text-primary)' }}>{value}</span>
    </div>
  );
}

function QuoteSummary({ data }: QuoteSummaryProps) {
  const { symbol, name, quote } = data;
  if (!quote) return null;

  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 8 }}>
        <span style={{ fontSize: '1.125rem', fontWeight: 700, color: 'var(--color-text-primary)' }}>
          {name || symbol}
        </span>
        <span style={{ fontSize: '0.8125rem', color: TEXT_COLOR }}>{symbol}</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 10 }}>
        <span style={{ fontSize: '1.375rem', fontWeight: 700, color: 'var(--color-text-primary)' }}>
          ${quote.price?.toFixed(2) || 'N/A'}
        </span>
        {quote.change != null && (
          <span style={{ fontSize: '0.8125rem', color: quote.change >= 0 ? GREEN : RED }}>
            {quote.change >= 0 ? '+' : ''}{quote.change?.toFixed(2)} ({quote.changePct?.toFixed(2)}%)
          </span>
        )}
      </div>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0 16px' }}>
        {quote.open != null && <QuoteStat label="Open" value={`$${quote.open.toFixed(2)}`} />}
        {quote.previousClose != null && <QuoteStat label="Prev Close" value={`$${quote.previousClose.toFixed(2)}`} />}
        {quote.dayLow != null && quote.dayHigh != null && (
          <QuoteStat label="Day Range" value={`$${quote.dayLow.toFixed(2)} - $${quote.dayHigh.toFixed(2)}`} />
        )}
        {quote.yearLow != null && quote.yearHigh != null && (
          <QuoteStat label="52W Range" value={`$${quote.yearLow.toFixed(2)} - $${quote.yearHigh.toFixed(2)}`} />
        )}
        {quote.volume != null && <QuoteStat label="Volume" value={formatNumber(quote.volume).replace('$', '')} />}
        {quote.marketCap != null && <QuoteStat label="Market Cap" value={formatNumber(quote.marketCap)} />}
        {quote.pe != null && <QuoteStat label="P/E" value={quote.pe.toFixed(2)} />}
        {quote.eps != null && <QuoteStat label="EPS" value={`$${quote.eps.toFixed(2)}`} />}
      </div>
    </div>
  );
}

export default function CompanyOverviewPanel({ symbol: _symbol, visible, onClose, data, loading }: CompanyOverviewPanelProps) {
  if (!visible) return null;

  const error = !data && !loading ? 'No data available' : null;

  return (
    <div className="company-overview-panel">
      <div className="company-overview-header">
        <h3>Company Overview</h3>
        <button className="company-overview-close" onClick={onClose}>
          <X size={16} />
        </button>
      </div>

      {loading && (
        <div className="company-overview-loading">
          <span aria-hidden="true" className="flex-shrink-0">
            <Loader size={16} className="text-current" />
          </span>
          Loading...
        </div>
      )}

      {error && !loading && (
        <div className="company-overview-error">{error}</div>
      )}

      {data && !loading && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          <QuoteSummary data={data} />
          <PerformanceBarChart performance={data.performance as Record<string, number> | undefined} />
          <AnalystRatingsChart ratings={data.analystRatings as Record<string, unknown> | undefined} />
          <QuarterlyRevenueChart data={data.quarterlyFundamentals as Record<string, unknown>[] | undefined} />
          <MarginsChart data={data.quarterlyFundamentals as Record<string, unknown>[] | undefined} />
          <EarningsSurpriseChart data={data.earningsSurprises as Record<string, unknown>[] | undefined} />
          <CashFlowChart data={data.cashFlow as Record<string, unknown>[] | undefined} />
          <RevenueBreakdownChart revenueByProduct={data.revenueByProduct as Record<string, number> | undefined} revenueByGeo={data.revenueByGeo as Record<string, number> | undefined} />
        </div>
      )}
    </div>
  );
}
