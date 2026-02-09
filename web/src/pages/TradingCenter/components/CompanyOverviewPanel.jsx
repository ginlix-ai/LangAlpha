import React from 'react';
import { X, Loader2 } from 'lucide-react';
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

const GREEN = '#10b981';
const RED = '#ef4444';
const TEXT_COLOR = '#8b8fa3';

const formatNumber = (num) => {
  if (num == null) return 'N/A';
  if (Math.abs(num) >= 1e12) return `$${(num / 1e12).toFixed(2)}T`;
  if (Math.abs(num) >= 1e9) return `$${(num / 1e9).toFixed(2)}B`;
  if (Math.abs(num) >= 1e6) return `$${(num / 1e6).toFixed(2)}M`;
  if (Math.abs(num) >= 1e3) return `$${(num / 1e3).toFixed(1)}K`;
  return typeof num === 'number' ? `$${num.toFixed(2)}` : String(num);
};

function QuoteStat({ label, value }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between', padding: '2px 0' }}>
      <span style={{ fontSize: 12, color: TEXT_COLOR, opacity: 0.7 }}>{label}</span>
      <span style={{ fontSize: 12, color: '#fff' }}>{value}</span>
    </div>
  );
}

function QuoteSummary({ data }) {
  const { symbol, name, quote } = data;
  if (!quote) return null;

  return (
    <div style={{ marginBottom: 16 }}>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 8 }}>
        <span style={{ fontSize: 18, fontWeight: 700, color: '#fff' }}>
          {name || symbol}
        </span>
        <span style={{ fontSize: 13, color: TEXT_COLOR }}>{symbol}</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 10 }}>
        <span style={{ fontSize: 22, fontWeight: 700, color: '#fff' }}>
          ${quote.price?.toFixed(2) || 'N/A'}
        </span>
        {quote.change != null && (
          <span style={{ fontSize: 13, color: quote.change >= 0 ? GREEN : RED }}>
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

export default function CompanyOverviewPanel({ symbol, visible, onClose, data, loading }) {
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
          <Loader2 size={16} className="animate-spin" />
          Loading...
        </div>
      )}

      {error && !loading && (
        <div className="company-overview-error">{error}</div>
      )}

      {data && !loading && (
        <div style={{ display: 'flex', flexDirection: 'column', gap: 20 }}>
          <QuoteSummary data={data} />
          <PerformanceBarChart performance={data.performance} />
          <AnalystRatingsChart ratings={data.analystRatings} />
          <QuarterlyRevenueChart data={data.quarterlyFundamentals} />
          <MarginsChart data={data.quarterlyFundamentals} />
          <EarningsSurpriseChart data={data.earningsSurprises} />
          <CashFlowChart data={data.cashFlow} />
          <RevenueBreakdownChart revenueByProduct={data.revenueByProduct} revenueByGeo={data.revenueByGeo} />
        </div>
      )}
    </div>
  );
}
