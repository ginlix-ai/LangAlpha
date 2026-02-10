import React, { useState } from 'react';
import { ExternalLink, FileText, Calendar, Building2, Layers, Newspaper } from 'lucide-react';
import { api } from '../../../../api/client';

const TEXT_COLOR = '#8b8fa3';
const ACCENT = '#6155F5';
const API_BASE = api.defaults.baseURL;

function InfoRow({ icon: Icon, label, value }) {
  if (value == null) return null;
  return (
    <div className="flex items-center gap-2 py-1">
      <Icon className="h-3.5 w-3.5 flex-shrink-0" style={{ color: TEXT_COLOR }} />
      <span className="text-xs" style={{ color: TEXT_COLOR }}>{label}</span>
      <span className="text-xs ml-auto" style={{ color: '#fff' }}>{value}</span>
    </div>
  );
}

function ItemChip({ label }) {
  return (
    <span
      className="inline-block text-xs px-2 py-0.5 rounded-full"
      style={{
        backgroundColor: 'rgba(97, 85, 245, 0.12)',
        color: 'rgba(255, 255, 255, 0.75)',
        border: '1px solid rgba(97, 85, 245, 0.2)',
      }}
    >
      {label}
    </span>
  );
}

/**
 * 10-K / 10-Q filing viewer with metadata header and embedded SEC document.
 */
function AnnualQuarterlyView({ data }) {
  const [iframeLoading, setIframeLoading] = useState(true);

  const proxyUrl = data.source_url
    ? `${API_BASE}/api/v1/sec-proxy/document?url=${encodeURIComponent(data.source_url)}`
    : null;

  return (
    <div className="flex flex-col h-full" style={{ gap: 16 }}>
      {/* Header */}
      <div className="flex-shrink-0">
        <div className="flex items-baseline gap-3 mb-2">
          <span
            className="text-xs font-bold px-2 py-0.5 rounded"
            style={{ backgroundColor: 'rgba(97, 85, 245, 0.15)', color: ACCENT }}
          >
            {data.symbol}
          </span>
          <span style={{ fontSize: 16, fontWeight: 700, color: '#fff' }}>
            {data.filing_type} Filing
          </span>
        </div>

        {/* Metadata rows */}
        <div
          className="rounded-lg px-3 py-2"
          style={{ backgroundColor: 'rgba(255, 255, 255, 0.03)', border: '1px solid rgba(255, 255, 255, 0.06)' }}
        >
          <InfoRow icon={Calendar} label="Filing Date" value={data.filing_date} />
          <InfoRow icon={Calendar} label="Period End" value={data.period_end} />
          <InfoRow icon={Building2} label="CIK" value={data.cik} />
          <InfoRow icon={Layers} label="Sections Extracted" value={data.sections_extracted} />
          {data.has_earnings_call && (
            <InfoRow icon={FileText} label="Earnings Call" value="Included" />
          )}
          {data.recent_8k_count != null && (
            <InfoRow icon={Newspaper} label="Recent 8-K Filings" value={`${data.recent_8k_count} (last 90 days)`} />
          )}
        </div>
      </div>

      {/* Embedded document — fills remaining height */}
      {proxyUrl && (
        <div className="flex flex-col flex-1 min-h-0">
          <div className="flex items-center justify-between mb-2 flex-shrink-0">
            <span className="text-xs font-medium uppercase tracking-wider" style={{ color: 'rgba(255, 255, 255, 0.4)' }}>
              SEC Filing Document
            </span>
            <a
              href={data.source_url}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 text-xs transition-colors hover:brightness-125"
              style={{ color: ACCENT, textDecoration: 'none' }}
            >
              Open on SEC EDGAR
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
          <div
            className="relative rounded-lg overflow-hidden flex-1 min-h-0"
            style={{ border: '1px solid rgba(255, 255, 255, 0.1)' }}
          >
            {iframeLoading && (
              <div
                className="absolute inset-0 flex items-center justify-center"
                style={{ backgroundColor: 'rgba(15, 20, 34, 0.9)' }}
              >
                <div className="flex items-center gap-2">
                  <div className="h-4 w-4 border-2 border-t-transparent rounded-full animate-spin" style={{ borderColor: `${ACCENT} transparent ${ACCENT} ${ACCENT}` }} />
                  <span className="text-xs" style={{ color: TEXT_COLOR }}>Loading SEC document...</span>
                </div>
              </div>
            )}
            <iframe
              src={proxyUrl}
              title={`${data.symbol} ${data.filing_type} Filing`}
              className="w-full h-full"
              style={{
                border: 'none',
                backgroundColor: '#fff',
              }}
              onLoad={() => setIframeLoading(false)}
              sandbox="allow-same-origin allow-scripts"
            />
          </div>
        </div>
      )}
    </div>
  );
}

/**
 * 8-K filing list viewer with expandable filing cards.
 */
function EightKListView({ data }) {
  const filings = data.filings || [];

  return (
    <div className="space-y-4">
      {/* Header */}
      <div>
        <div className="flex items-baseline gap-3 mb-1">
          <span
            className="text-xs font-bold px-2 py-0.5 rounded"
            style={{ backgroundColor: 'rgba(97, 85, 245, 0.15)', color: ACCENT }}
          >
            {data.symbol}
          </span>
          <span style={{ fontSize: 16, fontWeight: 700, color: '#fff' }}>
            8-K Filings
          </span>
        </div>
        <span className="text-xs" style={{ color: TEXT_COLOR }}>
          {data.filing_count} filing{data.filing_count !== 1 ? 's' : ''} in the last {data.days_range} days
        </span>
      </div>

      {/* Filing cards */}
      {filings.map((filing, i) => (
        <div
          key={i}
          className="rounded-lg px-4 py-3"
          style={{
            backgroundColor: 'rgba(255, 255, 255, 0.03)',
            border: '1px solid rgba(255, 255, 255, 0.06)',
          }}
        >
          {/* Date + press release indicator */}
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <Calendar className="h-3.5 w-3.5" style={{ color: TEXT_COLOR }} />
              <span className="text-sm font-medium" style={{ color: '#fff' }}>
                {filing.filing_date}
              </span>
              {filing.has_press_release && (
                <span
                  className="text-xs px-1.5 py-0.5 rounded"
                  style={{ backgroundColor: 'rgba(16, 185, 129, 0.12)', color: '#10b981', fontSize: 10 }}
                >
                  Press Release
                </span>
              )}
            </div>
            {filing.source_url && (
              <a
                href={`${API_BASE}/api/v1/sec-proxy/document?url=${encodeURIComponent(filing.source_url)}`}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1 text-xs transition-colors hover:brightness-125"
                style={{ color: ACCENT, textDecoration: 'none' }}
              >
                View
                <ExternalLink className="h-3 w-3" />
              </a>
            )}
          </div>

          {/* Item chips */}
          <div className="flex flex-wrap gap-1.5">
            {filing.items?.map((item, j) => (
              <ItemChip
                key={j}
                label={filing.items_desc?.[j] ? `${item}: ${filing.items_desc[j]}` : item}
              />
            ))}
          </div>
        </div>
      ))}

      {filings.length === 0 && (
        <div className="text-sm py-4 text-center" style={{ color: TEXT_COLOR }}>
          No 8-K filings found in the last {data.days_range} days.
        </div>
      )}
    </div>
  );
}

/**
 * SecFilingViewer routes between 10-K/10-Q and 8-K views.
 */
export default function SecFilingViewer({ data }) {
  if (!data) return null;

  if (data.filing_type === '8-K') {
    return <EightKListView data={data} />;
  }

  return <AnnualQuarterlyView data={data} />;
}
