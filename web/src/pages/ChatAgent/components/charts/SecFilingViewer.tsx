import React, { useState } from 'react';
import { ExternalLink, FileText, Calendar, Building2, Layers, Newspaper, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { api } from '../../../../api/client';

const TEXT_COLOR = 'var(--color-text-tertiary)';
const ACCENT = 'var(--color-accent-primary)';
const API_BASE = api.defaults.baseURL;

interface InfoRowProps {
  icon: LucideIcon;
  label: string;
  value: string | number | null | undefined;
}

function InfoRow({ icon: Icon, label, value }: InfoRowProps): React.ReactElement | null {
  if (value == null) return null;
  return (
    <div className="flex items-center gap-2 py-1">
      <Icon className="h-3.5 w-3.5 flex-shrink-0" style={{ color: TEXT_COLOR }} />
      <span className="text-xs" style={{ color: TEXT_COLOR }}>{label}</span>
      <span className="text-xs ml-auto" style={{ color: 'var(--color-text-primary)' }}>{value}</span>
    </div>
  );
}

interface ItemChipProps {
  label: string;
}

function ItemChip({ label }: ItemChipProps): React.ReactElement {
  return (
    <span
      className="inline-block text-xs px-2 py-0.5 rounded-full"
      style={{
        backgroundColor: 'var(--color-accent-soft)',
        color: 'var(--color-text-tertiary)',
        border: '1px solid var(--color-accent-soft)',
      }}
    >
      {label}
    </span>
  );
}

interface FilingDataProps {
  data: Record<string, unknown>;
}

/**
 * 10-K / 10-Q filing viewer with metadata header and embedded SEC document.
 */
function AnnualQuarterlyView({ data }: FilingDataProps): React.ReactElement {
  const { t } = useTranslation();
  const [iframeLoading, setIframeLoading] = useState(true);

  const proxyUrl = data.source_url
    ? `${API_BASE}/api/v1/sec-proxy/document?url=${encodeURIComponent(data.source_url as string)}`
    : null;

  return (
    <div className="flex flex-col h-full" style={{ gap: 16 }}>
      {/* Header */}
      <div className="flex-shrink-0">
        <div className="flex items-baseline gap-3 mb-2">
          <span
            className="text-xs font-bold px-2 py-0.5 rounded"
            style={{ backgroundColor: 'var(--color-accent-soft)', color: ACCENT }}
          >
            {data.symbol as string}
          </span>
          <span style={{ fontSize: '1rem', fontWeight: 700, color: 'var(--color-text-primary)' }}>
            {t('toolArtifact.filing', { type: data.filing_type as string })}
          </span>
        </div>

        {/* Metadata rows */}
        <div
          className="rounded-lg px-3 py-2"
          style={{ backgroundColor: 'var(--color-bg-surface)', border: '1px solid var(--color-border-muted)' }}
        >
          <InfoRow icon={Calendar} label={t('toolArtifact.filingDate')} value={data.filing_date as string} />
          <InfoRow icon={Calendar} label={t('toolArtifact.periodEnd')} value={data.period_end as string} />
          <InfoRow icon={Building2} label={t('toolArtifact.cik')} value={data.cik as string} />
          <InfoRow icon={Layers} label={t('toolArtifact.sectionsExtracted')} value={data.sections_extracted as number} />
          {!!data.has_earnings_call && (
            <InfoRow icon={FileText} label={t('toolArtifact.earningsCall')} value={t('toolArtifact.included')} />
          )}
          {data.recent_8k_count != null && (
            <InfoRow icon={Newspaper} label={t('toolArtifact.recent8KFilings')} value={t('toolArtifact.nLast90Days', { count: data.recent_8k_count as number })} />
          )}
        </div>
      </div>

      {/* Embedded document — fills remaining height */}
      {proxyUrl && (
        <div className="flex flex-col flex-1 min-h-0">
          <div className="flex items-center justify-between mb-2 flex-shrink-0">
            <span className="text-xs font-medium uppercase tracking-wider" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('toolArtifact.secFilingDocument')}
            </span>
            <a
              href={data.source_url as string}
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 text-xs transition-colors hover:brightness-125"
              style={{ color: ACCENT, textDecoration: 'none' }}
            >
              {t('toolArtifact.openOnEdgar')}
              <ExternalLink className="h-3 w-3" />
            </a>
          </div>
          <div
            className="relative rounded-lg overflow-hidden flex-1 min-h-0"
            style={{ border: '1px solid var(--color-border-muted)' }}
          >
            {iframeLoading && (
              <div
                className="absolute inset-0 flex items-center justify-center"
                style={{ backgroundColor: 'var(--color-bg-overlay-strong)' }}
              >
                <div className="flex items-center gap-2">
                  <span aria-hidden="true" className="flex-shrink-0">
                    <Loader size={16} className="text-[color:var(--color-accent-primary)]" />
                  </span>
                  <span className="text-xs" style={{ color: TEXT_COLOR }}>{t('toolArtifact.loadingSecDocument')}</span>
                </div>
              </div>
            )}
            <iframe
              src={proxyUrl}
              title={`${data.symbol as string} ${data.filing_type as string} Filing`}
              className="w-full h-full"
              style={{
                border: 'none',
                backgroundColor: 'var(--color-bg-chart-placeholder)',
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
function EightKListView({ data }: FilingDataProps): React.ReactElement {
  const { t } = useTranslation();
  const filings = (data.filings || []) as Record<string, unknown>[];

  return (
    <div className="space-y-4">
      {/* Header */}
      <div>
        <div className="flex items-baseline gap-3 mb-1">
          <span
            className="text-xs font-bold px-2 py-0.5 rounded"
            style={{ backgroundColor: 'var(--color-accent-soft)', color: ACCENT }}
          >
            {data.symbol as string}
          </span>
          <span style={{ fontSize: '1rem', fontWeight: 700, color: 'var(--color-text-primary)' }}>
            {t('toolArtifact.8kFilings')}
          </span>
        </div>
        <span className="text-xs" style={{ color: TEXT_COLOR }}>
          {t('toolArtifact.nFilings', { count: data.filing_count as number, days: data.days_range as number })}
        </span>
      </div>

      {/* Filing cards */}
      {filings.map((filing, i) => (
        <div
          key={i}
          className="rounded-lg px-4 py-3"
          style={{
            backgroundColor: 'var(--color-bg-surface)',
            border: '1px solid var(--color-border-muted)',
          }}
        >
          {/* Date + press release indicator */}
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-2">
              <Calendar className="h-3.5 w-3.5" style={{ color: TEXT_COLOR }} />
              <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {filing.filing_date as string}
              </span>
              {!!filing.has_press_release && (
                <span
                  className="text-xs px-1.5 py-0.5 rounded"
                  style={{ backgroundColor: 'var(--color-profit-soft)', color: 'var(--color-profit)', fontSize: '0.625rem' }}
                >
                  {t('toolArtifact.pressRelease')}
                </span>
              )}
            </div>
            {!!filing.source_url && (
              <a
                href={`${API_BASE}/api/v1/sec-proxy/document?url=${encodeURIComponent(filing.source_url as string)}`}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1 text-xs transition-colors hover:brightness-125"
                style={{ color: ACCENT, textDecoration: 'none' }}
              >
                {t('toolArtifact.view')}
                <ExternalLink className="h-3 w-3" />
              </a>
            )}
          </div>

          {/* Item chips */}
          <div className="flex flex-wrap gap-1.5">
            {(filing.items as string[] | undefined)?.map((item, j) => (
              <ItemChip
                key={j}
                label={(filing.items_desc as string[] | undefined)?.[j] ? `${item}: ${(filing.items_desc as string[])[j]}` : item}
              />
            ))}
          </div>
        </div>
      ))}

      {filings.length === 0 && (
        <div className="text-sm py-4 text-center" style={{ color: TEXT_COLOR }}>
          {t('toolArtifact.no8KFilings', { days: data.days_range as number })}
        </div>
      )}
    </div>
  );
}

/**
 * SecFilingViewer routes between 10-K/10-Q and 8-K views.
 */
export default function SecFilingViewer({ data }: FilingDataProps): React.ReactElement | null {
  if (!data) return null;

  if (data.filing_type === '8-K') {
    return <EightKListView data={data} />;
  }

  return <AnnualQuarterlyView data={data} />;
}
