import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { api } from '../../../../api/client';
import { readSecFiling, type PanelProps, type SecFiling } from './cardReaders';
import { itemNumber } from './cardWords';
import { TermCard, TermKV, TermTag } from './TermCard';
import { TermLink, TermPanelHead } from './TermPanel';

const API_BASE = api.defaults.baseURL;

const proxied = (url: string): string =>
  `${API_BASE}/api/v1/sec-proxy/document?url=${encodeURIComponent(url)}`;

/** A 10-K or 10-Q: what the agent read of it, over the document itself. */
function AnnualQuarterlyView({ filing }: { filing: Extract<SecFiling, { kind: 'periodic' }> }): React.ReactElement {
  const { t } = useTranslation();
  const [iframeLoading, setIframeLoading] = useState(true);
  const { symbol, form, filingDate, periodEnd, cik, sections, recent8Ks, sourceUrl, earningsCall } = filing;

  return (
    <>
      <TermPanelHead
        title={symbol}
        sub={t('toolArtifact.filing', { type: form })}
        aside={filingDate ? <span className="term-t3">{t('toolArtifact.filedOn', { date: filingDate })}</span> : undefined}
      />
      <TermCard title={t('toolArtifact.termHead.filing')}>
        <TermKV
          cols={2}
          pairs={[
            periodEnd != null && [t('toolArtifact.periodEnd'), periodEnd],
            cik != null && [t('toolArtifact.cik'), cik],
            sections != null && [t('toolArtifact.sectionsExtracted'), String(sections)],
            earningsCall && [t('toolArtifact.earningsCall'), t('toolArtifact.included')],
            recent8Ks != null && [t('toolArtifact.recent8KFilings'), t('toolArtifact.nLast90Days', { count: recent8Ks })],
          ]}
        />
      </TermCard>
      {sourceUrl && (
        <TermCard
          title={t('toolArtifact.secFilingDocument')}
          meta={<TermLink href={sourceUrl}>{t('toolArtifact.openOnEdgar')}</TermLink>}
          className="term-panel-fill"
        >
          <div className="relative flex-1 min-h-0">
            {iframeLoading && (
              <div className="absolute inset-0 flex items-center justify-center gap-2">
                <Loader size={16} className="text-(--color-term-text-tertiary)" />
                <span className="term-t3">{t('toolArtifact.loadingSecDocument')}</span>
              </div>
            )}
            <iframe
              src={proxied(sourceUrl)}
              title={`${symbol} ${form} Filing`}
              className="w-full h-full"
              style={{ border: 'none', background: 'var(--color-bg-chart-placeholder)' }}
              onLoad={() => setIframeLoading(false)}
              sandbox="allow-same-origin allow-scripts"
            />
          </div>
        </TermCard>
      )}
    </>
  );
}

/** Recent 8-Ks: each filing a frame dated on its edge, every item in full. */
function EightKListView({ filing }: { filing: Extract<SecFiling, { kind: '8-K' }> }): React.ReactElement {
  const { t } = useTranslation();

  return (
    <>
      <TermPanelHead
        title={filing.symbol}
        sub={t('toolArtifact.8kFilings')}
        aside={<span className="term-t3">{t('toolArtifact.nFilings', { count: filing.count, days: filing.days })}</span>}
      />
      {filing.filings.map((report, i) => (
        <TermCard
          key={i}
          title={report.filingDate}
          meta={
            <span style={{ display: 'inline-flex', alignItems: 'baseline', gap: '1.25ch' }}>
              {report.pressRelease && <TermTag>{t('toolArtifact.pressRelease')}</TermTag>}
              {report.sourceUrl && <TermLink href={proxied(report.sourceUrl)}>{t('toolArtifact.view')}</TermLink>}
            </span>
          }
        >
          <div style={{ display: 'grid', gridTemplateColumns: 'max-content 1fr', columnGap: '2ch', rowGap: 4 }}>
            {report.items.map((item, j) => (
              <React.Fragment key={j}>
                <span>{itemNumber(item)}</span>
                <span className="term-t2 term-word">{report.descs[j] ?? ''}</span>
              </React.Fragment>
            ))}
          </div>
        </TermCard>
      ))}
      {filing.filings.length === 0 && (
        <div className="term-t3" style={{ padding: '1rem 0', textAlign: 'center' }}>
          {t('toolArtifact.no8KFilings', { days: filing.days })}
        </div>
      )}
    </>
  );
}

/** Routes a filing to its view: the 8-K list, or a 10-K or 10-Q document. */
export default function SecFilingViewer({ data }: PanelProps): React.ReactElement | null {
  const filing = readSecFiling(data);
  if (!filing) return null;
  return filing.kind === '8-K' ? <EightKListView filing={filing} /> : <AnnualQuarterlyView filing={filing} />;
}
