import React from 'react';
import { useTranslation } from 'react-i18next';
import { readSecFiling, type CurrentReport, type InlineCardProps, type SecFiling } from './cardReaders';
import { itemNumber } from './cardWords';
import { TermCard, TermKV } from './TermCard';
import { useCardNarrow } from './termContext';
import { TermTable } from './TermTable';

const MAX_INLINE_8K = 3;

const FILING_KIND_KEY: Record<string, string> = {
  '10-K': 'toolArtifact.annualReport',
  '10-Q': 'toolArtifact.quarterlyReport',
};

export function InlineSecFilingCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const filing = readSecFiling(artifact);
  if (!filing) return null;
  return filing.kind === '8-K'
    ? <Inline8KCard filing={filing} onClick={onClick} />
    : <InlinePeriodicCard filing={filing} onClick={onClick} />;
}

function InlinePeriodicCard({
  filing,
  onClick,
}: {
  filing: Extract<SecFiling, { kind: 'periodic' }>;
  onClick?: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const narrow = useCardNarrow();
  const { form, filingDate, periodEnd, cik, sections, recent8Ks, sourceUrl, earningsCall } = filing;
  const kindKey = form ? FILING_KIND_KEY[form] : undefined;
  // A phone's edge has room for the form and the date, not the kind too.
  const sub = kindKey && !narrow ? `${form} · ${t(kindKey)}` : form;

  return (
    <TermCard
      title={filing.symbol}
      sub={sub}
      meta={filingDate && t('toolArtifact.filedOn', { date: filingDate })}
      onClick={onClick}
    >
      <TermKV
        cols={narrow ? 2 : 3}
        pairs={[
          !!periodEnd && [t('toolArtifact.periodEnd'), periodEnd],
          !!cik && [t('toolArtifact.cik'), cik],
          sections != null && [t('toolArtifact.sections'), String(sections)],
          earningsCall && [
            t('toolArtifact.earningsCall'),
            // Half a phone's width holds the label or the word, not both.
            narrow ? (
              <span
                key="included"
                role="img"
                aria-label={t('toolArtifact.included')}
                title={t('toolArtifact.included')}
                className="term-yes"
              >
                ✓
              </span>
            ) : (
              t('toolArtifact.included')
            ),
          ],
          recent8Ks != null && [t('toolArtifact.recent8Ks'), String(recent8Ks)],
          !!sourceUrl && [
            t('toolArtifact.source'),
            <a
              key="edgar"
              className="term-out"
              href={sourceUrl}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(e) => e.stopPropagation()}
              title={t('toolArtifact.viewOnEdgar')}
            >
              {t('toolArtifact.edgarLink')}
            </a>,
          ],
        ]}
      />
    </TermCard>
  );
}

/** Items a row names before the rest become a count. */
const MAX_ROW_ITEMS = 2;

/**
 * An 8-K's items on one line: each number in full, each description in what
 * room is left, so a long description is cut before a later item's number is.
 * Past two, the rest are counted rather than cut off unseen.
 */
function FilingItems({ items, descs }: { items: string[]; descs: string[] }): React.ReactElement {
  const more = items.length - MAX_ROW_ITEMS;
  return (
    <span className="term-items">
      {items.slice(0, MAX_ROW_ITEMS).map((item, j) => (
        <React.Fragment key={j}>
          {j > 0 && <span className="term-t3 term-items-sep">{' · '}</span>}
          <span className="term-item">
            <span className="term-code">{itemNumber(item)}</span>
            {descs[j] && <span className="term-t2 term-item-desc">{descs[j]}</span>}
          </span>
        </React.Fragment>
      ))}
      {more > 0 && <span className="term-t3 term-items-more">+{more}</span>}
    </span>
  );
}

function Inline8KCard({
  filing,
  onClick,
}: {
  filing: Extract<SecFiling, { kind: '8-K' }>;
  onClick?: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const sub = filing.days
    ? `${t('toolArtifact.8kFilings')} · ${t('toolArtifact.nDays', { count: filing.days })}`
    : t('toolArtifact.8kFilings');

  return (
    <TermCard title={filing.symbol} sub={sub} meta={t('toolArtifact.filingCount', { count: filing.count })} onClick={onClick}>
      <TermTable<CurrentReport>
        rows={filing.filings}
        limit={MAX_INLINE_8K}
        total={filing.count}
        more={(n) => t('toolArtifact.nMoreFilings', { count: n })}
        rowKey={(_, i) => i}
        columns={[
          { key: 'filed', label: t('toolArtifact.termHead.filed'), cell: (f) => f.filingDate },
          {
            key: 'items',
            label: t('toolArtifact.termHead.items'),
            fill: 'clip',
            title: (f) => f.items.map((item, j) => (f.descs[j] ? `${item}: ${f.descs[j]}` : item)).join('\n'),
            cell: (f) => <FilingItems items={f.items} descs={f.descs} />,
          },
          {
            key: 'press',
            label: t('toolArtifact.termHead.press'),
            num: true,
            cell: (f) =>
              f.pressRelease ? (
                <span className="term-yes">{t('toolArtifact.pressReleaseShort')}</span>
              ) : (
                <span className="term-t3" aria-hidden="true">·</span>
              ),
          },
        ]}
      />
    </TermCard>
  );
}
