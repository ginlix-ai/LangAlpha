import React from 'react';
import { useTranslation } from 'react-i18next';
import { readWebSearch, type InlineCardProps } from './cardReaders';
import { TermCard } from './TermCard';
import { TermTable } from './TermTable';

const MAX_INLINE_RESULTS = 4;

export function InlineWebSearchCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const { query, hits } = readWebSearch(artifact);
  if (!hits.length) return null;

  return (
    <TermCard title={t('toolArtifact.tool.webSearch')} meta={t('toolArtifact.nResults', { count: hits.length })} onClick={onClick}>
      {query && (
        <div className="term-t2 term-query">
          <span className="term-t3">{'> '}</span>&quot;<span>{query}</span>&quot;
        </div>
      )}
      <TermTable
        rows={hits}
        limit={MAX_INLINE_RESULTS}
        more={(n) => t('toolArtifact.nMoreResults', { count: n })}
        rowKey={(_, i) => i}
        columns={[
          { key: 'n', className: 'term-t3', cell: (_, i) => String(i + 1).padStart(2, '0') },
          { key: 'title', fill: 'clip', cell: (hit) => hit.title || hit.domain },
          // A long host gives way to the title on a phone.
          { key: 'host', num: true, className: 'term-t3', cell: (hit) => <span className="term-host">{hit.domain}</span> },
        ]}
      />
    </TermCard>
  );
}
