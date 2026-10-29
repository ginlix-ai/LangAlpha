import React from 'react';
import { useTranslation } from 'react-i18next';
import { formatMoney } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { readOptionsChain, type InlineCardProps } from './cardReaders';
import { expiryLabeler, formatVolume, moneyPlaces, optionsEdge, retrievedTitle } from './cardWords';
import { TermCard, TermChange, TermStatus, TermTag } from './TermCard';
import { useCardNarrow } from './termContext';
import { TermTable } from './TermTable';

const MAX_INLINE_ROWS = 5;

/**
 * A chain's first contracts by strike. What every row shares goes on the
 * edge instead of a column: the type in the sub-title, the expiry at the far
 * end. A chain saved before the artifact carried session prices still lists
 * its strikes, with no price columns.
 */
export function InlineOptionsChainCard({ artifact, onClick, toolArgs }: InlineCardProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const chain = readOptionsChain(artifact, toolArgs);
  const edge = optionsEdge(chain, t, locale, narrow);
  const { rows } = chain;
  const showExp = chain.expiries.length > 1;
  const showType = chain.types.length > 1;
  const expiryOf = expiryLabeler(chain.expiries, locale);
  const money = (n: number): string => formatMoney(n, chain.currency, locale, { decimals: moneyPlaces(n) });

  return (
    <TermCard
      title={edge.title}
      sub={edge.sub}
      meta={[
        chain.failed && <TermStatus key="failed" tone="loss">{t('common.failed')}</TermStatus>,
        edge.status && <TermTag key="status">{edge.status}</TermTag>,
        edge.expiry,
      ]}
      metaTitle={retrievedTitle(t, chain.asOfTs, locale)}
      onClick={onClick}
    >
      {/* An error or an empty chain keeps its call in the transcript; the
          panel behind it holds the tool's own words for why. */}
      {!rows.length && <div className="term-t3">{t('toolArtifact.noOptionsData')}</div>}
      <TermTable
        rows={rows}
        limit={MAX_INLINE_ROWS}
        more={(n) => t('toolArtifact.nMoreContracts', { count: n })}
        rowKey={(row, i) => row.ticker || i}
        // The last of the contract's terms takes the spare width, so the figures sit right.
        columns={[
          { key: 'strike', label: t('toolArtifact.termHead.strike'), fill: 'grow', className: 'term-strong', cell: (r) => (r.strike != null ? money(r.strike) : '') },
          { key: 'exp', label: t('toolArtifact.termHead.exp'), fill: 'grow', className: 'term-t2', show: showExp, cell: (r) => expiryOf(r.expiry) },
          {
            key: 'type',
            label: t('toolArtifact.termHead.type'),
            fill: 'grow',
            className: 'term-t2 term-label',
            show: showType,
            cell: (r) => (r.type ? t(`toolArtifact.optionType.${r.type}`) : ''),
          },
          { key: 'last', label: t('toolArtifact.termHead.last'), num: true, show: rows.some((r) => r.last != null), cell: (r) => (r.last != null ? money(r.last) : '') },
          { key: 'change', label: t('toolArtifact.termHead.change'), num: true, show: rows.some((r) => r.changePct != null), cell: (r) => <TermChange value={r.changePct} /> },
          {
            key: 'vol',
            label: t('toolArtifact.termHead.vol'),
            num: true,
            className: 'term-t2',
            // A phone keeps the contract's terms and its price; volume needs
            // the width only when both terms take a column.
            show: rows.some((r) => r.volume != null) && !(narrow && showExp && showType),
            cell: (r) => (r.volume != null ? formatVolume(r.volume, locale) : ''),
          },
        ]}
      />
    </TermCard>
  );
}
