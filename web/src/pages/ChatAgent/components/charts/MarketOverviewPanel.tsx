import React from 'react';
import { useTranslation } from 'react-i18next';
import { formatCalendarDay } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { text, unwrapMarketOverview, type PanelProps } from './cardReaders';
import { MarketIndicesChart, SectorPerformanceChart } from './MarketOverviewParts';
import { TermPanelHead } from './TermPanel';

/** The composite `get_market_overview` answer: the indices and sectors panels under one head. */
export function MarketOverviewPanel({ data }: PanelProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const { indicesArtifact, sectorsArtifact, hasIndices, hasSectors } = unwrapMarketOverview(data);
  const date = text(data.date);
  return (
    <>
      <TermPanelHead
        title={t('toolArtifact.marketOverview')}
        sub={text(data.region)?.toUpperCase()}
        aside={date ? formatCalendarDay(date, locale) : undefined}
      />
      {hasIndices && <MarketIndicesChart data={indicesArtifact!} />}
      {hasSectors && <SectorPerformanceChart data={sectorsArtifact!} />}
    </>
  );
}
