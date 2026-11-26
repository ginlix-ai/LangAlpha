import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { X } from 'lucide-react';
import { HeaderButton, ListError, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { SegmentedControl } from '@/components/ui/segmented-control';
import { useDriveMemos } from '@/hooks/useDrive';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { formatTimestamp, relativeTime } from '@/lib/format';
import { cn } from '@/lib/utils';
import { EmptyState } from '@/pages/Plugins/components/EmptyState';
import type { DriveMemo, DriveMemoQuery, DriveMemoSort, DriveMemoStatus } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { drivePathKey, parentOf } from '../drivePaths';
import { loadFailedLine } from '../driveRefusal';
import { memoState } from '../memoMarks';
import { whoLabel } from '../who';
import { DriveViews } from './DriveViews';
import { EntryGlyph } from './EntryGlyph';
import { MemoStatus, TickerChips } from './MemoBadges';
import { PaneFrame } from './PaneFrame';

const COLUMNS: { sort: DriveMemoSort; label: string; className: string }[] = [
  { sort: 'name', label: 'common.name', className: 'drive-memos-name' },
  { sort: 'status', label: 'drive:memos.columns.status', className: 'drive-memos-status' },
];

function MemoRow({ memo, compact, active, onTicker }: {
  memo: DriveMemo;
  compact: boolean;
  active: boolean;
  onTicker: (ticker: string) => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { open } = useDriveContext();
  const { entry } = memo;
  const folder = parentOf(entry.path);
  const when = relativeTime(entry.modified_at, locale, now);
  const who = whoLabel(t, entry.modified_by);
  // The list holds live memos only; a row whose mark went meanwhile shows none.
  const state = memoState(entry);
  const mark = state.kind === 'memo' ? state.mark : null;
  const status = mark ? <MemoStatus mark={mark} head={entry.version} /> : null;
  return (
    <div role="row" className={cn('drive-memos-row', compact && 'is-compact', active && 'is-active')}>
      {/* The table role sits on a wrapper so the opener is still announced as a button. */}
      <span role="cell" className="drive-memos-slot">
        <button type="button" className="drive-memos-open" onClick={() => open(entry)} aria-current={active ? 'true' : undefined}>
          <EntryGlyph name={entry.name} className="drive-row-icon" />
          <span className="drive-row-text">
            <span className="drive-row-name">{entry.name}</span>
            <span className="drive-row-sub">
              {compact
                ? [folder || t('drive:title'), when].join(' · ')
                : folder || t('drive:title')}
            </span>
          </span>
        </button>
      </span>
      {compact ? (
        <span role="cell" className="drive-memos-compact">
          {status}
          <TickerChips tickers={mark?.tickers ?? []} onPick={onTicker} />
        </span>
      ) : (
        <>
          <span role="cell" className="drive-memos-status">{status}</span>
          <span role="cell" className="drive-memos-tickers"><TickerChips tickers={mark?.tickers ?? []} onPick={onTicker} /></span>
          <span role="cell" className="drive-memos-revised" title={formatTimestamp(entry.modified_at, locale)}>
            {t('drive:memos.revisedBy', { who, when })}
          </span>
        </>
      )}
    </div>
  );
}

/**
 * Every memo in the Drive, whatever folder it sits in: its name and folder,
 * status, tickers, and who last revised it and when. Sorted by a column,
 * filtered by status or by a ticker picked from a row.
 */
export function MemosView({ openPath, compact }: { openPath: string | null; compact: boolean }) {
  const { t } = useTranslation();
  const { space } = useDriveContext();
  const [query, setQuery] = useState<DriveMemoQuery>({ sort: 'revised', order: 'desc', status: null, ticker: null });
  const memos = useDriveMemos(space, query);
  const rows = memos.data?.pages.flatMap((p) => p.memos) ?? [];
  const openKey = openPath === null ? null : drivePathKey(openPath);

  const sortBy = (sort: DriveMemoSort) => setQuery((q) => ({
    ...q,
    sort,
    // The same column again turns the order round; a new one starts where it reads best.
    order: q.sort === sort ? (q.order === 'asc' ? 'desc' : 'asc') : sort === 'revised' ? 'desc' : 'asc',
  }));
  const filtered = !!query.status || !!query.ticker;

  let body: React.ReactNode;
  if (memos.error && !memos.data) {
    body = <ListError onRetry={() => void memos.refetch()}>{loadFailedLine(memos.error, t)}</ListError>;
  } else if (!memos.data) {
    body = <ListSkeleton rows={5} />;
  } else if (rows.length === 0) {
    body = (
      <EmptyState
        message={filtered ? t('drive:memos.noneMatch') : t('drive:memos.none')}
      />
    );
  } else {
    // A sortable heading is a columnheader holding a native button, so it is
    // still announced as a button; the sort state belongs to the header.
    const header = (sort: DriveMemoSort, label: string, className: string) => (
      <span
        key={sort}
        role="columnheader"
        className="drive-memos-slot"
        aria-sort={query.sort === sort ? (query.order === 'asc' ? 'ascending' : 'descending') : undefined}
      >
        <button
          type="button"
          className={cn('drive-col', className, query.sort === sort && 'is-sorted')}
          onClick={() => sortBy(sort)}
        >
          {t(label)}
        </button>
      </span>
    );
    body = (
      <div role="table" aria-label={t('drive:memos.title')}>
        {!compact && (
          <div role="row" className="drive-memos-columns">
            {COLUMNS.map((c) => header(c.sort, c.label, c.className))}
            <span role="columnheader" className="drive-col drive-memos-tickers">{t('drive:memos.columns.tickers')}</span>
            {header('revised', 'drive:memos.columns.revised', 'drive-memos-revised')}
          </div>
        )}
        <div role="rowgroup">
          {rows.map((memo) => (
            <MemoRow
              key={memo.entry.id}
              memo={memo}
              compact={compact}
              active={openKey !== null && drivePathKey(memo.entry.path) === openKey}
              onTicker={(ticker) => setQuery((q) => ({ ...q, ticker }))}
            />
          ))}
        </div>
        {memos.hasNextPage && (
          <div className="drive-more">
            {memos.isFetchNextPageError && <span role="alert">{loadFailedLine(memos.error, t)}</span>}
            <HeaderButton variant="ghost" onClick={() => void memos.fetchNextPage()} disabled={memos.isFetchingNextPage}>
              {t('drive:loadMore')}
            </HeaderButton>
          </div>
        )}
      </div>
    );
  }

  return (
    <PaneFrame
      className={cn('drive-memos', compact && 'is-compact')}
      title={t('drive:title')}
      views={<DriveViews current="memos" />}
      below={(
        <div className="drive-memos-filters">
          <SegmentedControl<DriveMemoStatus | 'all'>
            size="compact"
            label={t('drive:memos.columns.status')}
            value={query.status ?? 'all'}
            onChange={(status) => setQuery((q) => ({ ...q, status: status === 'all' ? null : status }))}
            options={[
              { value: 'all', label: t('drive:memos.all') },
              { value: 'draft', label: t('drive:memo.status.draft') },
              { value: 'final', label: t('drive:memo.status.final') },
            ]}
          />
          {query.ticker && (
            <button
              type="button"
              className="drive-tag drive-ticker is-button is-filter"
              onClick={() => setQuery((q) => ({ ...q, ticker: null }))}
              aria-label={t('drive:memos.clearTicker', { ticker: query.ticker })}
            >
              {query.ticker}
              <X className="h-3 w-3" aria-hidden="true" />
            </button>
          )}
        </div>
      )}
    >
      <div className="drive-pane-pad">{body}</div>
    </PaneFrame>
  );
}
