import { useTranslation } from 'react-i18next';
import { SlidersHorizontal } from 'lucide-react';
import { cn } from '@/lib/utils';
import type { DriveMemoMark } from '@/types/drive';
import { revisedSince, type MemoView } from '../memoMarks';
import { MemoStatus, TickerChips } from './MemoBadges';

const TABS: { view: MemoView; label: string }[] = [
  { view: 'document', label: 'drive:memo.tabs.document' },
  { view: 'record', label: 'drive:memo.tabs.record' },
  { view: 'changes', label: 'drive:memo.tabs.changes' },
  { view: 'who', label: 'drive:memo.tabs.who' },
];

/**
 * Under a memo's name: its status, and once it was revised past its final the
 * way to that version and the way to make the newest one final; its tickers
 * and the way to change them; then its views. Who wrote what reads line by line, so it is offered on a
 * text memo only.
 */
export function MemoHeader({ mark, head, isText, view, onView, onEdit, onViewFinal, onMakeFinal, busy }: {
  mark: DriveMemoMark;
  head: number | null;
  isText: boolean;
  view: MemoView;
  onView: (view: MemoView) => void;
  onEdit: () => void;
  onViewFinal: () => void;
  onMakeFinal: () => void;
  busy: boolean;
}) {
  const { t } = useTranslation();
  return (
    <div className="drive-memo-band">
      <div className="drive-memo-line">
        <span className="drive-memo-facts">
          <MemoStatus mark={mark} head={head} full />
          {revisedSince(mark, head) !== null && (
            <>
              <button type="button" className="drive-link-button" onClick={onViewFinal}>{t('drive:memo.viewFinal')}</button>
              <button type="button" className="drive-link-button" onClick={onMakeFinal} disabled={busy}>
                {t('drive:memo.makeFinal', { seq: head })}
              </button>
            </>
          )}
        </span>
        <TickerChips tickers={mark.tickers} />
        <button type="button" className="drive-link-button drive-memo-edit" onClick={onEdit}>
          <SlidersHorizontal className="h-3 w-3" aria-hidden="true" />
          {t('drive:memo.editDetails')}
        </button>
      </div>
      <div className="drive-memo-tabs" role="tablist" aria-label={t('drive:memo.tabs.label')}>
        {TABS.map(({ view: tab, label }) => {
          const disabled = tab === 'who' && !isText;
          return (
            <button
              key={tab}
              type="button"
              role="tab"
              aria-selected={view === tab}
              disabled={disabled}
              title={disabled ? t('drive:memo.tabs.whoTextOnly') : undefined}
              className={cn('drive-tab', view === tab && 'is-active')}
              onClick={() => onView(tab)}
            >
              {t(label)}
            </button>
          );
        })}
      </div>
    </div>
  );
}
