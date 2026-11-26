import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { shortMonthDay } from '@/lib/format';
import type { DriveMemoMark } from '@/types/drive';
import { revisedSince } from '../memoMarks';

/** The neutral "Memo" pill beside a memo's name. */
export function MemoChip() {
  const { t } = useTranslation();
  return <span className="drive-tag is-outline">{t('drive:memo.chip')}</span>;
}

/** A memo's tickers; with `onPick`, each one filters by it. */
export function TickerChips({ tickers, onPick }: { tickers: readonly string[]; onPick?: (ticker: string) => void }) {
  if (tickers.length === 0) return null;
  return (
    <span className="drive-tickers">
      {tickers.map((ticker) => (onPick ? (
        <button key={ticker} type="button" className="drive-tag drive-ticker is-button" onClick={() => onPick(ticker)}>{ticker}</button>
      ) : (
        <span key={ticker} className="drive-tag drive-ticker">{ticker}</span>
      )))}
    </span>
  );
}

/**
 * A memo's status as words: Draft, or Final with the version it pinned, and
 * the amber dot once the memo was revised past that version. `full` (the
 * memo's own header) adds the pin's date and says "revised since" in words;
 * a list row says it to a screen reader only.
 *
 * Whether final pins a version is not yet ruled on by the owner. This is the
 * one place that shows the pin: drawn as a plain "Final", nothing else on the
 * page needs to change.
 */
export function MemoStatus({ mark, head, full = false }: { mark: DriveMemoMark; head: number | null; full?: boolean }) {
  const { t } = useTranslation();
  const locale = useLocale();
  if (mark.status === 'draft') return <span className="drive-memo-status">{t('drive:memo.status.draft')}</span>;
  if (mark.status !== 'final') return null;
  const pinned = mark.final_seq;
  if (pinned === null) return <span className="drive-memo-status">{t('drive:memo.status.final')}</span>;
  const label = full && mark.final_at
    ? t('drive:memo.finalAtOn', { seq: pinned, date: shortMonthDay(new Date(mark.final_at), locale) })
    : t('drive:memo.finalAt', { seq: pinned });
  return (
    <span className="drive-memo-status">
      <span>{label}</span>
      {revisedSince(mark, head) !== null && (
        <span className="drive-memo-revised" title={full ? undefined : t('drive:memo.revisedSince')}>
          {/* The one amber mark on a memo: an annotation that the text moved on past what was signed off. */}
          <span className="drive-memo-dot" aria-hidden="true" />
          <span className={full ? undefined : 'sr-only'}>{t('drive:memo.revisedSince')}</span>
        </span>
      )}
    </span>
  );
}
