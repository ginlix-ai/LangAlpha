import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { formatTimestamp, relativeTime } from '@/lib/format';
import type { DriveEntry } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { undoDeadline } from '../memoMarks';
import { whoLabel } from '../who';
import { ActorSentence } from './Actor';

const DAY_MS = 86_400_000;

/**
 * A quiet line under a file that is not a memo while Undo is offered for its
 * last save: who saved it, when and where, and the way back. Undone, the save
 * it undid becomes the kept copy, so a second undo puts it back.
 */
export function UndoSave({ entry }: { entry: DriveEntry }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { actions } = useDriveContext();
  const until = undoDeadline(entry, now);
  if (until === null) return null;
  const by = entry.modified_by;
  const when = relativeTime(entry.modified_at, locale, now);
  const days = Math.max(1, Math.ceil((until - now) / DAY_MS));
  const you = by?.kind === 'person';
  const saved = by?.thread_id ? (
    <ActorSentence actor={by} threadId={by.thread_id} i18nKey={you ? 'drive:undo.savedByYouIn' : 'drive:undo.savedByIn'} values={{ when }} />
  ) : you ? t('drive:undo.savedByYou', { when }) : t('drive:undo.savedBy', { who: whoLabel(t, by), when });
  return (
    <div className="drive-undo" role="status">
      <span className="drive-undo-text" title={formatTimestamp(entry.modified_at, locale)}>{saved}</span>
      <span className="drive-undo-text" title={formatTimestamp(new Date(until).toISOString(), locale)}>
        {t('drive:undo.kept', { count: days })}
        {' · '}
        <button type="button" className="drive-link-button" onClick={() => void actions.undoSave(entry)} disabled={actions.pending.undoSave}>
          {t('drive:undo.action')}
        </button>
      </span>
    </div>
  );
}
