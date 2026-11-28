import { useTranslation } from 'react-i18next';
import { RotateCcw } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { relativeTime } from '@/lib/format';
import type { DriveEntry, DriveVersion } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { memoState, type MemoView } from '../memoMarks';
import type { DriveEditor } from '../useDriveEditor';
import { whoLabel } from '../who';
import { DriveBar } from './DriveBar';
import { UndoSave } from './UndoSave';

/**
 * The bands under an open file's header, each when it has something to say:
 * an editor's conflict or a change under it, a past version on screen, the
 * undo of a file's last save, a record that was a memo's.
 */
export function FileBars({ entry, view, viewed, editor, compare, onRestore, onBackToDocument }: {
  entry: DriveEntry | undefined;
  view: MemoView;
  /** The past version the document shows in place of the head. */
  viewed: DriveVersion | null;
  editor: DriveEditor;
  /** The editor's comparison with theirs, for the conflict it was opened on. */
  compare: { on: boolean; toggle: () => void };
  onRestore: () => void;
  /** Back to the file's current document. */
  onBackToDocument: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { actions } = useDriveContext();
  const { session } = editor;
  const state = memoState(entry);
  const theirsBy = whoLabel(t, entry?.modified_by);
  return (
    <>
      {session?.conflict && editor.theirs ? (
        <DriveBar
          role="alert"
          actions={(
            <>
              <HeaderButton variant="ghost" onClick={compare.toggle}>
                {compare.on ? t('drive:conflict.hideCompare') : t('drive:conflict.compare')}
              </HeaderButton>
              <HeaderButton onClick={editor.takeTheirs}>{t('drive:conflict.takeTheirs')}</HeaderButton>
              <HeaderButton variant="primary" onClick={editor.keepMine} disabled={session.save !== 'idle'}>{t('drive:conflict.keepMine')}</HeaderButton>
            </>
          )}
        >
          <strong>{t('drive:conflict.title')}</strong>{' '}{t('drive:conflict.body', { who: theirsBy })}
        </DriveBar>
      ) : editor.behind && editor.dirty && (
        <DriveBar role="status" actions={<HeaderButton onClick={editor.review}>{t('drive:edit.review')}</HeaderButton>}>
          {t('drive:edit.changedUnder', { who: theirsBy })}
        </DriveBar>
      )}
      {!session && viewed && view === 'document' && (
        <DriveBar
          actions={(
            <>
              <HeaderButton icon={RotateCcw} onClick={onRestore} disabled={actions.pending.restoreVersion} title={t('drive:versions.restoreHint')}>
                {t('drive:actions.restore')}
              </HeaderButton>
              <HeaderButton variant="ghost" onClick={onBackToDocument}>{t('drive:versions.backToCurrent')}</HeaderButton>
            </>
          )}
        >
          {t('drive:versions.viewing', { seq: viewed.seq, when: relativeTime(viewed.created_at, locale, now), who: whoLabel(t, viewed.actor) })}
        </DriveBar>
      )}
      {!session && entry && state.kind !== 'memo' && view === 'document' && <UndoSave entry={entry} />}
      {entry && state.kind === 'former' && view === 'record' && (
        <DriveBar role="status" actions={<HeaderButton variant="ghost" onClick={onBackToDocument}>{t('drive:record.backToFile')}</HeaderButton>}>
          {t('drive:record.wasMemo')}
        </DriveBar>
      )}
    </>
  );
}
