import { useTranslation } from 'react-i18next';
import { Code2, Eye, MoreHorizontal, Pencil } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { Button } from '@/components/ui/button';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { formatBytes, formatTimestamp, relativeTime } from '@/lib/format';
import { isImeComposing } from '@/lib/ime';
import { Input } from '@/components/ui/input';
import type { DriveEntry } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { nameOf, parentOf } from '../drivePaths';
import { memoState } from '../memoMarks';
import type { DriveEditor } from '../useDriveEditor';
import { whoLabel } from '../who';
import { EntryGlyph } from './EntryGlyph';
import { EntryMenuItems } from './EntryMenuItems';
import { EntryName } from './EntryName';
import { MemoChip } from './MemoBadges';
import { Crumbs } from './Crumbs';
import { BackButton, ICON_BUTTON, PaneAction } from './PaneFrame';

/** The save's quiet mark beside the editor's buttons. */
function useSaveMark(editor: DriveEditor): string {
  const { t } = useTranslation();
  const { session, dirty } = editor;
  if (!session) return '';
  if (session.save === 'retrying') return t('drive:retrying');
  if (session.save === 'saving') return t('common.saving');
  if (dirty) return t('drive:edit.unsaved');
  return session.savedAt ? t('common.saved') : '';
}

/**
 * The open file's name and where it stands (version, who changed it last,
 * size), then what can be done: edit, the host's action, the entry's menu;
 * or, while editing, the save, beside which a memo asks what changed. Under
 * it, a memo's band. Where the folder list is not beside it in the panel,
 * the folders it sits in sit above its name, each a way to that folder.
 */
export function FileHeader({ path, entry, mobile, editor, edit, html, comments, children }: {
  path: string;
  entry: DriveEntry | undefined;
  mobile: boolean;
  editor: DriveEditor;
  /**
   * Edit, offered for a text file at its head from the first frame, so it never
   * arrives late and shifts the actions; `ready` once its text is read with
   * the hash an edit is saved over.
   */
  edit: 'none' | 'waiting' | 'ready';
  /** An HTML file at its head: whether its source is shown, and the switch. */
  html: { source: boolean; toggle: () => void } | null;
  /** The file's comment controls (`CommentTools`), where its current content takes comments. */
  comments?: React.ReactNode;
  /** Under the name row, inside the header: a memo's status and views. */
  children?: React.ReactNode;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { back, backTo, leaveEditor, pageAction, fileCrumbs } = useDriveContext();
  const backLabel = backTo === 'memos' ? t('drive:backToMemos') : t('drive:backToFolder');
  const mark = useSaveMark(editor);
  const { session, dirty } = editor;
  const name = entry?.name ?? nameOf(path);
  const meta = entry
    ? [
        entry.version ? t('drive:file.version', { seq: entry.version }) : null,
        t('drive:file.changed', { when: relativeTime(entry.modified_at, locale, now), who: whoLabel(t, entry.modified_by) }),
        entry.size != null ? formatBytes(entry.size, locale) : null,
      ].filter(Boolean).join(' · ')
    : '';

  return (
    <div className="drive-file-top">
      {fileCrumbs && <Crumbs dir={parentOf(path)} compact here={false} className="drive-file-crumbs" />}
      <div className="drive-file-head">
        {/* The crumbs lead to folders; a file opened from the memo list keeps its way back there. */}
        {mobile && (!fileCrumbs || backTo === 'memos') && <BackButton label={backLabel} onClick={back} />}
        <EntryGlyph name={name} className="drive-file-glyph" />
        <div className="drive-file-title">
          <div className="drive-file-name-row">
            <h2 className="drive-file-name"><EntryName name={name} title={path} /></h2>
            {memoState(entry).kind === 'memo' && <MemoChip />}
          </div>
          {/* The line is there before the entry is, so the header never grows under the reader. */}
          <p className="drive-file-meta" title={entry ? formatTimestamp(entry.modified_at, locale) : undefined}>{meta || '\u00a0'}</p>
        </div>
        {entry && (
          <div className="drive-file-actions">
            {session ? (
              <>
                <span className="drive-save-state" aria-live="polite">{mark}</span>
                {editor.asksWhy && (
                  <Input
                    value={session.note}
                    onChange={(e) => editor.setNote(e.target.value)}
                    maxLength={500}
                    placeholder={t('drive:edit.whatChanged')}
                    aria-label={t('drive:edit.whatChanged')}
                    className="drive-save-note"
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' && !isImeComposing(e) && editor.canSave) {
                        e.preventDefault();
                        void editor.save();
                      }
                    }}
                  />
                )}
                <HeaderButton variant="ghost" onClick={() => leaveEditor()}>
                  {dirty ? t('common.cancel') : t('common.done')}
                </HeaderButton>
                <HeaderButton variant="primary" onClick={() => void editor.save()} disabled={!editor.canSave}>
                  {t('common.save')}
                </HeaderButton>
              </>
            ) : (
              <>
                {comments}
                {html && (
                  <PaneAction
                    variant="ghost"
                    icon={html.source ? Eye : Code2}
                    label={html.source ? t('drive:file.page') : t('drive:file.source')}
                    compact={mobile}
                    onClick={html.toggle}
                  />
                )}
                {edit !== 'none' && (
                  // On a phone the name needs the row: the action keeps its icon.
                  <PaneAction icon={Pencil} label={t('drive:actions.edit')} compact={mobile} onClick={editor.start} disabled={edit === 'waiting'} />
                )}
                {pageAction}
                <DropdownMenu>
                  <DropdownMenuTrigger asChild>
                    <Button type="button" variant="ghost" size="icon" className={ICON_BUTTON} aria-label={t('drive:actions.more', { name })}>
                      <MoreHorizontal className="h-4 w-4" aria-hidden="true" />
                    </Button>
                  </DropdownMenuTrigger>
                  <DropdownMenuContent align="end">
                    <EntryMenuItems entry={entry} parts={{ Item: DropdownMenuItem, Separator: DropdownMenuSeparator }} />
                  </DropdownMenuContent>
                </DropdownMenu>
              </>
            )}
          </div>
        )}
      </div>
      {children}
    </div>
  );
}
