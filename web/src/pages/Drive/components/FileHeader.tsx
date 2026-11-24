import { useTranslation } from 'react-i18next';
import { Code2, Eye, History, MoreHorizontal, Pencil } from 'lucide-react';
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
import { cn } from '@/lib/utils';
import type { DriveEntry } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { nameOf, parentOf } from '../drivePaths';
import type { DriveEditor } from '../useDriveEditor';
import { whoLabel } from '../who';
import { EntryGlyph } from './EntryGlyph';
import { EntryMenuItems } from './EntryMenuItems';
import { BackButton, ICON_BUTTON } from './PaneFrame';

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
 * size), then what can be done: edit, see its history, the entry's menu; or,
 * while editing, the save.
 */
export function FileHeader({ path, entry, mobile, editor, canEdit, html, historyOpen, onToggleHistory }: {
  path: string;
  entry: DriveEntry | undefined;
  mobile: boolean;
  editor: DriveEditor;
  /** A text file at its head, loaded with its hash. */
  canEdit: boolean;
  /** An HTML file at its head: whether its source is shown, and the switch. */
  html: { source: boolean; toggle: () => void } | null;
  historyOpen: boolean;
  onToggleHistory: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { go, leaveEditor } = useDriveContext();
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
    <div className="drive-file-head">
      {mobile && <BackButton label={t('drive:backToFolder')} onClick={() => go({ dir: parentOf(path) })} />}
      <EntryGlyph name={name} className="drive-file-glyph" />
      <div className="drive-file-title">
        <h2 className="drive-file-name" title={path}>{name}</h2>
        {meta && <p className="drive-file-meta" title={entry ? formatTimestamp(entry.modified_at, locale) : undefined}>{meta}</p>}
      </div>
      {entry && (
        <div className="drive-file-actions">
          {session ? (
            <>
              <span className="drive-save-state" aria-live="polite">{mark}</span>
              <HeaderButton variant="ghost" onClick={() => leaveEditor()}>
                {dirty ? t('common.cancel') : t('common.done')}
              </HeaderButton>
              <HeaderButton variant="primary" onClick={() => void editor.save()} disabled={!dirty || session.save !== 'idle' || session.conflict}>
                {t('common.save')}
              </HeaderButton>
            </>
          ) : (
            <>
              {html && (
                <HeaderButton variant="ghost" icon={html.source ? Eye : Code2} onClick={html.toggle}>
                  {html.source ? t('drive:file.page') : t('drive:file.source')}
                </HeaderButton>
              )}
              {canEdit && (
                <HeaderButton icon={Pencil} onClick={editor.start}>{t('drive:actions.edit')}</HeaderButton>
              )}
              <HeaderButton
                variant={historyOpen ? 'secondary' : 'ghost'}
                icon={History}
                onClick={onToggleHistory}
                aria-pressed={historyOpen}
                className={cn(historyOpen && 'drive-btn-on')}
              >
                <span className={cn(mobile && 'sr-only')}>{t('drive:history.title')}</span>
              </HeaderButton>
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
  );
}
