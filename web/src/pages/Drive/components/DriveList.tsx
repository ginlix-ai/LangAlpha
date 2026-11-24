import React, { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { FolderPlus, FolderUp, Trash2, Upload } from 'lucide-react';
import { HeaderButton, ListError, ListSkeleton } from '@/components/mcp/McpPrimitives';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { useLocale } from '@/hooks/useLocale';
import { formatBytes, integer } from '@/lib/format';
import { cn } from '@/lib/utils';
import { EmptyState } from '@/pages/Plugins/components/EmptyState';
import type { DriveEntry, DriveUsage } from '@/types/drive';
import { canKeepHandles, pickWithHandles } from '@/lib/fileHandles';
import { useDriveContext } from '../driveContext';
import { crumbsOf, drivePathKey, nameOf, sortEntries, type SortKey } from '../drivePaths';
import { useDriveDrop, type DriveDrop } from '../useDriveDrop';
import { pickedFiles, type PickedFile } from '../useDriveUpload';
import { DriveRow } from './DriveRow';
import { PaneFrame } from './PaneFrame';

type Upload = (files: PickedFile[], dir: string, folders?: string[]) => void;

/** The hidden file and folder pickers, opened from the upload menu and the empty state. */
function useFilePickers(dir: string, upload: Upload) {
  const files = useRef<HTMLInputElement>(null);
  const folder = useRef<HTMLInputElement>(null);
  const onChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    upload(pickedFiles(e.target.files), dir);
    e.target.value = '';
  };
  const inputs = (
    <>
      <input ref={files} type="file" multiple hidden onChange={onChange} />
      {/* React passes unknown attributes through; the folder picker is not in its types. */}
      <input ref={folder} type="file" hidden {...{ webkitdirectory: '', directory: '' }} onChange={onChange} />
    </>
  );
  // Where the browser can keep a handle to each file, its own picker gives one, so an
  // upload the page loses (a reload, a closed tab) can read the file again with one click.
  const pickFiles = async () => {
    if (!canKeepHandles()) return files.current?.click();
    const picked = await pickWithHandles(true);
    if (picked) upload(picked.map(({ file, handle }) => ({ file, relPath: file.name, handle })), dir);
  };
  return { inputs, pickFiles: () => void pickFiles(), pickFolder: () => folder.current?.click() };
}

function Crumbs({ dir, drop }: { dir: string; drop: DriveDrop }) {
  const { t } = useTranslation();
  const { go } = useDriveContext();
  const crumbs = [{ name: t('drive:title'), path: '' }, ...crumbsOf(dir)];
  return (
    <nav className="drive-crumbs" aria-label={t('drive:crumbs')}>
      {crumbs.map((c, i) => {
        const last = i === crumbs.length - 1;
        const { isDrop, ...handlers } = last ? { isDrop: false } : drop.targetProps(c.path, `crumb:${c.path}`, 'rows');
        return (
          <React.Fragment key={c.path || '/'}>
            {i > 0 && <span className="drive-crumb-sep" aria-hidden="true">/</span>}
            <button
              type="button"
              className={cn('drive-crumb', last && 'is-here', isDrop && 'is-drop')}
              aria-current={last ? 'page' : undefined}
              onClick={() => go({ dir: c.path })}
              {...handlers}
            >
              {c.name}
            </button>
          </React.Fragment>
        );
      })}
    </nav>
  );
}

const SORTS: { key: SortKey; label: string; className: string }[] = [
  { key: 'name', label: 'common.name', className: 'drive-col-name' },
  { key: 'modified', label: 'drive:columns.modified', className: 'drive-col-when' },
  { key: 'size', label: 'drive:columns.size', className: 'drive-col-size' },
];

function Columns({ sort, onSort }: { sort: SortKey; onSort: (key: SortKey) => void }) {
  const { t } = useTranslation();
  return (
    <div className="drive-columns" role="presentation">
      {SORTS.map((s) => (
        <React.Fragment key={s.key}>
          {s.key === 'size' && <span className="drive-col drive-col-who">{t('drive:columns.by')}</span>}
          <button
            type="button"
            className={cn('drive-col', s.className, sort === s.key && 'is-sorted')}
            onClick={() => onSort(s.key)}
            aria-pressed={sort === s.key}
          >
            {t(s.label)}
          </button>
        </React.Fragment>
      ))}
    </div>
  );
}

function ListFoot({ usage }: { usage: DriveUsage | null }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { go } = useDriveContext();
  return (
    <div className="drive-pane-foot">
      <button type="button" className="drive-foot-link" onClick={() => go({ trash: true })}>
        <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
        {t('drive:trash.title')}
      </button>
      {usage && (
        <span className="drive-usage" title={t('drive:usage.entries', { count: usage.entries, max: integer(usage.max_entries, locale) })}>
          {usage.max_bytes !== null && (
            <span className="drive-usage-bar" aria-hidden="true">
              <span style={{ width: `${Math.min(100, (usage.bytes / Math.max(1, usage.max_bytes)) * 100)}%` }} />
            </span>
          )}
          {usage.max_bytes === null
            ? t('drive:usage.usedNoCap', { used: formatBytes(usage.bytes, locale) })
            : t('drive:usage.used', { used: formatBytes(usage.bytes, locale), max: formatBytes(usage.max_bytes, locale) })}
        </span>
      )}
    </div>
  );
}

/**
 * One folder of the Drive: its crumbs and actions, then its entries, folders
 * first, then the trash link and the usage at its foot.
 */
export function DriveList({ dir, entries, loading, error, openPath, compact, usage, upload, onRetry, tray }: {
  dir: string;
  entries: DriveEntry[] | undefined;
  loading: boolean;
  /** The folder could not be read: a 404 is one that is gone. */
  error: 'gone' | 'failed' | null;
  /** The open file's path, marked in the list. */
  openPath: string | null;
  /** Beside an open file: one column, each row's details on a second line. */
  compact: boolean;
  usage: DriveUsage | null;
  upload: Upload;
  onRetry: () => void;
  /** The uploads tray, pinned above the foot. */
  tray?: React.ReactNode;
}) {
  const { t } = useTranslation();
  const { ask, go, actions } = useDriveContext();
  const [sort, setSort] = useState<SortKey>('name');
  const drop = useDriveDrop(dir, entries, upload, actions.moveInto);
  const pickers = useFilePickers(dir, upload);
  const locale = useLocale();
  const sorted = entries ? sortEntries(entries, sort, locale) : [];
  // The URL may spell the open file in another case than the listing does.
  const openKey = openPath === null ? null : drivePathKey(openPath);

  let body: React.ReactNode;
  if (error) {
    body = (
      <div className="drive-pane-pad">
        {error === 'gone' ? (
          <>
            <ListError>{t('drive:errors.missing', { name: nameOf(dir) })}</ListError>
            <div className="mt-3"><HeaderButton onClick={() => go({ dir: '' })}>{t('drive:backToRoot')}</HeaderButton></div>
          </>
        ) : (
          <ListError onRetry={onRetry}>{t('drive:errors.loadFailed')}</ListError>
        )}
      </div>
    );
  } else if (loading) {
    body = <div className="drive-pane-pad" aria-busy="true"><ListSkeleton rows={6} /></div>;
  } else if (sorted.length === 0) {
    body = (
      <div className="drive-pane-pad">
        <EmptyState
          message={dir ? t('drive:emptyFolder') : t('drive:empty')}
          action={<HeaderButton variant="primary" icon={Upload} onClick={pickers.pickFiles}>{t('drive:actions.uploadFiles')}</HeaderButton>}
        />
      </div>
    );
  } else {
    body = (
      <>
        {!compact && <Columns sort={sort} onSort={setSort} />}
        <div role="list" data-drive-rows aria-label={t('drive:listLabel')}>
          {sorted.map((entry) => (
            <DriveRow key={entry.id} entry={entry} compact={compact} active={openKey !== null && drivePathKey(entry.path) === openKey} drop={drop} />
          ))}
        </div>
      </>
    );
  }

  return (
    <PaneFrame
      title={t('drive:title')}
      className={cn('drive-list', compact && 'is-compact', drop.filesOver && 'is-files-over')}
      {...drop.listProps}
      actions={(
        <>
          <HeaderButton icon={FolderPlus} onClick={() => ask.newFolder(dir)} aria-label={t('drive:actions.newFolder')} title={t('drive:actions.newFolder')}>
            <span className={cn(compact && 'sr-only')}>{t('drive:actions.newFolder')}</span>
          </HeaderButton>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <HeaderButton variant="primary" icon={Upload}>{t('drive:actions.upload')}</HeaderButton>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onSelect={pickers.pickFiles}>
                <Upload className="h-3.5 w-3.5" aria-hidden="true" />
                {t('drive:actions.uploadFiles')}
              </DropdownMenuItem>
              <DropdownMenuItem onSelect={pickers.pickFolder}>
                <FolderUp className="h-3.5 w-3.5" aria-hidden="true" />
                {t('drive:actions.uploadFolder')}
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </>
      )}
      below={(
        <>
          <Crumbs dir={dir} drop={drop} />
          {pickers.inputs}
        </>
      )}
      foot={(
        <>
          {tray}
          <ListFoot usage={usage} />
        </>
      )}
    >
      {body}
      {drop.filesOver && (
        <div className="drive-drop-hint" aria-hidden="true">
          {t('drive:dropHere', { folder: dir ? nameOf(dir) : t('drive:title') })}
        </div>
      )}
    </PaneFrame>
  );
}
