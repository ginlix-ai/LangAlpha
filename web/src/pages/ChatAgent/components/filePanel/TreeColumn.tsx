import React, { useRef } from 'react';
import {
  ArrowUpDown, CheckSquare, Globe, HardDrive, RefreshCw, Search, Settings, Trash2, Upload, X,
} from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { motion } from 'framer-motion';
import { DURATION, EASE_OUT } from '@/lib/motion';
import { Loader } from '@/components/ui/loader';
import type { MemoEntry } from '../../utils/api';
import type { ContextMenuData, ContextPayload } from './types';
import { SORT_OPTIONS } from './fileMeta';
import { DirectoryNode } from './DirectoryNode';
import { useTreeKeyboard } from './useTreeKeyboard';
import type { TreeFilter } from './useTreeFilter';
import type { FileSelection } from './useFileSelection';
import type { FileBackup } from './useFileBackup';
import type { PreviewEntry } from './usePreviews';
import './TreeColumn.css';

export interface TreeColumnProps {
  /** The three models the column is a view of. */
  filter: TreeFilter;
  selection: FileSelection;
  backup: FileBackup;

  filesLoading: boolean;
  filesError: string | null;
  onRefreshFiles?: () => void;

  showSystemFiles: boolean;
  onToggleSystemFiles: (() => void) | null;
  /** The folder a chat link pointed the tree at; it filters until cleared. */
  scopeDir: string | null;
  onClearScope: () => void;

  openPaths: Set<string>;
  activePath: string | null;
  onFileClick: (path: string, event: React.MouseEvent) => void;
  onFileDoubleClick: (path: string) => void;
  onOpenFromKeyboard: (path: string, modifiers: { shiftKey: boolean; metaKey: boolean; ctrlKey: boolean }) => void;
  onEscape: () => void;
  memoedMap: Map<string, MemoEntry>;
  memoedTitle: string;
  onAddContext: ((ctx: ContextPayload) => void) | null;
  setContextMenu: (menu: ContextMenuData | null) => void;
  activeContextPath: string | null;

  /** Dev servers the agent started in this workspace, in port order. */
  previews: PreviewEntry[];
  onOpenPreview: (port: number) => void;
  /** The port on screen, tinted the way the open file is. */
  activePreviewPort: number | null;

  readOnly: boolean;
  uploadDisabled: boolean;
  onUpload: () => void;
  onOpenSettings: (() => void) | null;
  workspaceName?: string;
  /** The backup restore did not finish, so the listing may be short. */
  filesRestoreIncomplete: boolean;

  /** Under a narrow panel the column floats over the viewer instead of beside it. */
  overlay: boolean;
  onDismissOverlay: () => void;
}

/** Matches `.file-panel-tree` in TreeColumn.css; the fold animates to this. */
const TREE_WIDTH = 260;

export function TreeColumn(props: TreeColumnProps): React.ReactElement {
  const { filter, selection, backup } = props;
  const { t } = useTranslation();
  const listRef = useRef<HTMLDivElement>(null);
  const { onKeyDown, focusFirst } = useTreeKeyboard({
    listRef,
    expandedDirs: filter.expandedDirs,
    toggleDir: filter.toggleDir,
    onOpenFile: props.onOpenFromKeyboard,
    onEscape: props.onEscape,
  });

  const onSort = (value: string) => { filter.setSortBy(value); filter.setShowSortMenu(false); };
  const toggleSortMenu = () => filter.setShowSortMenu((v) => !v);
  const enterSelectMode = () => selection.setSelectMode(true);
  const trimmed = filter.searchQuery.trim();
  const showList = !props.filesLoading && !props.filesError;

  // Docked, the column folds on its width so the viewer reflows with it;
  // floating, it slides in from the edge it hangs off. Exits are shorter.
  const enter = { duration: DURATION.fold, ease: EASE_OUT };
  const exit = { duration: DURATION.exit, ease: EASE_OUT };
  const columnMotion = props.overlay
    ? { initial: { x: '100%' }, animate: { x: 0, transition: enter }, exit: { x: '100%', transition: exit } }
    : { initial: { width: 0 }, animate: { width: TREE_WIDTH, transition: enter }, exit: { width: 0, transition: exit } };

  return (
    <>
      {props.overlay && (
        <motion.div
          className="file-panel-tree-scrim"
          onClick={props.onDismissOverlay}
          aria-hidden="true"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1, transition: enter }}
          exit={{ opacity: 0, transition: exit }}
        />
      )}
      <motion.aside
        className={`file-panel-tree${props.overlay ? ' is-overlay' : ''}`}
        aria-label={t('chat.workspaceFiles')}
        {...columnMotion}
      >
       <div className="file-panel-tree-inner">
        {/* Icon row — or the selection bar, which takes the same slot. */}
        <div className="file-panel-tree-tools">
          {selection.selectMode ? (
            <>
              <span className="file-panel-tree-count">{selection.selectedPaths.size}</span>
              <button className="file-panel-chip" onClick={selection.toggleSelectAll}>
                {selection.selectedPaths.size === filter.filteredSortedFiles.length
                  ? t('filePanel.deselectAll')
                  : t('filePanel.selectAllVisible')}
              </button>
              <span className="file-panel-crumb-spacer" />
              {selection.deleteConfirm ? (
                <button onClick={selection.handleDelete} className="file-panel-delete-confirm-btn" disabled={selection.deleteLoading}>
                  {t('filePanel.deleteConfirm', { count: selection.selectedPaths.size })}
                </button>
              ) : (
                <button
                  onClick={selection.handleDelete}
                  className="file-panel-icon-btn"
                  title={t('filePanel.deleteSelected')}
                  disabled={selection.selectedPaths.size === 0 || selection.deleteLoading}
                  style={selection.selectedPaths.size > 0 ? { color: 'var(--color-icon-danger)' } : undefined}
                >
                  <Trash2 className="h-4 w-4" />
                </button>
              )}
              <button onClick={selection.exitSelectMode} className="file-panel-icon-btn" title={t('filePanel.cancelSelection')}>
                <X className="h-4 w-4" />
              </button>
            </>
          ) : (
            <>
              {!props.readOnly && (
                <>
                  <button
                    onClick={enterSelectMode}
                    className="file-panel-icon-btn"
                    title={t('filePanel.selectFiles')}
                    disabled={filter.listedFiles.length === 0}
                  >
                    <CheckSquare className="h-4 w-4" />
                  </button>
                  <button
                    onClick={props.onUpload}
                    className="file-panel-icon-btn"
                    title={t('filePanel.uploadFile')}
                    disabled={props.uploadDisabled}
                  >
                    <Upload className="h-4 w-4" />
                  </button>
                  <button onClick={backup.handleBackup} className="file-panel-icon-btn" title={t('filePanel.backupFiles')} disabled={backup.backingUp}>
                    <HardDrive className={`h-4 w-4 ${backup.backingUp ? 'animate-pulse' : ''}`} />
                  </button>
                  <button onClick={props.onRefreshFiles} className="file-panel-icon-btn" title={t('filePanel.refresh')}>
                    {props.filesLoading ? <Loader size={16} className="text-current" /> : <RefreshCw className="h-4 w-4" />}
                  </button>
                </>
              )}
              <div
                className="file-panel-sort-wrapper"
                ref={filter.sortMenuRef}
                // On the wrapper rather than the menu: Escape closes it from the
                // trigger too, and the panel's own Escape (fold the tree) is
                // not what a reader dismissing a menu asked for.
                onKeyDown={(e) => {
                  if (e.key !== 'Escape' || !filter.showSortMenu) return;
                  e.stopPropagation();
                  filter.setShowSortMenu(false);
                }}
              >
                <button
                  className="file-panel-icon-btn"
                  title={t('filePanel.sortFiles')}
                  aria-label={t('filePanel.sortFiles')}
                  aria-haspopup="menu"
                  aria-expanded={filter.showSortMenu}
                  onClick={toggleSortMenu}
                >
                  <ArrowUpDown className="h-4 w-4" />
                </button>
                {filter.showSortMenu && (
                  <div className="file-panel-sort-menu" role="menu" aria-label={t('filePanel.sortFiles')}>
                    {SORT_OPTIONS.map((opt) => (
                      <button
                        key={opt.value}
                        type="button"
                        role="menuitemradio"
                        aria-checked={filter.sortBy === opt.value}
                        className={`file-panel-sort-item ${filter.sortBy === opt.value ? 'active' : ''}`}
                        onClick={() => onSort(opt.value)}
                      >
                        {opt.label}
                      </button>
                    ))}
                  </div>
                )}
              </div>
              <span className="file-panel-crumb-spacer" />
              {props.onToggleSystemFiles && (
                <button
                  className={`file-panel-chip ${props.showSystemFiles ? 'active' : ''}`}
                  onClick={props.onToggleSystemFiles}
                  title={t('filePanel.systemDirsHint')}
                >
                  {t('filePanel.systemDirs')}
                </button>
              )}
              {props.onOpenSettings && (
                <button
                  className="file-panel-icon-btn"
                  onClick={props.onOpenSettings}
                  title={props.workspaceName || t('chat.workspaceSettings')}
                >
                  <Settings className="h-4 w-4" />
                </button>
              )}
            </>
          )}
        </div>

        {/* The pill answers for the field inside it, twice over: `rings-within`
            draws the keyboard ring on its behalf, and `owns-its-edge` moves the
            focused-field accent edge onto the pill's own border. Both rules
            live in tokens.css. */}
        <div className="file-panel-tree-filter">
          <div
            className="rings-within owns-its-edge flex items-center gap-1.5 h-8 px-2 rounded-md border"
            style={{ backgroundColor: 'var(--color-bg-input)', borderColor: 'var(--color-border-muted)' }}
          >
            <Search className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            <input
              type="text"
              value={filter.searchQuery}
              onChange={(e) => filter.search(e.target.value)}
              onKeyDown={(e) => { if (e.key === 'Escape' && filter.searchQuery) { e.stopPropagation(); filter.clearSearch(); } }}
              placeholder={t('filePanel.searchFiles')}
              aria-label={t('filePanel.searchFiles')}
              className="flex-1 min-w-0 text-base sm:text-xs bg-transparent border-none"
              style={{ color: 'var(--color-text-primary)' }}
            />
            {filter.searchQuery && (
              <button
                type="button"
                onClick={filter.clearSearch}
                className="file-panel-icon-btn"
                title={t('filePanel.clearSearch')}
                aria-label={t('filePanel.clearSearch')}
                style={{ margin: '-0.25rem' }}
              >
                <X className="h-3 w-3" />
              </button>
            )}
          </div>
          {filter.missedRef && (
            <p className="mt-1.5 text-xs break-all" style={{ color: 'var(--color-text-secondary)' }}>
              {filter.extraMatches.length > 1
                ? t('filePanel.refAmbiguous', { path: filter.missedRef })
                : props.filesRestoreIncomplete
                  ? t('filePanel.restoreIncomplete')
                  : t('filePanel.refNotFound', { path: filter.missedRef })}
            </p>
          )}
        </div>

        {/* An unfinished restore, said once. The missed-reference line above
            carries the same sentence when that is what landed here. */}
        {!selection.selectMode && !filter.missedRef && props.filesRestoreIncomplete && (
          <p className="px-2 pb-1.5 text-xs" style={{ color: 'var(--color-text-secondary)' }}>
            {t('filePanel.restoreIncomplete')}
          </p>
        )}

        {props.scopeDir != null && (
          <div className="file-panel-tree-scope">
            <span className="truncate">{props.scopeDir ? `${props.scopeDir}/` : '/'}</span>
            <button type="button" onClick={props.onClearScope} className="file-panel-icon-btn" title={t('filePanel.backToAllFiles')}>
              <X className="h-3 w-3" />
            </button>
          </div>
        )}

        {filter.availableTypes.length > 1 && !selection.selectMode && (
          <div className="file-panel-tree-chips">
            <button className={`file-panel-chip ${filter.filterType === 'All' ? 'active' : ''}`} onClick={() => filter.setFilterType('All')}>
              {t('filePanel.filterAll')}
            </button>
            {filter.availableTypes.map((type) => (
              <button
                key={type}
                className={`file-panel-chip ${filter.filterType === type ? 'active' : ''}`}
                onClick={() => filter.setFilterType(filter.filterType === type ? 'All' : type)}
              >
                {type}
              </button>
            ))}
          </div>
        )}

        {/* Above the tree, and outside it: the apps the agent started are not
            files, and a `role="tree"` cannot hold rows that are not treeitems.
            Staying out of the scroller also keeps them reachable from a long list. */}
        {props.previews.length > 0 && (
          <div className="file-panel-preview-group">
            <div className="file-panel-preview-group-header">
              <Globe className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
              <span className="text-xs font-medium truncate" style={{ color: 'var(--color-text-tertiary)' }}>
                {t('filePanel.runningApps')}
              </span>
              <span className="text-xs" style={{ color: 'var(--color-icon-muted)' }}>{props.previews.length}</span>
            </div>
            {props.previews.map((app) => (
              <button
                key={app.port}
                type="button"
                className={`file-panel-item file-panel-preview-row${app.port === props.activePreviewPort ? ' file-panel-item-active' : ''}`}
                onClick={() => props.onOpenPreview(app.port)}
                title={app.title ? `${app.title} :${app.port}` : `:${app.port}`}
              >
                <Globe className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                <span className="flex-1 min-w-0 truncate text-xs" style={{ color: 'var(--color-text-primary)' }}>
                  {app.title || `:${app.port}`}
                </span>
                {/* Amber only while the URL is being minted — the one moment
                    this row is saying something the reader is waiting on. */}
                {app.loading && (
                  <Loader size={12} label={t('filePanel.startingServer')} style={{ color: 'var(--color-accent-primary)' }} />
                )}
                <span className="file-panel-port-chip">:{app.port}</span>
              </button>
            ))}
          </div>
        )}

        <div
          className="file-panel-tree-list file-tree-root"
          role="tree"
          aria-label={t('chat.workspaceFiles')}
          aria-multiselectable={selection.selectMode}
          ref={listRef}
          // The rows rove with tabIndex -1, so the tree itself is the tab stop;
          // landing on it hands focus to the first row.
          tabIndex={0}
          onFocus={(e) => { if (e.target === e.currentTarget) focusFirst(); }}
          onKeyDown={onKeyDown}
        >
          {props.filesLoading ? (
            Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="file-panel-item animate-pulse">
                <div className="h-4 w-4 rounded" style={{ backgroundColor: 'var(--color-border-muted)' }} />
                <div className="h-4 flex-1 rounded" style={{ backgroundColor: 'var(--color-border-muted)', width: `${50 + i * 10}%` }} />
              </div>
            ))
          ) : props.filesError ? (
            <p className="px-3 py-6 text-center text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{props.filesError}</p>
          ) : filter.listedFiles.length === 0 && !trimmed ? (
            <p className="px-3 py-6 text-center text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{t('filePanel.noFilesYet')}</p>
          ) : showList && filter.filteredSortedFiles.length === 0 ? (
            <p className="px-3 py-6 text-center text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {trimmed ? t('filePanel.noSearchMatches') : t('filePanel.noTypeMatches', { type: filter.filterType.toLowerCase() })}
            </p>
          ) : (
            filter.fileTree.map((node) => (
              <DirectoryNode
                key={node.fullPath}
                node={node}
                depth={0}
                showHeader={node.name !== '/'}
                expandedDirs={filter.expandedDirs}
                toggleDir={filter.toggleDir}
                selectMode={selection.selectMode}
                selectedPaths={selection.selectedPaths}
                toggleDirSelect={selection.toggleDirSelect}
                onFileClick={props.onFileClick}
                onFileDoubleClick={props.onFileDoubleClick}
                readOnly={props.readOnly}
                backedUpSet={backup.backedUpSet}
                modifiedSet={backup.modifiedSet}
                memoedMap={props.memoedMap}
                memoedTitle={props.memoedTitle}
                openPaths={props.openPaths}
                activePath={props.activePath}
                onAddContext={props.onAddContext}
                setContextMenu={props.setContextMenu}
                activeContextPath={props.activeContextPath}
              />
            ))
          )}
        </div>
       </div>
      </motion.aside>
    </>
  );
}
