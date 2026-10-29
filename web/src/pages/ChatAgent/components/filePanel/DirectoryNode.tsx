import React from 'react';
import { CheckSquare, ChevronDown, ChevronRight, Folder, ScrollText, Square } from 'lucide-react';
import type { MemoEntry } from '../../utils/api';
import type { ContextMenuData, ContextPayload, TreeNode } from './types';
import { fileGlyph } from './fileMeta';
import { collectTreeFiles } from './fileTree';

// --- Row geometry ---

/** What one level of nesting indents a row by. */
const INDENT_STEP = 12;
/** Where the first guide line sits, inside the row's left padding. */
const GUIDE_OFFSET = 16;
/** The left padding a top-level row keeps before its indent. */
const ROW_PAD = 6;

const rowIndent = (depth: number) => depth * INDENT_STEP + ROW_PAD;

// --- IndentGuides ---

interface IndentGuidesProps {
  depth: number;
}

/** Renders vertical indent guide lines for a given depth */
function IndentGuides({ depth }: IndentGuidesProps): React.ReactElement | null {
  if (depth <= 0) return null;
  const guides: React.ReactElement[] = [];
  for (let i = 0; i < depth; i++) {
    guides.push(
      <span
        key={i}
        className="file-tree-indent-guide"
        style={{ left: i * INDENT_STEP + GUIDE_OFFSET }}
      />
    );
  }
  return <>{guides}</>;
}

// --- DirectoryNode ---

interface DirectoryNodeProps {
  node: TreeNode;
  depth: number;
  showHeader: boolean;
  expandedDirs: Set<string>;
  toggleDir: (dir: string) => void;
  selectMode: boolean;
  selectedPaths: Set<string>;
  toggleDirSelect: (dirFiles: string[]) => void;
  /** The click carries its modifiers: shift extends a range, Cmd/Ctrl toggles one row. */
  onFileClick: (filePath: string, event: React.MouseEvent) => void;
  /** Double click keeps the file — it stops being the reused preview tab. */
  onFileDoubleClick: (filePath: string) => void;
  readOnly: boolean;
  backedUpSet: Set<string>;
  modifiedSet: Set<string>;
  memoedMap: Map<string, MemoEntry>;
  memoedTitle: string;
  /** Files with a tab open, tinted so the tree says where the reader is. */
  openPaths: Set<string>;
  activePath: string | null;
  onAddContext: ((ctx: ContextPayload) => void) | null;
  setContextMenu: (menu: ContextMenuData | null) => void;
  activeContextPath: string | null;
}

/** Recursive directory node renderer for the file tree */
export function DirectoryNode({
  node, depth, showHeader,
  expandedDirs, toggleDir,
  selectMode, selectedPaths, toggleDirSelect,
  onFileClick, onFileDoubleClick, readOnly, backedUpSet, modifiedSet, memoedMap, memoedTitle,
  openPaths, activePath,
  onAddContext, setContextMenu, activeContextPath,
}: DirectoryNodeProps): React.ReactElement {
  const isRoot = node.name === '/';
  const isCollapsed = isRoot ? false : !expandedDirs.has(node.fullPath);
  const allFiles = collectTreeFiles(node);
  const totalCount = allFiles.length;
  const allSelected = allFiles.every((f) => selectedPaths.has(f));
  const indent = rowIndent(depth + 1);

  return (
    <div key={node.fullPath}>
      {showHeader && (
        <div
          className="file-panel-dir-header file-tree-row"
          role="treeitem"
          tabIndex={-1}
          aria-expanded={!isCollapsed}
          aria-selected={selectMode ? allSelected : undefined}
          data-tree-row=""
          data-row-kind="dir"
          data-row-path={node.fullPath}
          data-row-depth={depth}
          style={depth > 0 ? { paddingLeft: rowIndent(depth) } : undefined}
          onClick={() => selectMode ? toggleDirSelect(allFiles) : toggleDir(node.fullPath)}
        >
          <IndentGuides depth={depth} />
          {selectMode ? (
            allSelected
              ? <CheckSquare className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              : <Square className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          ) : isCollapsed
            ? <ChevronRight className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            : <ChevronDown className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          }
          <Folder className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          <span className="text-xs font-medium truncate" style={{ color: 'var(--color-text-tertiary)' }}>
            {isRoot ? '/' : `${node.name}/`}
          </span>
          <span className="text-xs" style={{ color: 'var(--color-icon-muted)' }}>
            {totalCount}
          </span>
        </div>
      )}
      {(!isCollapsed || selectMode) && (
        <>
          {/* Subdirectories */}
          {node.children.map((child) => (
            <DirectoryNode
              key={child.fullPath}
              node={child}
              depth={showHeader ? depth + 1 : depth}
              showHeader={true}
              expandedDirs={expandedDirs}
              toggleDir={toggleDir}
              selectMode={selectMode}
              selectedPaths={selectedPaths}
              toggleDirSelect={toggleDirSelect}
              onFileClick={onFileClick}
              onFileDoubleClick={onFileDoubleClick}
              readOnly={readOnly}
              backedUpSet={backedUpSet}
              modifiedSet={modifiedSet}
              memoedMap={memoedMap}
              memoedTitle={memoedTitle}
              openPaths={openPaths}
              activePath={activePath}
              onAddContext={onAddContext}
              setContextMenu={setContextMenu}
              activeContextPath={activeContextPath}
            />
          ))}
          {/* Files in this directory */}
          {node.files.map((filePath) => {
            const name = filePath.split('/').pop()!;
            const Icon = fileGlyph(filePath);
            const isSelected = selectedPaths.has(filePath);
            const fileDepth = showHeader ? depth + 1 : depth;
            return (
              <div
                key={filePath}
                role="treeitem"
                tabIndex={-1}
                aria-selected={selectMode ? isSelected : activePath === filePath}
                data-tree-row=""
                data-row-kind="file"
                data-row-path={filePath}
                data-row-depth={fileDepth}
                className={[
                  'file-panel-item file-tree-row',
                  selectMode && isSelected ? 'file-panel-item-selected' : '',
                  activeContextPath === filePath ? 'file-panel-item-context-active' : '',
                  openPaths.has(filePath) ? 'file-panel-item-open' : '',
                  activePath === filePath ? 'file-panel-item-active' : '',
                ].filter(Boolean).join(' ')}
                style={{ paddingLeft: showHeader ? indent : undefined }}
                // Every click goes one way, modifiers and all: select mode is
                // the panel's state, not the row's, and a row that decided for
                // itself is what made shift-click unreachable while selecting.
                onClick={(e) => onFileClick(filePath, e)}
                onDoubleClick={() => { if (!selectMode) onFileDoubleClick(filePath); }}
                onContextMenu={(e: React.MouseEvent) => {
                  e.preventDefault();
                  setContextMenu({ x: e.clientX, y: e.clientY, filePath });
                }}
              >
                <IndentGuides depth={fileDepth} />
                {selectMode ? (
                  isSelected
                    ? <CheckSquare className="h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                    : <Square className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                ) : (
                  <Icon className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                )}
                <span className="text-xs truncate" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
                {!selectMode && (memoedMap.has(filePath) || (!readOnly && (backedUpSet.has(filePath) || modifiedSet.has(filePath)))) && (
                  <span className="file-panel-row-status">
                    {memoedMap.has(filePath) && (
                      <span className="file-panel-memo-badge" title={memoedTitle}>
                        <ScrollText
                          className="h-3.5 w-3.5"
                          style={{ color: 'var(--color-text-tertiary)', opacity: 0.85 }}
                        />
                      </span>
                    )}
                    {!readOnly && (backedUpSet.has(filePath) || modifiedSet.has(filePath)) && (
                      <span
                        className={`file-panel-backup-dot ${backedUpSet.has(filePath) ? 'backed-up' : 'modified'}`}
                        title={backedUpSet.has(filePath) ? 'Backed up' : 'Modified since last backup'}
                      />
                    )}
                  </span>
                )}
              </div>
            );
          })}
        </>
      )}
    </div>
  );
}
