import React from 'react';
import { CheckSquare, ChevronDown, ChevronRight, Folder, ScrollText, Square } from 'lucide-react';
import type { MemoEntry } from '../../utils/api';
import type { ContextMenuData, ContextPayload, TreeNode } from './types';
import { getFileIcon } from './fileMeta';
import { collectTreeFiles } from './fileTree';

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
        style={{ left: i * 16 + 20 }}
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
  toggleSelect: (path: string) => void;
  toggleDirSelect: (dirFiles: string[]) => void;
  handleFileClick: (filePath: string) => void;
  readOnly: boolean;
  backedUpSet: Set<string>;
  modifiedSet: Set<string>;
  memoedMap: Map<string, MemoEntry>;
  memoedTitle: string;
  onAddContext: ((ctx: ContextPayload) => void) | null;
  setContextMenu: (menu: ContextMenuData | null) => void;
  activeContextPath: string | null;
}

/** Recursive directory node renderer for the file tree */
export function DirectoryNode({
  node, depth, showHeader,
  expandedDirs, toggleDir,
  selectMode, selectedPaths, toggleSelect, toggleDirSelect,
  handleFileClick, readOnly, backedUpSet, modifiedSet, memoedMap, memoedTitle,
  onAddContext, setContextMenu, activeContextPath,
}: DirectoryNodeProps): React.ReactElement {
  const isRoot = node.name === '/';
  const isCollapsed = isRoot ? false : !expandedDirs.has(node.fullPath);
  const allFiles = collectTreeFiles(node);
  const totalCount = allFiles.length;
  const indent = (depth + 1) * 16 + 8; // base 8px + 16px per depth level

  return (
    <div key={node.fullPath}>
      {showHeader && (
        <div
          className="file-panel-dir-header file-tree-row"
          style={depth > 0 ? { paddingLeft: depth * 16 + 8 } : undefined}
          onClick={() => selectMode ? toggleDirSelect(allFiles) : toggleDir(node.fullPath)}
        >
          <IndentGuides depth={depth} />
          {selectMode ? (
            allFiles.every((f) => selectedPaths.has(f))
              ? <CheckSquare className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
              : <Square className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          ) : isCollapsed
            ? <ChevronRight className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
            : <ChevronDown className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
          }
          <Folder className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
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
              toggleSelect={toggleSelect}
              toggleDirSelect={toggleDirSelect}
              handleFileClick={handleFileClick}
              readOnly={readOnly}
              backedUpSet={backedUpSet}
              modifiedSet={modifiedSet}
              memoedMap={memoedMap}
              memoedTitle={memoedTitle}
              onAddContext={onAddContext}
              setContextMenu={setContextMenu}
              activeContextPath={activeContextPath}
            />
          ))}
          {/* Files in this directory */}
          {node.files.map((filePath) => {
            const name = filePath.split('/').pop()!;
            const Icon = getFileIcon(name);
            const isSelected = selectedPaths.has(filePath);
            const fileDepth = showHeader ? depth + 1 : depth;
            return (
              <div
                key={filePath}
                className={`file-panel-item file-tree-row ${selectMode && isSelected ? 'file-panel-item-selected' : ''} ${activeContextPath === filePath ? 'file-panel-item-context-active' : ''}`}
                style={{ paddingLeft: showHeader ? indent : undefined }}
                onClick={() => selectMode ? toggleSelect(filePath) : handleFileClick(filePath)}
                onContextMenu={!selectMode && onAddContext ? (e: React.MouseEvent) => {
                  e.preventDefault();
                  setContextMenu({ x: e.clientX, y: e.clientY, filePath });
                } : undefined}
              >
                <IndentGuides depth={fileDepth} />
                {selectMode ? (
                  isSelected
                    ? <CheckSquare className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                    : <Square className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                ) : (
                  <Icon className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                )}
                <span className="text-sm truncate" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
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
