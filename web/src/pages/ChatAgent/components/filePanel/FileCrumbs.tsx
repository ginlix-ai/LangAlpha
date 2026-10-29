import React from 'react';
import { ChevronRight } from 'lucide-react';

interface FileCrumbsProps {
  path: string;
  /** Point the tree at a directory on the way to this file. */
  onOpenDir: (dir: string) => void;
  /** What is known about the file: lines, pages, sheets. Empty where nothing is. */
  meta: string | null;
  /** The focus chip a reference left behind, when there is one. */
  chip?: React.ReactNode;
  /** Download menu, edit toolbar — the actions that belong to the open file. */
  actions?: React.ReactNode;
  unsaved?: boolean;
}

/**
 * The row under the tab strip: where the open file sits, what is known about
 * it, and what can be done with it. Each segment points the tree at that
 * directory, which is how a reader gets from a file to its neighbours.
 */
export function FileCrumbs({ path, onOpenDir, meta, chip, actions, unsaved }: FileCrumbsProps): React.ReactElement {
  const segments = path.split('/').filter(Boolean);
  return (
    <div className="file-panel-crumbs">
      <nav className="file-panel-crumb-trail" aria-label={path}>
        {segments.map((segment, i) => {
          const last = i === segments.length - 1;
          const dir = segments.slice(0, i + 1).join('/');
          return (
            <React.Fragment key={dir}>
              {last ? (
                <span className="file-panel-crumb is-file">
                  {segment}
                  {unsaved && <span className="file-panel-crumb-unsaved"> *</span>}
                </span>
              ) : (
                <button type="button" className="file-panel-crumb" onClick={() => onOpenDir(dir)}>
                  {segment}
                </button>
              )}
              {!last && <ChevronRight className="file-panel-crumb-sep h-3 w-3" aria-hidden="true" />}
            </React.Fragment>
          );
        })}
      </nav>
      {chip}
      <span className="file-panel-crumb-spacer" />
      {meta && <span className="file-panel-crumb-meta">{meta}</span>}
      {actions}
    </div>
  );
}
