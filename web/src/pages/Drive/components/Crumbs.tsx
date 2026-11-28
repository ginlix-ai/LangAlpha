import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Folder } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { cn } from '@/lib/utils';
import { useDriveContext } from '../driveContext';
import { crumbsOf } from '../drivePaths';
import { carriesRow, type DriveDrop } from '../useDriveDrop';

/**
 * The folders a deep path folds away, behind "…". A row dragged onto it opens
 * the menu, as a folder springs open in Finder, and each folder in it takes
 * the drop the crumb it stands for would; the menu closes when the drag ends.
 */
function FoldedCrumbs({ folded, drop }: { folded: { name: string; path: string }[]; drop?: DriveDrop }) {
  const { t } = useTranslation();
  const { go } = useDriveContext();
  const [open, setOpen] = useState(false);
  // Opened by a drag: not modal, so the rest of the page still takes the drag.
  const [sprung, setSprung] = useState(false);
  useEffect(() => {
    if (!sprung) return;
    const close = () => {
      setOpen(false);
      setSprung(false);
    };
    window.addEventListener('dragend', close);
    window.addEventListener('drop', close);
    return () => {
      window.removeEventListener('dragend', close);
      window.removeEventListener('drop', close);
    };
  }, [sprung]);
  return (
    <DropdownMenu
      open={open}
      onOpenChange={(next) => {
        setOpen(next);
        if (!next) setSprung(false);
      }}
      modal={!sprung}
    >
      <DropdownMenuTrigger asChild>
        <button
          type="button"
          className="drive-crumb is-fold"
          aria-label={t('drive:crumbsFolded', { count: folded.length })}
          onDragEnter={(e) => {
            if (!drop || !carriesRow(e) || open) return;
            setSprung(true);
            setOpen(true);
          }}
        >
          …
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="start">
        {folded.map((f) => {
          const { isDrop, ...handlers } = drop ? drop.targetProps(f.path, `crumb:${f.path}`, 'rows') : { isDrop: false };
          return (
            <DropdownMenuItem
              key={f.path}
              className={cn(isDrop && 'drive-crumb-item is-drop')}
              onSelect={() => go({ dir: f.path })}
              {...handlers}
            >
              <Folder className="h-3.5 w-3.5" aria-hidden="true" />
              {f.name}
            </DropdownMenuItem>
          );
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

/**
 * Where a folder sits, on one line whatever its depth: a line that wrapped
 * pushed the list down by a row as the pane narrowed. Past a few folders the
 * ones between the root and the last few fold into a menu, the last two
 * beside a file and three at full width, so a deep path never runs past the pane.
 * Above a folder's list the last crumb is that folder and takes the reader
 * nowhere; above a file (`here` false) each one is a way back to its folder.
 * With `drop`, a crumb takes a row dragged onto it.
 */
export function Crumbs({ dir, drop, compact, here = true, className }: {
  dir: string;
  drop?: DriveDrop;
  compact: boolean;
  here?: boolean;
  className?: string;
}) {
  const { t } = useTranslation();
  const { go } = useDriveContext();
  const crumbs = [{ name: t('drive:title'), path: '' }, ...crumbsOf(dir)];
  const kept = compact ? 2 : 3;
  const folded = crumbs.length > kept + 1 ? crumbs.slice(1, -kept) : [];
  const shown = folded.length ? [crumbs[0], null, ...crumbs.slice(-kept)] : crumbs;
  return (
    <nav className={cn('drive-crumbs', className)} aria-label={t('drive:crumbs')}>
      {shown.map((c, i) => {
        const sep = i > 0 && <span className="drive-crumb-sep" aria-hidden="true">/</span>;
        if (c === null) {
          return (
            <React.Fragment key="…">
              {sep}
              <FoldedCrumbs folded={folded} drop={drop} />
            </React.Fragment>
          );
        }
        const last = here && i === shown.length - 1;
        const { isDrop, ...handlers } = last || !drop ? { isDrop: false } : drop.targetProps(c.path, `crumb:${c.path}`, 'rows');
        return (
          <React.Fragment key={c.path || '/'}>
            {sep}
            <button
              type="button"
              className={cn('drive-crumb', last && 'is-here', i === 0 && 'is-root', isDrop && 'is-drop')}
              aria-current={last ? 'page' : undefined}
              title={c.name}
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
