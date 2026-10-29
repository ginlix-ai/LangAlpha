import { useCallback } from 'react';
import type React from 'react';
import type { RefObject } from 'react';

interface TreeKeyboardArgs {
  listRef: RefObject<HTMLElement | null>;
  expandedDirs: Set<string>;
  toggleDir: (dir: string) => void;
  onOpenFile: (path: string, event: { shiftKey: boolean; metaKey: boolean; ctrlKey: boolean }) => void;
  /** Escape leaves the tree — select mode first, then the panel's own handler. */
  onEscape: () => void;
}

/**
 * Arrow-key navigation over the rendered tree.
 *
 * The rows are the source of truth rather than a second flattening of the
 * tree: they already carry their kind, path and depth, and reading the DOM is
 * what keeps the order the keyboard walks identical to the order the eye sees,
 * including while a filter is collapsing whole branches out of it.
 */
export function useTreeKeyboard({ listRef, expandedDirs, toggleDir, onOpenFile, onEscape }: TreeKeyboardArgs) {
  const rows = useCallback(
    () => Array.from(listRef.current?.querySelectorAll<HTMLElement>('[data-tree-row]') ?? []),
    [listRef],
  );

  const focusPath = useCallback((path: string) => {
    const row = rows().find((el) => el.dataset.rowPath === path);
    row?.focus();
    return !!row;
  }, [rows]);

  const focusFirst = useCallback(() => {
    rows()[0]?.focus();
  }, [rows]);

  const onKeyDown = useCallback((event: React.KeyboardEvent<HTMLElement>) => {
    if (event.key === 'Escape') {
      // The panel's own Escape handler sits above this one and puts the tree
      // back open, so letting the event past here makes the overlay impossible
      // to dismiss from the keyboard.
      event.stopPropagation();
      onEscape();
      return;
    }
    const all = rows();
    if (!all.length) return;
    const current = all.findIndex((el) => el === document.activeElement);
    const move = (to: number) => {
      event.preventDefault();
      all[Math.max(0, Math.min(all.length - 1, to))]?.focus();
    };

    switch (event.key) {
      case 'ArrowDown': return move(current < 0 ? 0 : current + 1);
      case 'ArrowUp': return move(current < 0 ? all.length - 1 : current - 1);
      case 'Home': return move(0);
      case 'End': return move(all.length - 1);
      default: break;
    }

    if (current < 0) return;
    const row = all[current];
    const path = row.dataset.rowPath ?? '';
    const isDir = row.dataset.rowKind === 'dir';
    const depth = Number(row.dataset.rowDepth ?? 0);

    if (event.key === 'ArrowRight') {
      event.preventDefault();
      if (!isDir) return;
      if (!expandedDirs.has(path)) toggleDir(path);
      else all[current + 1]?.focus();
      return;
    }
    if (event.key === 'ArrowLeft') {
      event.preventDefault();
      if (isDir && expandedDirs.has(path)) { toggleDir(path); return; }
      // Otherwise climb: the nearest row above that sits one level shallower
      // is this row's folder, which is where Left means "go up".
      for (let i = current - 1; i >= 0; i--) {
        if (Number(all[i].dataset.rowDepth ?? 0) < depth) return all[i].focus();
      }
      return;
    }
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      if (isDir) toggleDir(path);
      else onOpenFile(path, { shiftKey: event.shiftKey, metaKey: event.metaKey, ctrlKey: event.ctrlKey });
    }
  }, [rows, expandedDirs, toggleDir, onOpenFile, onEscape]);

  return { onKeyDown, focusPath, focusFirst };
}
