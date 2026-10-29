import { useCallback, useEffect, useState } from 'react';
import type React from 'react';
import type { RefObject } from 'react';
import { userLocalStorage } from '@/lib/userStorage';
import type { FileSelection } from './useFileSelection';

type SetOpen = (next: boolean | ((prev: boolean) => boolean)) => void;

interface TreeInteractionArgs {
  /** The panel root: the rows and the focused element are both looked up in it. */
  rootRef: RefObject<HTMLElement | null>;
  selection: FileSelection;
  /** True while the column would starve the viewer, so it floats over it. */
  narrow: boolean;
  activePath: string | null;
  openFile: (path: string) => void;
  setOpen: SetOpen;
}

function readDocked(key: string): boolean {
  try { return userLocalStorage.getItem(key) !== 'false'; } catch { return true; }
}

/**
 * Whether the tree column is showing. A hook of its own because the file
 * opener needs it (a missed reference lands on the tree's search) and the
 * tree's clicks need the file opener.
 */
export function useTreeOpen(workspaceId: string, narrow: boolean): { open: boolean; setOpen: SetOpen } {
  // Two states, because the column means two different things. Beside the
  // viewer it is furniture and the reader's choice is worth remembering; over
  // it, it is a sheet, and a remembered sheet would greet every narrow panel
  // with a scrim over the file it was opened to show.
  // Keyed the way useFileTabs keys its strip: a workspace switch must reload
  // the new workspace's choice, and must not write the old one's under its key
  // on the render in between.
  const storageKey = `filePanel.treeOpen.${workspaceId}`;
  const [dock, setDock] = useState(() => ({ key: storageKey, docked: readDocked(storageKey) }));
  useEffect(() => {
    if (dock.key === storageKey) return;
    setDock({ key: storageKey, docked: readDocked(storageKey) });
  }, [storageKey, dock.key]);
  useEffect(() => {
    if (dock.key !== storageKey) return;
    try { userLocalStorage.setItem(storageKey, String(dock.docked)); } catch { /* blocked store */ }
  }, [dock, storageKey]);
  const docked = dock.docked;
  const setDocked = useCallback((next: boolean | ((prev: boolean) => boolean)) => {
    setDock((prev) => ({ ...prev, docked: typeof next === 'function' ? next(prev.docked) : next }));
  }, []);
  const [sheet, setSheet] = useState(false);
  useEffect(() => { if (narrow) setSheet(false); }, [narrow]);

  const open = narrow ? sheet : docked;
  const setOpen = useCallback<SetOpen>((next) => {
    (narrow ? setSheet : setDocked)(next);
  }, [narrow, setDocked]);
  return { open, setOpen };
}

/**
 * What clicking or typing in the tree does.
 *
 * A modified click is a selection gesture rather than an open, and it turns
 * select mode on rather than being swallowed: the reader who shift-clicks two
 * files means to have selected two files, not to have done nothing.
 */
export function useTreeInteraction({
  rootRef, selection, narrow, activePath, openFile, setOpen,
}: TreeInteractionArgs) {
  /** The visible file rows, in the order the reader sees them. */
  const visibleRowPaths = useCallback(
    () => Array.from(rootRef.current?.querySelectorAll<HTMLElement>('[data-row-kind="file"]') ?? [])
      .map((el) => el.dataset.rowPath!)
      .filter(Boolean),
    [rootRef],
  );

  const onOpen = useCallback((path: string, mod: { shiftKey: boolean; metaKey: boolean; ctrlKey: boolean }) => {
    if (mod.shiftKey) {
      if (!selection.selectMode) selection.setSelectMode(true);
      selection.selectRange(path, visibleRowPaths());
      return;
    }
    if (mod.metaKey || mod.ctrlKey || selection.selectMode) {
      if (!selection.selectMode) selection.setSelectMode(true);
      selection.toggleSelect(path);
      return;
    }
    // A sheet covers the file it just opened.
    if (narrow) setOpen(false);
    openFile(path);
  }, [selection, visibleRowPaths, narrow, openFile, setOpen]);

  /**
   * Escape leaves select mode, and otherwise hands the focus back to the open
   * file's row. Only the keyboard path restores anything: a pointer put the
   * focus where it is, and moving it would be the panel arguing with the mouse.
   */
  const onEscape = useCallback((event: React.KeyboardEvent) => {
    if (event.key !== 'Escape') return;
    if (selection.selectMode) {
      event.stopPropagation();
      selection.exitSelectMode();
      return;
    }
    const active = document.activeElement as HTMLElement | null;
    if (!active || !rootRef.current?.contains(active)) return;
    if (active.closest('input, textarea, .monaco-editor')) return;
    const row = (activePath && rootRef.current.querySelector<HTMLElement>(`[data-row-path="${CSS.escape(activePath)}"]`))
      || rootRef.current.querySelector<HTMLElement>('[data-tree-row]');
    if (!row) return;
    event.stopPropagation();
    setOpen(true);
    // A frame late: the column may have only just been asked to show.
    requestAnimationFrame(() => row.focus());
  }, [selection, rootRef, activePath, setOpen]);

  return { onOpen, onEscape };
}
