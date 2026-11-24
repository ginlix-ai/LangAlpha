import { useState } from 'react';
import type React from 'react';
import type { DriveEntry } from '@/types/drive';
import { isWithin, parentOf } from './drivePaths';
import { collectDropped, type PickedFile } from './useDriveUpload';

/** The drag payload of a row picked up inside the page: its path. */
const DRAG_TYPE = 'application/x-langalpha-drive-path';

type Upload = (files: PickedFile[], dir: string, folders?: string[]) => void;
type MoveInto = (entry: DriveEntry, folder: string) => Promise<unknown>;

/**
 * Dragging on a folder's list. Files from the machine dropped anywhere on it
 * go up into the folder, or into the folder row they land on; a row dragged
 * onto a folder row or a crumb moves there. `filesOver` counts enters and
 * leaves, since a drag crossing the list's children leaves and enters it
 * again at every edge.
 */
export function useDriveDrop(dir: string, entries: DriveEntry[] | undefined, upload: Upload, moveInto: MoveInto) {
  const [depth, setDepth] = useState(0);
  const [target, setTarget] = useState<string | null>(null);

  const dropOn = async (data: DataTransfer, folder: string) => {
    const dragged = data.getData(DRAG_TYPE);
    if (dragged) {
      if (isWithin(folder, dragged) || parentOf(dragged) === folder) return;
      const entry = entries?.find((e) => e.path === dragged);
      if (entry) await moveInto(entry, folder);
      return;
    }
    if (data.types.includes('Files')) {
      const { files, folders } = await collectDropped(data);
      upload(files, folder, folders);
    }
  };

  const hasFiles = (e: React.DragEvent) => e.dataTransfer.types.includes('Files');

  /** The list as a whole: files dropped on it go into the folder in view. */
  const listProps = {
    onDragEnter: (e: React.DragEvent) => {
      if (hasFiles(e)) setDepth((n) => n + 1);
    },
    onDragLeave: (e: React.DragEvent) => {
      if (hasFiles(e)) setDepth((n) => Math.max(0, n - 1));
    },
    onDragOver: (e: React.DragEvent) => {
      if (!hasFiles(e)) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = 'copy';
    },
    onDrop: (e: React.DragEvent) => {
      if (!hasFiles(e)) return;
      e.preventDefault();
      setDepth(0);
      void dropOn(e.dataTransfer, dir);
    },
  };

  /**
   * A folder a drop can land on: a row takes rows and files, a crumb rows
   * only. `key` tells a crumb from the row of the same folder.
   */
  const targetProps = (folder: string, key: string, accepts: 'rows' | 'rows-and-files') => ({
    isDrop: target === key,
    onDragOver: (e: React.DragEvent) => {
      const types = e.dataTransfer.types;
      const files = accepts === 'rows-and-files' && types.includes('Files');
      if (!types.includes(DRAG_TYPE) && !files) return;
      e.preventDefault();
      e.stopPropagation();
      e.dataTransfer.dropEffect = files ? 'copy' : 'move';
      if (target !== key) setTarget(key);
    },
    onDragLeave: () => {
      if (target === key) setTarget(null);
    },
    onDrop: (e: React.DragEvent) => {
      e.preventDefault();
      e.stopPropagation();
      setTarget(null);
      setDepth(0);
      void dropOn(e.dataTransfer, folder);
    },
  });

  /** A row that can be picked up and dropped on a folder. */
  const sourceProps = (entry: DriveEntry) => ({
    draggable: true,
    onDragStart: (e: React.DragEvent) => {
      e.dataTransfer.setData(DRAG_TYPE, entry.path);
      e.dataTransfer.effectAllowed = 'move';
    },
  });

  return { filesOver: depth > 0, listProps, targetProps, sourceProps };
}

export type DriveDrop = ReturnType<typeof useDriveDrop>;
