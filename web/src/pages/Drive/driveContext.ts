import { createContext, use } from 'react';
import type { DriveLocation } from '@/lib/driveLinks';
import type { DriveEntry } from '@/types/drive';
import type { DriveActions } from './useDriveActions';

export interface DriveContextValue {
  space: string | null;
  actions: DriveActions;
  /** Goes to a place on the page; leaving a file with unsaved edits asks first. */
  go: (next: DriveLocation, opts?: { replace?: boolean }) => void;
  /** Opens an entry: a folder in the list, a file beside it. */
  open: (entry: DriveEntry) => void;
  /** Moves an entry to the trash, leaving the open file when it went with it. */
  trash: (entry: DriveEntry) => void;
  /** Closes the editor, asking first when it holds unsaved text; `then` runs once it is closed. */
  leaveEditor: (then?: () => void) => void;
  ask: {
    rename: (entry: DriveEntry) => void;
    move: (entry: DriveEntry) => void;
    newFolder: (dir: string) => void;
  };
}

export const DriveContext = createContext<DriveContextValue | null>(null);

export function useDriveContext(): DriveContextValue {
  const value = use(DriveContext);
  if (!value) throw new Error('useDriveContext outside DriveProvider');
  return value;
}
