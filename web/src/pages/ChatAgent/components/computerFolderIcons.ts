/**
 * Folders that say which kind of machine a workspace lives on, for the places
 * a workspace is drawn as a folder (the sidebar). Lucide has no such folders,
 * so these are drawn in its terms: its 24-unit grid, its folder outline, and a
 * mark inside it sized like the one in its own `FolderCode`.
 *
 * - `FolderCommand`: ⌘ in the folder, a Mac's. The mark is Lucide's `Command`
 *   scaled into the folder's body; its stroke is set so it draws as heavy as
 *   the outline at that scale.
 * - `FolderTerminal`: >_ in the folder, a machine reached over SSH, the
 *   prompt the computer badge's `Terminal` shows.
 */
import { createLucideIcon } from 'lucide-react';

const FOLDER = 'M20 20a2 2 0 0 0 2-2V8a2 2 0 0 0-2-2h-7.9a2 2 0 0 1-1.69-.9L9.6 3.9A2 2 0 0 0 7.93 3H4a2 2 0 0 0-2 2v13a2 2 0 0 0 2 2Z';

export const FolderCommand = createLucideIcon('folder-command', [
  ['path', { d: FOLDER, key: 'folder' }],
  [
    'path',
    {
      d: 'M13.08 11.04v4.32a1.08 1.08 0 1 0 1.08-1.08H9.84a1.08 1.08 0 1 0 1.08 1.08V11.04a1.08 1.08 0 1 0-1.08 1.08h4.32a1.08 1.08 0 1 0-1.08-1.08',
      strokeWidth: 1.66,
      key: 'command',
    },
  ],
]);

export const FolderTerminal = createLucideIcon('folder-terminal', [
  ['path', { d: FOLDER, key: 'folder' }],
  ['path', { d: 'm8 10.5 2.5 2.5L8 15.5', key: 'prompt' }],
  ['path', { d: 'M13 15.5h3', key: 'cursor' }],
]);
