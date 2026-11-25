import { createElement } from 'react';
import { Folder } from 'lucide-react';
import { fileGlyph } from '@/pages/ChatAgent/components/filePanel/fileMeta';

/** The glyph a file or folder carries everywhere on the page, the file panel's own by name. */
export function EntryGlyph({ name, folder = false, className }: { name: string; folder?: boolean; className?: string }) {
  return createElement(folder ? Folder : fileGlyph(name), { className, 'aria-hidden': true });
}
