import { cn } from '@/lib/utils';

/** Past this an ending is part of the name, not a kind of file: it gives way like the rest. */
const EXTENSION_MAX = 8;

/** A name's stem and its extension (with the dot); a dotfile or a folder has no extension. */
function splitName(name: string, folder = false): [string, string] {
  const dot = name.lastIndexOf('.');
  if (folder || dot <= 0 || name.length - dot > EXTENSION_MAX + 1) return [name, ''];
  return [name.slice(0, dot), name.slice(dot)];
}

/**
 * A file's name on one line that gives way from the middle: the stem takes
 * the ellipsis and the extension stays, so two reports that differ only in
 * kind (`.docx`, `.pdf`) still read apart in a narrow column. The whole name
 * is its title unless the caller gives a fuller one (a path).
 */
export function EntryName({ name, folder = false, className, title = name }: {
  name: string;
  folder?: boolean;
  className?: string;
  title?: string;
}) {
  const [stem, ext] = splitName(name, folder);
  return (
    <span className={cn('drive-name', className)} title={title}>
      {/* Read once, whole; the two parts are only how it is drawn. */}
      <span className="sr-only">{name}</span>
      <span className="drive-name-stem" aria-hidden="true">{stem}</span>
      {ext && <span className="drive-name-ext" aria-hidden="true">{ext}</span>}
    </span>
  );
}
