import { cleanDrivePath } from '@/lib/driveLinks';
import { nameCollator } from '@/lib/format';
import { classifyAgentPath, parseAgentPath } from '@/pages/ChatAgent/utils/agentPaths';
import { CASEFOLD_EXCEPTIONS } from '@/pages/ChatAgent/utils/agentPaths.generated';
import type { DriveEntry } from '@/types/drive';

/** The folder a path sits in; '' is the space's root. */
export function parentOf(path: string): string {
  const i = path.lastIndexOf('/');
  return i < 0 ? '' : path.slice(0, i);
}

export function nameOf(path: string): string {
  return path.slice(path.lastIndexOf('/') + 1);
}

/** `dir` and a path inside it, `..` and `.` folded, never climbing above the root. */
export function joinDrivePath(dir: string, rel: string): string {
  const out: string[] = [];
  for (const seg of `${dir}/${rel}`.split('/')) {
    if (!seg || seg === '.') continue;
    if (seg === '..') out.pop();
    else out.push(seg);
  }
  return out.join('/');
}

/**
 * Where a reference in a Drive document in `dir` points, in its space. One
 * that names where it starts (`/images/logo.png`) starts at the space's top
 * folder, as a workspace document's starts at the workspace's; any other is
 * read from `dir`.
 */
export function driveLinkPath(dir: string, ref: string, rooted?: boolean): string {
  const parsed = parseAgentPath(ref);
  return joinDrivePath((rooted ?? parsed.absolute) ? '' : dir, parsed.path);
}

/** A file a Drive file in `dir` of `space` links to: a Drive path in the space it names, else as `driveLinkPath` reads it. */
export function linkedFile(space: string, dir: string, ref: string): { space: string; path: string } {
  const info = classifyAgentPath(ref);
  return info.kind === 'drive' ? { space: info.space, path: info.path } : { space, path: driveLinkPath(dir, ref) };
}

/**
 * What the server takes `path` for when it looks a name up (`path_key`): NFC,
 * full case folding, NFC again. The folding is the generated table of where
 * Python's `casefold` differs from lowercasing, so the two agree but for code
 * points one engine's Unicode version has and the other's lacks.
 */
export function drivePathKey(path: string): string {
  const fold = (s: string) => Array.from(s.normalize('NFC'), (c) => CASEFOLD_EXCEPTIONS[c] ?? c.toLowerCase()).join('').normalize('NFC');
  return path.split('/').map(fold).join('/');
}

/**
 * How many leading segments of `path`, spelled any way the server takes it
 * (a URL, a link), name the server's `folder`; null when it is not `folder`
 * or under it. Counted in segments, since folding can change a name's length
 * (`STRASSE` for `straße`).
 */
function keyedDepth(path: string, folder: string): number | null {
  const segments = path.split('/');
  const depth = folder.split('/').length;
  if (segments.length < depth) return null;
  return drivePathKey(segments.slice(0, depth).join('/')) === drivePathKey(folder) ? depth : null;
}

/** Whether `path`, in any spelling the server takes, is the server's `folder` or under it. */
export function isWithinKey(path: string, folder: string): boolean {
  return keyedDepth(path, folder) !== null;
}

/**
 * `path`, in any spelling the server takes, with the server's moved `from`
 * replaced by its `to`, when it is `from` or under it; the rest keeps its own
 * spelling.
 */
export function followMove(path: string, from: string, to: string): string | null {
  const depth = keyedDepth(path, from);
  if (depth === null) return null;
  return [to, ...path.split('/').slice(depth)].join('/');
}

/** Whether `path` is `folder` or anything under it, both as the server spells them. */
export function isWithin(path: string, folder: string): boolean {
  return path === folder || path.startsWith(`${folder}/`);
}

/** Each folder from the root to `dir`, for the crumbs. */
export function crumbsOf(dir: string): { name: string; path: string }[] {
  const parts = cleanDrivePath(dir).split('/').filter(Boolean);
  return parts.map((name, i) => ({ name, path: parts.slice(0, i + 1).join('/') }));
}

export type SortKey = 'name' | 'modified' | 'size';

/** Folders first, then by the chosen column: names A to Z in `locale`, the newest change, the largest file. */
export function sortEntries(entries: DriveEntry[], key: SortKey, locale: string): DriveEntry[] {
  const collator = nameCollator(locale);
  return [...entries].sort((a, b) => {
    if (a.kind !== b.kind) return a.kind === 'folder' ? -1 : 1;
    if (key === 'modified') {
      const d = Date.parse(b.modified_at) - Date.parse(a.modified_at);
      if (d) return d;
    } else if (key === 'size') {
      const d = (b.size ?? 0) - (a.size ?? 0);
      if (d) return d;
    }
    return collator.compare(a.name, b.name);
  });
}
