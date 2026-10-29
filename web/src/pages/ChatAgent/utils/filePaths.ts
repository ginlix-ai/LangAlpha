import type { LucideIcon } from 'lucide-react';
import { FileText, FileSpreadsheet, Globe, Image, Presentation } from 'lucide-react';
import {
  folderWorkspacePath,
  onOtherComputer,
  parseAgentHref,
  parseAgentPath,
  workspaceScopedPath,
  type ComputerFolders,
  type FolderPath,
} from './agentPaths';
import { hasLineSuffix, splitFileLocation } from './fileLocation';

/**
 * The kinds of file a person opens to read an answer.
 *
 * A turn writes two sorts of file: the report, model or deck it was asked for,
 * and the scripts it wrote to get there. Only these kinds are the answer, so
 * this table is both the deliverable test and the card's own vocabulary.
 */
export type FileKind = 'document' | 'presentation' | 'spreadsheet' | 'page' | 'image';

// Read as a deliverable's kind, which is not the same question as the file
// tree's type filter (`fileMeta.EXT_TO_TYPE`, which groups by what a reader
// browses: Docs / Code / Data / Image). A `.csv` is a spreadsheet here and
// Data there on purpose; keep both in mind when adding an extension.
const KIND_BY_EXT: Record<string, FileKind> = {
  md: 'document', markdown: 'document', pdf: 'document',
  docx: 'document', doc: 'document', rtf: 'document', odt: 'document',
  // A plain-text answer is still the answer: a Chief of Staff relay hands back
  // `notes.txt` as readily as `notes.md`. Executable material stays out
  // (`.py`, `.sh`, `.ipynb`) — that is how the turn got where it went, and a
  // deck of it buries the two files that are the answer.
  txt: 'document',
  pptx: 'presentation', ppt: 'presentation', key: 'presentation',
  xlsx: 'spreadsheet', xlsm: 'spreadsheet', xls: 'spreadsheet', csv: 'spreadsheet',
  tsv: 'spreadsheet', parquet: 'spreadsheet',
  html: 'page', htm: 'page',
  png: 'image', jpg: 'image', jpeg: 'image', gif: 'image', svg: 'image',
  webp: 'image', bmp: 'image',
};

const KIND_ICONS: Record<FileKind, LucideIcon> = {
  document: FileText,
  presentation: Presentation,
  spreadsheet: FileSpreadsheet,
  page: Globe,
  image: Image,
};

/**
 * The file's extension in lower case, or '' when the name carries none.
 *
 * The one extension reader in the app: `fileMeta` and `FileHeaderActions`
 * re-export it, so a dotfile (`.env` → `env`) and an extensionless name
 * (`Makefile` → '') read the same everywhere.
 *
 * Takes a path, not a link destination. A `#` reaches here as part of the name
 * (`issue#1.md`), so cutting at one reported no extension and made the file
 * read as working material with no card and no icon. `isImagePath` splits a
 * destination's location off first, which is where that rule belongs.
 */
export function fileExtension(path: string): string {
  const name = path.split('/').pop() ?? '';
  const dot = name.lastIndexOf('.');
  return dot >= 0 ? name.slice(dot + 1).toLowerCase() : '';
}

/** What kind of deliverable this is, or null when it is working material. */
export function fileKind(path: string): FileKind | null {
  return KIND_BY_EXT[fileExtension(path)] ?? null;
}

/** The glyph standing for a kind of file. */
export function fileKindIcon(kind: FileKind): LucideIcon {
  return KIND_ICONS[kind];
}

/** Prefix used for cross-workspace file references: __wsref__/{workspaceId}/path */
const WSREF_PREFIX = '__wsref__/';

/**
 * The workspace a `__wsref__/{workspaceId}/path` reference names, with the
 * canonical path beside it, or null when the reference names no workspace.
 */
export function parseWsPath(href: string | undefined): { workspaceId: string; path: string } | null {
  if (!href) return null;
  const { workspaceId, path } = parseAgentPath(href);
  return workspaceId ? { workspaceId, path } : null;
}

/**
 * The other workspace a destination reaches through its folder, on this
 * computer or another (`agentPaths.folderWorkspacePath`), with the path inside
 * it read the way `normalizeFilePath` reads a destination. A `__wsref__`
 * destination names its workspace outright and gets null here.
 */
export function parseFolderHref(
  href: string | undefined,
  folders: ComputerFolders | null | undefined,
): FolderPath | null {
  return href ? folderWorkspacePath(parseAgentHref(href, folders?.rootDir), folders) : null;
}

/**
 * Check if an href looks like a sandbox file path (not an external URL).
 *
 * Expects pre-normalized input: `normalizeFileRefs` has unwrapped `file://`,
 * but it keeps a `/home/workspace/` root, which is how a reference says it
 * starts at the workspace rather than beside the file quoting it. Any relative
 * href is a file: the agent has
 * no other use for one, and a relative link left to the browser opens the app
 * itself in a new tab. The file panel owns resolving it, so a name with
 * an unfamiliar extension or none at all still opens. A root-absolute href
 * still needs an extension, since `/settings` style links are app routes, unless
 * it starts at that sandbox root, which already says it names the workspace and
 * so names a file or a folder whatever it ends in.
 * A `name.py:42` line suffix is checked before the scheme test, which would
 * otherwise read `name.py:` as a URL scheme. The computer's own root
 * (`ComputerFolders.rootDir`) says "start at the workspace" the same way, and
 * so does a path through the workspace's folder under a root it sat under
 * before a move (`ComputerFolders.otherRoots`), or through another
 * workspace's folder on another computer (`ComputerFolders.otherComputers`),
 * and so does anything else under that computer's root.
 */
export function isFilePath(href: string | undefined, folders?: ComputerFolders | null): boolean {
  if (!href) return false;
  if (href.startsWith(WSREF_PREFIX)) return !!parseWsPath(href);
  if (hasLineSuffix(href) && !/^[a-z][a-z0-9+.-]*:\/\//i.test(href)) return !/^www\./i.test(href);
  if (/^[a-z][a-z0-9+.-]*:/i.test(href) || href.startsWith('//') || href.startsWith('#') || href.startsWith('?')) return false;
  if (/^www\./i.test(href)) return false;
  if (href.startsWith('/')) {
    // A sandbox root is the reference saying "start at the workspace", so the
    // parser strips it and what comes back is a workspace path with no leading
    // slash. `/settings` keeps its slash through the parser and still has to
    // look like a file. Asking a rooted destination for an extension read
    // `/home/workspace/data/` as an app route, because a folder has none.
    const parts = parseAgentPath(href, folders?.rootDir);
    if (!parts.path.startsWith('/')) return parts.path !== '' || parts.directory;
    // Another root keeps its slash through the parser, so it is asked the way
    // the click reads it: a path the folder folds off is the workspace's,
    // `/old/root/<folder>/data/` as much as `/home/workspace/data/`.
    if (folders?.otherRoots?.length && !normalizeFilePath(href, folders).startsWith('/')) return true;
    // So is a workspace's folder on another of the user's computers, which
    // `/that/root/<folder>/data/` names as surely, and the rest of that
    // machine, which the link then shows as its name rather than as a route
    // of this app (`onOtherComputer`), with an extension or without.
    if (parseFolderHref(href, folders)) return true;
    if (onOtherComputer(parseAgentHref(href, folders?.rootDir), folders)) return true;
    // The extension has to be in the name, not in what follows it. Reading the
    // whole destination let a query supply one, so `/search?q=notes.md` claimed
    // to be a file, the click was swallowed, and the panel was handed
    // `/search`, which is what `normalizeFilePath` leaves once the query is
    // gone. A literal `#` here is a fragment, the same reading that normalizer
    // applies, and a name that really holds one arrives as `%23`.
    return /\.[a-z0-9]{1,8}$/i.test(href.split(/[?#]/, 1)[0]);
  }
  return true;
}

/**
 * A markdown destination as the API wants it: workspace-relative, decoded once.
 * `agentPaths.parseAgentHref` owns the rules and documents why. Given the
 * workspace's folders, a path through its own folder reads from its root, as
 * routing reads it (`agentPaths.workspaceScopedPath`).
 */
export function normalizeFilePath(path: string, folders?: ComputerFolders | null): string {
  return workspaceScopedPath(parseAgentHref(path, folders?.rootDir), folders?.dirName, folders?.previousDirNames, folders?.otherRoots);
}

/** Whether an href points at an image, by the same table the cards read. */
export function isImagePath(href: string | undefined): boolean {
  // A destination, so `#L4` and `:42` come off before the name is read.
  return !!href && fileKind(splitFileLocation(href).path) === 'image';
}
