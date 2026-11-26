/**
 * Addresses of the Drive page. The folder or file in view lives in the URL,
 * so a link from a chat, a tool row or another tab opens the same place.
 *
 * `?dir=a/b` is a folder, `?file=a/b/x.md` a file (shown in its folder),
 * `?trash=1` the trash, `?memos=1` every memo (beside a file it opens). On a
 * memo, `?tab=` picks the record, the changes or who wrote what, `?v=` the
 * version shown or compared and `?vs=` what it is compared with. `space`
 * names a space by id and is left out for the personal one, the only one
 * there is for now.
 */
export interface DriveLocation {
  space?: string | null;
  dir?: string | null;
  file?: string | null;
  trash?: boolean;
  /** The list of every memo, in place of a folder. */
  memos?: boolean;
  /** A version of the open file: shown in place of its head, or the one Changes shows. */
  version?: number | null;
  /** A memo's view other than its document. */
  tab?: MemoTab | null;
  /** The version Changes compares with; null is the one before. */
  against?: number | null;
}

export const MEMO_TABS = ['record', 'changes', 'who'] as const;
export type MemoTab = (typeof MEMO_TABS)[number];

export const DRIVE_ROUTE = '/drive';

export function driveHref(loc: DriveLocation = {}): string {
  const params = new URLSearchParams();
  if (loc.space) params.set('space', loc.space);
  if (loc.trash) params.set('trash', '1');
  else {
    if (loc.memos) params.set('memos', '1');
    if (loc.file) {
      params.set('file', loc.file);
      if (loc.tab) params.set('tab', loc.tab);
      if (loc.version) params.set('v', String(loc.version));
      if (loc.against) params.set('vs', String(loc.against));
    } else if (loc.dir && !loc.memos) params.set('dir', loc.dir);
  }
  const query = params.toString();
  return query ? `${DRIVE_ROUTE}?${query}` : DRIVE_ROUTE;
}

/** Folds a path to the Drive's own spelling: no leading, trailing or doubled slash. */
export function cleanDrivePath(path: string | null | undefined): string {
  return (path ?? '').split('/').filter((s) => s && s !== '.').join('/');
}

/** The location a page address names, every part filled in. */
export interface ReadLocation {
  space: string | null;
  /** The folder in view; '' is the space's root. */
  dir: string;
  file: string | null;
  trash: boolean;
  memos: boolean;
  version: number | null;
  tab: MemoTab | null;
  against: number | null;
}

function seqParam(value: string | null): number | null {
  const n = Number(value);
  return value && Number.isInteger(n) && n > 0 ? n : null;
}

export function readDriveLocation(params: URLSearchParams): ReadLocation {
  const tab = params.get('tab');
  // A stage 1 link to a file's history (`?history=1`) is its record.
  const legacy = params.get('history') === '1' ? 'record' : null;
  return {
    space: params.get('space'),
    dir: cleanDrivePath(params.get('dir')),
    file: cleanDrivePath(params.get('file')) || null,
    trash: params.get('trash') === '1',
    memos: params.get('memos') === '1',
    version: seqParam(params.get('v')),
    tab: MEMO_TABS.find((known) => known === tab) ?? legacy,
    against: seqParam(params.get('vs')),
  };
}

/**
 * The open file at `path` (renamed or moved) in the view the address had:
 * what a move carries, from the page's own rename or from the change feed.
 * Whether the memo list stays beside it is the page's to decide.
 */
export function withFile(loc: ReadLocation, path: string): DriveLocation {
  return { file: path, tab: loc.tab, version: loc.version, against: loc.against };
}
