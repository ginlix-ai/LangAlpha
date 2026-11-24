/**
 * Addresses of the Drive page. The folder or file in view lives in the URL,
 * so a link from a chat, a tool row or another tab opens the same place.
 *
 * `?dir=a/b` is a folder, `?file=a/b/x.md` a file (shown in its folder),
 * `?trash=1` the trash. `space` names a space by id and is left out for the
 * personal one, the only one there is for now.
 */
export interface DriveLocation {
  space?: string | null;
  dir?: string | null;
  file?: string | null;
  trash?: boolean;
  /** A version of the open file shown in place of its head. */
  version?: number | null;
  /** The open file's history beside it. */
  history?: boolean;
}

export const DRIVE_ROUTE = '/drive';

export function driveHref(loc: DriveLocation = {}): string {
  const params = new URLSearchParams();
  if (loc.space) params.set('space', loc.space);
  if (loc.trash) params.set('trash', '1');
  else if (loc.file) {
    params.set('file', loc.file);
    if (loc.history) params.set('history', '1');
    if (loc.version) params.set('v', String(loc.version));
  } else if (loc.dir) params.set('dir', loc.dir);
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
  version: number | null;
  history: boolean;
}

export function readDriveLocation(params: URLSearchParams): ReadLocation {
  const version = Number(params.get('v'));
  return {
    space: params.get('space'),
    dir: cleanDrivePath(params.get('dir')),
    file: cleanDrivePath(params.get('file')) || null,
    trash: params.get('trash') === '1',
    version: Number.isInteger(version) && version > 0 ? version : null,
    history: params.get('history') === '1',
  };
}
