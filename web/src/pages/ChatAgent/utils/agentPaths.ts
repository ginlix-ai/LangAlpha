// The sandbox layout is emitted from src/ptc_agent/core/paths.py: see
// agentPaths.generated.ts. Re-exported here so callers keep one import.
import {
  AGENT_MD_FILE,
  AUTOMATION_FILE_NAME,
  AUTOMATIONS_DIR,
  CASEFOLD_EXCEPTIONS,
  MEMO_INDEX_FILENAME,
  MEMO_USER_DIR,
  MEMORY_INDEX_FILENAME,
  MEMORY_USER_DIR,
  MEMORY_WORKSPACE_DIR,
  SANDBOX_ROOT_PREFIXES,
  SKILLS_DIR,
  SUBAGENT_FILE_NAME,
  USER_AGENTS_DIR,
  USER_DATA_DIRS,
  USER_DATA_FILES,
} from './agentPaths.generated';

export {
  AGENT_MD_FILE,
  MEMO_INDEX_FILENAME,
  MEMO_USER_DIR,
  MEMORY_INDEX_FILENAME,
  MEMORY_USER_DIR,
  MEMORY_WORKSPACE_DIR,
  SKILLS_DIR,
  USER_DATA_FILES,
};

type UserDataFileName = (typeof USER_DATA_FILES)[keyof typeof USER_DATA_FILES][number];
type WithoutJson<F> = F extends `${infer Name}.json` ? Name : never;

/** Which DB-backed user file a path names: a profile file's name without
 *  `.json`, or `automations` for any automation's file. */
export type UserDataEntity = WithoutJson<UserDataFileName> | 'automations';

const USER_DATA_ENTITY_BY_PATH: ReadonlyMap<string, UserDataEntity> = new Map(
  Object.entries(USER_DATA_FILES as Record<string, readonly UserDataFileName[]>).flatMap(
    ([dir, files]) => files.map(
      (file): [string, UserDataEntity] => [`${dir}/${file}`, file.replace(/\.json$/, '') as UserDataEntity],
    ),
  ),
);

/** The schema documentation file the agent reads to learn the JSON shapes of
 *  the DB-backed user files. Treated as chatter and hidden from the chat
 *  timeline. */
export const USER_DATA_README_FILENAME = 'README.md';
const USER_DATA_README_PATHS = new Set(
  USER_DATA_DIRS.map((dir) => `${dir}/${USER_DATA_README_FILENAME}`),
);

function userDataEntity(norm: string): UserDataEntity | undefined {
  const fixed = USER_DATA_ENTITY_BY_PATH.get(norm);
  if (fixed) return fixed;
  const name = norm.startsWith(`${AUTOMATIONS_DIR}/`) ? norm.slice(AUTOMATIONS_DIR.length + 1) : '';
  return AUTOMATION_FILE_NAME.test(name) ? 'automations' : undefined;
}

/**
 * True when `rawPath` resolves to the README beside a DB-backed user file
 * (`.agents/user/profile/README.md`, `.agents/user/automations/README.md`)
 * under any of the prefixes the agent emits (relative, absolute sandbox root,
 * `file://`, `__wsref__/<wsid>/...`).
 * Used by the chat timeline to suppress the agent's "Read schema doc" calls:
 * they're not user-actionable.
 */
export function isUserDataReadmePath(rawPath: string, root?: string | null): boolean {
  if (!rawPath) return false;
  return USER_DATA_README_PATHS.has(workspaceRelativePath(rawPath, root));
}

/**
 * True for a subagent's file (`.agents/user/agents/<name>.md`), under any
 * prefix the agent emits. The README beside them and anything nested are not
 * one.
 */
export function isSubagentFilePath(rawPath: string, root?: string | null): boolean {
  if (!rawPath) return false;
  const norm = workspaceRelativePath(rawPath, root);
  const name = norm.startsWith(`${USER_AGENTS_DIR}/`) ? norm.slice(USER_AGENTS_DIR.length + 1) : '';
  return SUBAGENT_FILE_NAME.test(name);
}

/**
 * True for the workspace's own notes file, which is runtime context the agent
 * keeps for itself rather than a deliverable. That file lives at the workspace
 * root, however the path spells it (see `workspaceScopedPath`), so
 * `alpha/agent.md` is a nested file and `/tmp/agent.md` keeps its own root.
 */
export function isAgentNotesPath(
  parts: AgentPathParts,
  workspaceDirName?: string | null,
  previousDirNames?: readonly string[] | null,
  otherRoots?: readonly string[] | null,
): boolean {
  return !parts.directory && workspaceScopedPath(parts, workspaceDirName, previousDirNames, otherRoots) === AGENT_MD_FILE;
}

/**
 * A reference read from the workspace's own folder. A path that names that
 * folder, or one it was renamed out of, has the folder folded off, from a
 * machine root or from one climb out of the working directory: to Bash,
 * `../<folder>/x` is `x`. Any other relative path already reads from the
 * folder, so `alpha/x.md` stays a nested file. Without the folder names this
 * is the canonical path, which reads a bare sandbox-root path the way the
 * layout before workspace folders did. Under another computer's root
 * (`ComputerFolders.otherRoots`) only a path through the folder is this
 * workspace's: the rest of that machine is not on this one.
 *
 * The folder matches as the server's file routes match it: the current one
 * exactly, since another spelling is another directory on a case-sensitive
 * disk, and a former one by its name key, since the rename moved it away and
 * a path an older turn wrote can only mean this workspace, unless a sibling
 * has taken it since; a caller that knows the siblings asks
 * `folderWorkspacePath` first.
 */
export function workspaceScopedPath(
  parts: AgentPathParts,
  dirName?: string | null,
  previousDirNames?: readonly string[] | null,
  otherRoots?: readonly string[] | null,
): string {
  const { path } = parts;
  const inside = (rest: string): string | null => {
    const first = rest.split('/', 1)[0];
    const inFolder = !!first && (first === dirName || heldBefore(previousDirNames, foldFolderName(first)));
    return inFolder ? rest.slice(first.length + 1) : null;
  };
  if (parts.absolute) {
    // A root the sandbox did not claim survives parsing (`/tmp/x`): no folder
    // there, unless it is a root the folder sat under before a move. The root
    // it was read under can hold one of those too: moved from `~/langalpha`
    // to `~`, the old path reads `langalpha/<folder>/x`.
    const here = path.startsWith('/') ? null : inside(path);
    return here ?? underRoot(wholePath(parts), otherRoots ?? [], inside) ?? path;
  }
  // A `__wsref__` path climbs out of that workspace's folder, not this one's.
  if (parts.workspaceId || !path.startsWith('../')) return path;
  return inside(path.slice(3)) ?? path;
}

/**
 * The server's `casefold`, code point by code point. Anything looser lets a
 * sibling's folder match a former one: `ı` and `i` are two keys there.
 */
function foldFolderName(name: string): string {
  return Array.from(name, (c) => CASEFOLD_EXCEPTIONS[c] ?? c.toLowerCase()).join('');
}

/** Another workspace, by the folders that address it on its computer. */
export interface FolderWorkspace {
  workspaceId: string;
  dirName: string;
  previousDirNames?: readonly string[] | null;
}

/**
 * The live workspaces on the user's other computers whose folders sit under
 * one root. Two computers with the same root share an entry, since a path
 * cannot say which of them it is on.
 */
export interface RootFolders {
  /** Slash-led with no trailing slash, or '' for the stock roots, which read as one. */
  root: string;
  workspaces: readonly FolderWorkspace[];
}

/**
 * The viewed workspace's own folders, and the folders of every other live
 * workspace a path can name: its siblings on its computer, and the ones on
 * the user's other computers.
 */
export interface ComputerFolders {
  /**
   * The computer's root, where its workspace folders sit. A computer whose
   * user cannot create the stock root keeps them under one of its own, and
   * the agent spells that one out in an absolute path like any other.
   */
  rootDir?: string | null;
  /**
   * The roots the workspace's folder may have sat under before a move, slash-led
   * and sorted, stock ones and its own left out: the user's other computers'
   * and the ones the workspace recorded leaving. A workspace moves between
   * computers with its folder, so an absolute path an older turn wrote under a
   * root it has since left still names it, and the computer that root belonged
   * to may be gone from the list.
   */
  otherRoots?: readonly string[];
  /**
   * The roots among `otherRoots` the workspace recorded leaving. Under one of
   * them a path through its folder is its own before a namesake's there; under
   * the rest, where it may never have been, a live namesake's goes first.
   */
  leftRoots?: readonly string[];
  dirName?: string | null;
  previousDirNames?: readonly string[] | null;
  siblings: readonly FolderWorkspace[];
  /**
   * The workspaces on the user's other computers, by root, sorted. The Chief
   * of Staff reads them at their whole path on that machine and links them
   * so, and the path names the workspace the same way a sibling's does.
   */
  otherComputers?: readonly RootFolders[];
}

/** The fields of a computer that place its workspaces' folders. */
export interface ComputerRootRow {
  computer_id: string;
  root_dir?: string | null;
}

/** The fields of a workspace row that place it on a computer. */
export interface WorkspaceFolderRow {
  workspace_id: string;
  computer_id?: string | null;
  dir_name?: string | null;
  previous_dir_names?: readonly string[] | null;
  /** The root of its computer, when the row was read with it. */
  root_dir?: string | null;
  previous_root_dirs?: readonly string[] | null;
  status?: string | null;
}

/**
 * The viewed workspace's folders and the other live workspaces on its
 * computer and on the user's others, or null until its row is known. A
 * workspace on no computer has no siblings, a deleted workspace's folder went
 * with it, and a row with no folder yet is not one a path can name.
 * `computers` are every computer the user has, its own included.
 *
 * Every row is placed by the root it was read with, then by its computer's on
 * the list: the row gates the folders, so its root is there the moment they
 * are, while the list loads later and a tool title read against no root
 * flashes from one wording to another. A workspace elsewhere is left out
 * while neither is known. Only the viewed row's former roots are read: a root
 * another workspace left names that workspace's folder, not this one's.
 */
export function computerFolders(
  viewed: WorkspaceFolderRow | null | undefined,
  rows: readonly WorkspaceFolderRow[] | null | undefined,
  computers?: readonly ComputerRootRow[] | null,
): ComputerFolders | null {
  if (!viewed) return null;
  const computerId = viewed.computer_id;
  const rootOf = new Map((computers ?? []).map((c) => [c.computer_id, c.root_dir]));
  const siblings: FolderWorkspace[] = [];
  const elsewhere = new Map<string, FolderWorkspace[]>();
  const rowRoots: (string | null | undefined)[] = [];
  // A computer with no workspace on it still holds its root.
  for (const computer of computerId ? computers ?? [] : []) {
    const key = rootKey(computer.root_dir);
    if (computer.computer_id !== computerId && key !== null && !elsewhere.has(key)) elsewhere.set(key, []);
  }
  for (const row of computerId ? rows ?? [] : []) {
    if (row.workspace_id === viewed.workspace_id || !row.computer_id) continue;
    if (row.status === 'deleted' || !row.dir_name) continue;
    const folders = { workspaceId: row.workspace_id, dirName: row.dir_name, previousDirNames: row.previous_dir_names };
    if (row.computer_id === computerId) {
      siblings.push(folders);
      continue;
    }
    const key = rootKey(row.root_dir || rootOf.get(row.computer_id));
    if (key === null) continue;
    rowRoots.push(row.root_dir);
    const group = elsewhere.get(key);
    if (group) group.push(folders);
    else elsewhere.set(key, [folders]);
  }
  // A stock root reads the same as none, so it is left out rather than giving
  // every cache keyed on the root a second entry for the same paths. The
  // others are sorted so the list reordering by activity is no change. The
  // rows' roots join the list's, which can land after them.
  const root = viewed.root_dir || (computerId ? rootOf.get(computerId) : undefined);
  const own = ownRoot(root);
  const others = (roots: readonly (string | null | undefined)[]) => [...new Set(roots.map(ownRoot))]
    .filter((other) => other && other !== own)
    .sort()
    .map((other) => `/${other}`);
  const otherRoots = others([...(computers ?? []).map((c) => c.root_dir), ...rowRoots, ...(viewed.previous_root_dirs ?? [])]);
  const leftRoots = others(viewed.previous_root_dirs ?? []);
  const otherComputers = [...elsewhere]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([key, workspaces]) => ({ root: key, workspaces }));
  return {
    ...(own ? { rootDir: root } : {}),
    ...(otherRoots.length ? { otherRoots } : {}),
    ...(leftRoots.length ? { leftRoots } : {}),
    dirName: viewed.dir_name,
    previousDirNames: viewed.previous_dir_names,
    siblings,
    ...(otherComputers.length ? { otherComputers } : {}),
  };
}

/**
 * A root as `RootFolders.root` spells it: '' for a stock one, since the
 * sandbox reads every stock spelling as the same root, and null for none.
 */
function rootKey(root: string | null | undefined): string | null {
  if (!root?.replace(/^\/+|\/+$/g, '')) return null;
  const own = ownRoot(root);
  return own ? `/${own}` : '';
}

/** A reference that names another workspace's folder: that workspace, and the path inside it. */
export interface FolderPath {
  workspaceId: string;
  path: string;
}

/**
 * The other workspace a reference reaches through its folder. A sibling's is
 * reached the way the agent reaches it from Bash: `../<folder>/x` from its
 * working directory, or `<folder>/x` under a sandbox root. A bare `<folder>/x`
 * is a path inside this workspace, and a folder no other workspace holds is
 * left to the caller's usual reading.
 *
 * Folders match as `workspaceScopedPath` matches them, the current name exactly
 * and a former one by its name key, in this order: this workspace's current
 * folder, a sibling's current one, this workspace's former ones, then a former
 * one only a single sibling left. A current name goes first because a folder a
 * sibling took over after a rename is the sibling's on disk. This workspace's
 * own folders get null here and fold in `workspaceScopedPath`, so asking here
 * first and reading the rest through it keeps that order. The folder itself is
 * the other workspace's root, which reads as `./`.
 *
 * An absolute path that names no folder on this computer may name one on
 * another of the user's (`ComputerFolders.otherComputers`), at its whole path
 * there: `<that root>/<folder>/x`. A folder this workspace recorded sitting
 * under there before a move is still its own (`ComputerFolders.leftRoots`),
 * so that reading comes after this computer's and after
 * `workspaceScopedPath`'s, and a path neither places stays as it was. A stock
 * root on a computer with a root of its own is the exception: it is only an
 * old spelling here and a real one on a stock computer, so a folder there
 * goes before a sibling's here.
 */
export function folderWorkspacePath(
  parts: AgentPathParts,
  folders: ComputerFolders | null | undefined,
): FolderPath | null {
  if (!folders || parts.workspaceId) return null;
  // One level up is the computer root; a second climb leaves it.
  if (!parts.absolute && !parts.path.startsWith('../')) return null;
  // A root the sandbox did not claim (`/tmp/x`, another computer's) holds no
  // folder on this computer, so only the others are asked.
  if (!parts.path.startsWith('/')) {
    // The view's own folder is one more candidate, and a path through it reads
    // from this workspace, so a hit on it gets null. So does a former name it
    // shares with a sibling, which `workspaceScopedPath` then folds as its
    // own. The folders carry no id for it, and no workspace's id is empty.
    const own = { workspaceId: '', dirName: folders.dirName ?? '', previousDirNames: folders.previousDirNames };
    const match = folderPath(parts.absolute ? parts.path : parts.path.slice(3), [own, ...folders.siblings]);
    if (match?.workspaceId === own.workspaceId) return null;
    // On a computer with a root of its own, a stock root is a stock computer's,
    // so a folder there names its workspace before a namesake here does.
    if (parts.absolute && folders.rootDir && !ownRoot(parts.root)) {
      const there = otherComputerPath(parts, folders.otherComputers ?? []);
      if (there) return there;
    }
    // A climb out of the working directory stays on this computer.
    if (match || !parts.absolute) return match;
  }
  if (!folders.otherComputers?.length) return null;
  // This workspace's folder under a root it moved off is still its own.
  if (workspaceScopedPath(parts, folders.dirName, folders.previousDirNames, folders.leftRoots) !== parts.path) return null;
  return otherComputerPath(parts, folders.otherComputers);
}

function heldBefore(names: readonly string[] | null | undefined, key: string): boolean {
  return (names ?? []).some((name) => !!name && foldFolderName(name) === key);
}

/**
 * The workspace among `workspaces` whose folder `rest` starts with, and the
 * path inside it: a current folder exactly, else a former one by its name key
 * that a single one of them left. Two that both left one folder, or two
 * computers on one root that each hold it, give no way to tell whose it is.
 */
function folderPath(rest: string, workspaces: readonly FolderWorkspace[]): FolderPath | null {
  const slash = rest.indexOf('/');
  const folder = slash < 0 ? rest : rest.slice(0, slash);
  if (!folder || folder === '..') return null;
  const key = foldFolderName(folder);
  const current = workspaces.filter((w) => w.dirName === folder);
  const held = current.length ? current : workspaces.filter((w) => heldBefore(w.previousDirNames, key));
  if (held.length !== 1) return null;
  const inside = slash < 0 ? '' : rest.slice(slash + 1);
  return { workspaceId: held[0].workspaceId, path: inside || './' };
}

/**
 * The first reading `read` gives of `whole` past a root it starts under.
 * Roots compare whole segments, so `/srv/a-2` is not under `/srv/a`, and one
 * can sit inside another, so each is tried, the longest first, as the relay
 * reads them: under `/srv` and `/srv/old`, `/srv/old/<folder>/x` is read as
 * `<folder>/x` under `/srv/old` before it is read as `old/<folder>/x` under `/srv`.
 */
function underRoot<T>(
  whole: string,
  roots: readonly string[],
  read: (rest: string, root: string) => T | null,
): T | null {
  const under = roots.filter((root) => whole.startsWith(`${root}/`));
  for (const root of under.sort((a, b) => b.length - a.length)) {
    const hit = read(whole.slice(root.length + 1), root);
    if (hit !== null) return hit;
  }
  return null;
}

/**
 * An absolute path as it reads on its machine, with any sandbox root the
 * parser took off put back: another computer's root can sit inside this
 * one's (`~` here, `~/langalpha` there).
 */
function wholePath(parts: AgentPathParts): string {
  return parts.path.startsWith('/') ? parts.path : `${parts.root ?? ''}/${parts.path}`;
}

/**
 * The workspace on another computer an absolute path names through its
 * folder, under each root it starts under (`underRoot`). The stock roots read
 * as one, whatever the spelling, so their group takes a path read under one,
 * after every root of its own.
 */
function otherComputerPath(parts: AgentPathParts, groups: readonly RootFolders[]): FolderPath | null {
  const at = new Map(groups.map(({ root, workspaces }) => [root, workspaces]));
  const read = (rest: string, root: string) => folderPath(rest, at.get(root) ?? []);
  const stock = !parts.path.startsWith('/') && !ownRoot(parts.root);
  return underRoot(wholePath(parts), [...at.keys()].filter(Boolean), read) ?? (stock ? read(parts.path, '') : null);
}

/**
 * Whether a path the folders left as written is on another of the user's
 * computers: its root, or rooted under it through no workspace's folder there
 * (`folderWorkspacePath` names one through a folder). Nothing this view reads
 * holds it, so it opens nothing. The roots are read as `underRoot` reads them,
 * the longer first, so a computer whose root sits inside this one's (`~` here,
 * `~/langalpha` there) holds the paths under it, and one sharing this root
 * holds none. From a computer with a root of its own, a stock computer holds
 * the stock root.
 */
export function onOtherComputer(parts: AgentPathParts, folders: ComputerFolders | null | undefined): boolean {
  if (!folders?.otherComputers?.length || !parts.absolute || parts.workspaceId) return false;
  // This workspace's folder under a root it moved off is still its own.
  if (workspaceScopedPath(parts, folders.dirName, folders.previousDirNames, folders.otherRoots) !== parts.path) return false;
  const whole = wholePath(parts);
  const here = parts.path.startsWith('/') ? 0 : (parts.root ?? '').length;
  // On a computer with a root of its own, a stock root is a stock computer's
  // (`folderWorkspacePath`), whose group has no root of its own to compare.
  const stock = !!folders.rootDir && !parts.path.startsWith('/') && !!parts.root && !ownRoot(parts.root);
  return folders.otherComputers.some(({ root }) => (root ? root.length > here && (whole === root || whole.startsWith(`${root}/`)) : stock));
}

/**
 * Whether a tool call's arguments name a computer: the Chief of Staff's file
 * tools take one to run on another of the user's computers. The path such a
 * call read or wrote is on that computer, whatever its spelling, so nothing
 * here opens it as a file this view reads (the server's `ran_elsewhere`).
 */
export function ranElsewhere(args: Record<string, unknown> | null | undefined): boolean {
  return calledOn(args) !== '';
}

/**
 * The computer a tool call's arguments name, folded as the server reads a
 * name (case and runs of spaces), or '' for none: two calls on one path are
 * one file only on one computer.
 */
export function calledOn(args: Record<string, unknown> | null | undefined): string {
  const computer = args?.computer;
  return typeof computer === 'string' ? computer.trim().split(/\s+/).join(' ').toLowerCase() : '';
}

// The sandbox roots the agent sometimes emits, bare or `file:///`-wrapped (see
// normalizeFileRefs.ts), from the generated layout. The trailing slash is
// optional so a bare root collapses to '' instead of surviving as `home/workspace`.
const escapeRe = (v: string) => v.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
export const SANDBOX_ROOTS_ALTERNATION = SANDBOX_ROOT_PREFIXES
  .map((root) => escapeRe(root.replace(/\/$/, '')))
  .join('|');
const SANDBOX_ROOT_RE = new RegExp(`^/?(?:${SANDBOX_ROOTS_ALTERNATION})(?:/|$)`);

/** The computer's own root without its slashes, or '' for none or a stock one. */
function ownRoot(root: string | null | undefined): string {
  const own = root?.replace(/^\/+|\/+$/g, '') ?? '';
  return (SANDBOX_ROOT_PREFIXES as readonly string[]).includes(`${own}/`) ? '' : own;
}

const OWN_ROOTS = new Map<string, { alternation: string; re: RegExp }>();

/**
 * The computer's own root, compiled once per root: a page sees one root per
 * computer, and every path read under it would otherwise build the same
 * expression again.
 */
function ownRootPatterns(own: string): { alternation: string; re: RegExp } {
  let hit = OWN_ROOTS.get(own);
  if (!hit) {
    // A markdown destination is tested before it is decoded, so a root with
    // characters that travel percent-encoded has to match in that spelling too.
    const encoded = own.split('/').map(encodeURIComponent).join('/');
    const alternation = encoded === own ? escapeRe(own) : `${escapeRe(own)}|${escapeRe(encoded)}`;
    // Only the stock roots may drop the leading slash: with a one-segment root
    // like `/data`, a relative `data/x.csv` would otherwise read as rooted.
    const re = new RegExp(`^(?:/(?:${alternation})|/?(?:${SANDBOX_ROOTS_ALTERNATION}))(?:/|$)`);
    hit = { alternation, re };
    OWN_ROOTS.set(own, hit);
  }
  return hit;
}

/**
 * The computer's own root as a regex alternation, in both spellings a
 * reference can hold it in, or '' when the stock roots are all it has.
 */
export function ownRootAlternation(root: string | null | undefined): string {
  const own = ownRoot(root);
  return own ? ownRootPatterns(own).alternation : '';
}

/** The stock roots, plus the computer's own when it is not one of them. */
function sandboxRootRe(root: string | null | undefined): RegExp {
  const own = ownRoot(root);
  return own ? ownRootPatterns(own).re : SANDBOX_ROOT_RE;
}
const FILE_PROTO_RE = /^file:\/\/(?=\/)/;
const WSREF_PREFIX = '__wsref__/';

/**
 * A reference the agent emitted, taken apart once.
 *
 * Every path helper reads a path through this, so a path means the same thing
 * whichever route it arrived by. `path` is the canonical form: `file://`
 * unwrapped, the sandbox root and the `__wsref__/<wsid>/` qualifier stripped,
 * `?query`/`#fragment` dropped, `//` collapsed, `.` dropped and `..` folded.
 * A path still rooted once the sandbox root came off is a real absolute path
 * (`/tmp/x`) and keeps its root; `absolute` covers both, since either way the
 * reference named a fixed spot rather than one relative to the linking file.
 */
export interface AgentPathParts {
  /** Workspace id from a `__wsref__/<wsid>/…` qualifier, when the path carried one. */
  workspaceId?: string;
  path: string;
  absolute: boolean;
  /** The reference ended in `/`, so it named a directory. */
  directory: boolean;
  /** The sandbox root taken off `path`, slash-led, when one was. */
  root?: string;
}

/**
 * The shared reading of a reference. `url` says the input is a markdown
 * destination rather than a path, which decides one rule: see `parseAgentHref`.
 * `root` is the computer's own root (`ComputerFolders.rootDir`), read as a
 * sandbox root alongside the stock ones.
 */
function takeApart(raw: string, url = false, root?: string | null): AgentPathParts {
  let p = raw.trim().replace(FILE_PROTO_RE, '');
  let workspaceId: string | undefined;

  // The qualifier can sit behind a leading slash (`/__wsref__/…`).
  const marked = p.replace(/^\/+/, '');
  if (marked.startsWith(WSREF_PREFIX)) {
    const tail = marked.slice(WSREF_PREFIX.length);
    const slash = tail.indexOf('/');
    if (slash > 0) {
      workspaceId = tail.slice(0, slash);
      p = tail.slice(slash + 1);
    }
  }

  // A link can carry a `?query` or `#fragment`. A path cannot: the `#` in
  // `issue#1.md` is part of the name, and it is a path by the time this runs
  // on it, because the href reading below already decoded the `%23` it
  // travelled as. Stripping on both readings is what truncated it to
  // `results/issue`, which is why the rule is the caller's to choose.
  if (url) p = p.replace(/[?#].*$/, '');

  const rooted = p.startsWith('/');
  const taken = sandboxRootRe(root).exec(p)?.[0];
  const sandbox = taken !== undefined;
  const directory = p.endsWith('/');
  if (sandbox) p = p.slice(taken.length);

  const segments: string[] = [];
  for (const seg of p.split('/')) {
    if (seg === '' || seg === '.') continue;
    if (seg === '..') {
      // A relative reference that climbs above its own start keeps the `..`.
      // Dropping it rewrites `../data.csv` into `data.csv`, which is a
      // different file and often a real one, so the reference opens the wrong
      // document instead of missing; and it erases the one thing the link's
      // reader needs to join it against the directory it was written in.
      // Rooted and sandbox paths start at a root, so they have nowhere to climb.
      if (segments.length && segments[segments.length - 1] !== '..') segments.pop();
      else if (!rooted && !sandbox) segments.push('..');
      continue;
    }
    segments.push(seg);
  }
  const joined = segments.join('/');
  return {
    workspaceId,
    path: (rooted && !sandbox ? '/' : '') + joined + (directory && joined ? '/' : ''),
    absolute: rooted || sandbox,
    directory,
    ...(sandbox ? { root: `/${taken.replace(/^\/+|\/+$/g, '')}` } : {}),
  };
}

/**
 * A path a tool reported, or one already canonical. Idempotent. `root` is the
 * computer's own root, when the caller knows which computer the path is on.
 */
export function parseAgentPath(raw: string, root?: string | null): AgentPathParts {
  return takeApart(raw, false, root);
}

/** @see parseAgentPath */
export function normalizeAgentPath(raw: string, root?: string | null): string {
  return takeApart(raw, false, root).path;
}

/**
 * A markdown destination, which is a URL and not yet a path: the same rules,
 * plus the one that is not idempotent.
 *
 * Percent-decoding happens here and nowhere downstream, so `a%2520b.md` keeps
 * its literal `%20`. It has to happen somewhere: an LLM-emitted link like
 * `[name](results/%E9%95%BF….md)` must reach the API as raw Unicode and be
 * encoded exactly once by the HTTP layer, or the API client re-encodes the leading `%`
 * to `%25` and the backend's single `unquote` looks for a literal `%XX` name.
 */
export function parseAgentHref(raw: string, root?: string | null): AgentPathParts {
  const parts = takeApart(raw, true, root);
  try {
    return {
      ...parts,
      path: decodeURIComponent(parts.path),
      ...(parts.root ? { root: decodeURIComponent(parts.root) } : {}),
    };
  } catch {
    // A lone `%` is a literal here, not a broken escape.
    return parts;
  }
}

/** @see parseAgentHref */
export function normalizeAgentHref(raw: string, root?: string | null): string {
  return parseAgentHref(raw, root).path;
}

export type AgentPathKind = 'memory' | 'memo' | 'user-data' | 'skill' | 'file';
export type MemoryTier = 'user' | 'workspace';

export interface MemoryPathInfo {
  kind: 'memory';
  tier: MemoryTier;
  /** Bare filename, e.g. "memory.md" or "risk-preferences.md". */
  key: string;
  isIndex: boolean;
  /** Original (unnormalized) path for display. */
  rawPath: string;
  /** Workspace id extracted from a `__wsref__/<wsid>/...` cross-workspace ref. */
  crossWorkspaceId?: string;
}

export interface MemoPathInfo {
  kind: 'memo';
  /** Slug from the memo entry (opaque to the frontend; matches MemoEntry.key with ===). */
  key: string;
  isIndex: boolean;
  rawPath: string;
  /** Workspace id extracted from a `__wsref__/<wsid>/...` cross-workspace ref. */
  crossWorkspaceId?: string;
}

export interface SkillPathInfo {
  kind: 'skill';
  /** Directory segment after `.agents/skills/`. */
  name: string;
  rawPath: string;
  /** Workspace id extracted from a `__wsref__/<wsid>/...` cross-workspace ref. */
  crossWorkspaceId?: string;
}

export interface UserDataPathInfo {
  kind: 'user-data';
  entity: UserDataEntity;
  rawPath: string;
  /** Workspace id extracted from a `__wsref__/<wsid>/...` cross-workspace ref. */
  crossWorkspaceId?: string;
}

export interface FilePathInfo {
  kind: 'file';
  rawPath: string;
}

export type AgentPathInfo =
  | MemoryPathInfo
  | MemoPathInfo
  | UserDataPathInfo
  | SkillPathInfo
  | FilePathInfo;

/**
 * Workspace-relative form: the canonical path with any root dropped, so a file
 * shown in the UI reads the same way it routes when clicked. The bare sandbox
 * root collapses to an empty string; the caller picks a label for that case.
 */
export function workspaceRelativePath(rawPath: string, root?: string | null): string {
  return takeApart(rawPath, false, root).path.replace(/^\/+/, '');
}

/** A path through the selected workspace's folder with the folder folded off (`workspaceScopedPath`); any other path as it came. */
function selectedWorkspacePath(
  rawPath: string,
  folders: ComputerFolders | null | undefined,
): string {
  const { dirName, previousDirNames, rootDir, otherRoots } = folders ?? {};
  const parts = takeApart(rawPath, true, rootDir);
  const scoped = workspaceScopedPath(parts, dirName, previousDirNames, otherRoots);
  if (scoped === parts.path) return rawPath;
  return scoped || (parts.directory ? './' : '');
}

// The pre-folder spelling of the workspace memory dir, before a workspace
// became a folder on a shared computer. Stored transcripts still carry it.
const LEGACY_MEMORY_WORKSPACE_DIR = '.agents/workspace/memory';
const WORKSPACE_MEMORY_DIRS = [MEMORY_WORKSPACE_DIR, LEGACY_MEMORY_WORKSPACE_DIR];

/**
 * Classify an agent file path into its semantic domain.
 *
 * Bare names like `memory.md` or `memo.md` (no prefix) fall to `kind: 'file'`
 * because the agent middleware emits the full prefix for store-backed paths.
 *
 * `__wsref__/<wsid>/<rest>` is unwrapped: the inner `<rest>` is re-classified
 * and the extracted `<wsid>` is attached as `crossWorkspaceId` so callers can
 * propagate the workspace context to MemoryPanel/MemoPanel/FilePanel.
 */
export function classifyAgentPath(rawPath: string, root?: string | null): AgentPathInfo {
  if (!rawPath) return { kind: 'file', rawPath };

  // `takeApart` peels the `__wsref__/<wsid>/` qualifier, so classification sees
  // the inner path and the workspace id at once. The original rawPath rides
  // through untouched, so display layers still show the full link; `kind:
  // 'file'` carries no id because Files-tab routing pipes it through
  // `computeAgentArtifactRouting`'s own `targetWorkspaceId` argument instead.
  const { workspaceId: crossWorkspaceId, path } = takeApart(rawPath, true, root);
  const norm = path.replace(/^\/+/, '');

  if (norm.startsWith(`${MEMORY_USER_DIR}/`)) {
    const key = norm.slice(MEMORY_USER_DIR.length + 1);
    // Trailing-slash dir paths (e.g. `.agents/user/memory/`) yield key === ''
    // which would trigger MemoryPanel's not-found banner. Treat as malformed
    // and fall through to Files tab — Glob/bash artifacts emit dir paths there.
    if (!key) {
      return { kind: 'file', rawPath };
    }
    return {
      kind: 'memory',
      tier: 'user',
      key,
      isIndex: key === MEMORY_INDEX_FILENAME,
      rawPath,
      crossWorkspaceId,
    };
  }
  const workspaceMemoryDir = WORKSPACE_MEMORY_DIRS.find((dir) => norm.startsWith(`${dir}/`));
  if (workspaceMemoryDir) {
    const key = norm.slice(workspaceMemoryDir.length + 1);
    if (!key) {
      return { kind: 'file', rawPath };
    }
    return {
      kind: 'memory',
      tier: 'workspace',
      key,
      isIndex: key === MEMORY_INDEX_FILENAME,
      rawPath,
      crossWorkspaceId,
    };
  }
  if (norm.startsWith(`${MEMO_USER_DIR}/`)) {
    const key = norm.slice(MEMO_USER_DIR.length + 1);
    // Empty-string memo key remains the documented LIST-view sentinel
    // (`computeAgentArtifactRouting` maps it to `targetMemoKey: ''`).
    return {
      kind: 'memo',
      key,
      isIndex: key === MEMO_INDEX_FILENAME,
      rawPath,
      crossWorkspaceId,
    };
  }
  // Only the data files themselves: the README beside them, and any other name
  // in those directories, stays a generic file.
  const entity = userDataEntity(norm);
  if (entity) {
    return { kind: 'user-data', entity, rawPath, crossWorkspaceId };
  }
  if (norm.startsWith(`${SKILLS_DIR}/`)) {
    const tail = norm.slice(SKILLS_DIR.length + 1);
    const name = tail.split('/')[0] || '';
    return { kind: 'skill', name, rawPath, crossWorkspaceId };
  }
  return { kind: 'file', rawPath };
}

/** Strip `.md` and turn `risk-preferences` / `risk_preferences` into `risk preferences`. */
export function topicFromMemoryKey(key: string): string {
  if (!key) return '';
  let topic = key;
  if (topic.toLowerCase().endsWith('.md')) topic = topic.slice(0, -3);
  return topic.replace(/[-_]+/g, ' ').trim();
}

/**
 * Routing decision returned by `computeAgentArtifactRouting`. The ChatView
 * routing handler applies these by clearing all four target fields first and
 * then assigning whichever the routing returned. Empty-string targetMemoKey
 * means "open Memo tab to LIST view (no entry pre-select)" — used for the
 * memo index path.
 */
export interface AgentArtifactRouting {
  targetFile: string | null;
  targetMemoryKey: string | null;
  targetMemoryTier: MemoryTier | null;
  /** `''` (empty string) means open Memo tab without selecting; null means no memo target. */
  targetMemoKey: string | null;
  /** True when the routing must clear filePanelWorkspaceId (user-scoped artifact). */
  clearWorkspaceId: boolean;
  /** Workspace id to set on filePanelWorkspaceId (only for cross-workspace file links). */
  setWorkspaceId: string | null;
  /** A folder to open the Files tab on, for a link ending in `/`; `''` is the workspace root. */
  targetDirectory: string | null;
}

/**
 * Pure routing decision. Caller (ChatView) applies the resulting state
 * transitions atomically. Centralized here so it's testable without mounting
 * the whole chat shell.
 *
 * `targetWorkspaceId` takes precedence over a `__wsref__/<wsid>/...` embedded
 * id — caller-supplied context wins. When neither is provided for a workspace-
 * tier memory path, we emit `clearWorkspaceId: true` so a stale
 * filePanelWorkspaceId from a prior cross-workspace click doesn't leak into
 * the new query (a regression guard for chats whose links name other
 * workspaces).
 *
 * A path through another workspace's folder (`folderWorkspacePath`) routes
 * as the `__wsref__` link to that workspace would. It is read as a path, which
 * is what an `OpenFileHandler` is handed, so a `#` inside a name stays.
 */
export function computeAgentArtifactRouting(
  rawPath: string,
  targetWorkspaceId?: string,
  folders?: ComputerFolders | null,
): AgentArtifactRouting {
  const root = folders?.rootDir;
  const other = targetWorkspaceId ? null : folderWorkspacePath(takeApart(rawPath, false, root), folders);
  if (other) return computeAgentArtifactRouting(other.path, other.workspaceId);
  const routedPath = selectedWorkspacePath(rawPath, folders);
  const info = classifyAgentPath(routedPath, root);
  // Caller-supplied wsid wins; otherwise fall back to the wsid extracted from
  // a `__wsref__/...` marker in the path itself.
  const embeddedWsid = info.kind === 'file' ? undefined : info.crossWorkspaceId;
  const resolvedWsid = targetWorkspaceId ?? embeddedWsid ?? null;

  const base: AgentArtifactRouting = {
    targetFile: null,
    targetMemoryKey: null,
    targetMemoryTier: null,
    targetMemoKey: null,
    clearWorkspaceId: false,
    setWorkspaceId: null,
    targetDirectory: null,
  };
  if (info.kind === 'memory') {
    if (info.tier === 'user') {
      // User memory is global to the user — workspace context is meaningless,
      // and any stale ws id from a cross-workspace `ws://` link must be cleared so the
      // memory query doesn't leak into the wrong workspace.
      return {
        ...base,
        targetMemoryKey: info.key,
        targetMemoryTier: 'user',
        clearWorkspaceId: true,
      };
    }
    // Workspace memory:
    //  - With a resolved wsid (caller-supplied OR embedded `__wsref__`), set
    //    it on the panel so MemoryPanel queries the correct workspace.
    //  - Without one, clear filePanelWorkspaceId. Leaving it as-is leaks a
    //    stale wsid from a prior cross-workspace click into the new memory
    //    list query.
    if (resolvedWsid) {
      return {
        ...base,
        targetMemoryKey: info.key,
        targetMemoryTier: 'workspace',
        setWorkspaceId: resolvedWsid,
      };
    }
    return {
      ...base,
      targetMemoryKey: info.key,
      targetMemoryTier: 'workspace',
      clearWorkspaceId: true,
    };
  }
  if (info.kind === 'memo') {
    return {
      ...base,
      targetMemoKey: info.isIndex ? '' : info.key,
      clearWorkspaceId: true,
    };
  }
  if (info.kind === 'user-data') {
    // The DB-backed user files are user-scoped (not workspace-scoped), so
    // clear any stale filePanelWorkspaceId from a prior cross-workspace click,
    // the same pattern as user-tier memory.
    return {
      ...base,
      targetFile: routedPath,
      clearWorkspaceId: true,
    };
  }
  // skill / file → Files tab; pass-through workspace id for cross-workspace links.
  const parts = takeApart(routedPath, true, root);
  if (parts.directory) {
    return {
      ...base,
      targetDirectory: parts.path.replace(/^\/+|\/+$/g, ''),
      setWorkspaceId: resolvedWsid,
    };
  }
  return {
    ...base,
    targetFile: routedPath,
    setWorkspaceId: resolvedWsid,
  };
}
