// The sandbox layout is emitted from src/ptc_agent/core/paths.py: see
// agentPaths.generated.ts. Re-exported here so callers keep one import.
import {
  MEMO_INDEX_FILENAME,
  MEMO_USER_DIR,
  MEMORY_INDEX_FILENAME,
  MEMORY_USER_DIR,
  MEMORY_WORKSPACE_DIR,
  SANDBOX_ROOT_PREFIXES,
  SKILLS_DIR,
  USER_PROFILE_DIR,
  USER_PROFILE_FILES,
} from './agentPaths.generated';

export {
  MEMO_INDEX_FILENAME,
  MEMO_USER_DIR,
  MEMORY_INDEX_FILENAME,
  MEMORY_USER_DIR,
  MEMORY_WORKSPACE_DIR,
  SKILLS_DIR,
  USER_PROFILE_DIR,
  USER_PROFILE_FILES,
};

export type UserProfileEntity = keyof typeof USER_PROFILE_FILES;

/** The schema documentation file the agent reads to learn the JSON shapes.
 *  Treated as chatter and hidden from the chat timeline. */
export const USER_PROFILE_README_FILENAME = 'README.md';

/**
 * True when `rawPath` resolves to `.agents/user/profile/README.md` under any of
 * the prefixes the agent emits (relative, absolute sandbox root, `file://`,
 * `__wsref__/<wsid>/...`). Used by the chat timeline to suppress the agent's
 * "Read schema doc" calls — they're not user-actionable.
 */
export function isUserProfileReadmePath(rawPath: string): boolean {
  if (!rawPath) return false;
  return workspaceRelativePath(rawPath) === `${USER_PROFILE_DIR}/${USER_PROFILE_README_FILENAME}`;
}

// The sandbox roots the agent sometimes emits, bare or `file:///`-wrapped (see
// normalizeFileRefs.ts), from the generated layout. The trailing slash is
// optional so a bare root collapses to '' instead of surviving as `home/workspace`.
const SANDBOX_ROOTS_ALTERNATION = SANDBOX_ROOT_PREFIXES
  .map((root) => root.replace(/\/$/, '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
  .join('|');
const SANDBOX_ROOT_RE = new RegExp(`^/?(?:${SANDBOX_ROOTS_ALTERNATION})(?:/|$)`);
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
}

/**
 * The shared reading of a reference. `url` says the input is a markdown
 * destination rather than a path, which decides one rule: see `normalizeAgentHref`.
 */
function takeApart(raw: string, url = false): AgentPathParts {
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
  const sandbox = SANDBOX_ROOT_RE.test(p);
  const directory = p.endsWith('/');
  if (sandbox) p = p.replace(SANDBOX_ROOT_RE, '');

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
  };
}

/** A path a tool reported, or one already canonical. Idempotent. */
export function parseAgentPath(raw: string): AgentPathParts {
  return takeApart(raw);
}

/** @see parseAgentPath */
export function normalizeAgentPath(raw: string): string {
  return takeApart(raw).path;
}

/**
 * A markdown destination, which is a URL and not yet a path: the same rules,
 * plus the one that is not idempotent.
 *
 * Percent-decoding happens here and nowhere downstream, so `a%2520b.md` keeps
 * its literal `%20`. It has to happen somewhere: an LLM-emitted link like
 * `[name](results/%E9%95%BF….md)` must reach the API as raw Unicode and be
 * encoded exactly once by the HTTP layer, or Axios re-encodes the leading `%`
 * to `%25` and the backend's single `unquote` looks for a literal `%XX` name.
 */
export function normalizeAgentHref(raw: string): string {
  const { path } = takeApart(raw, true);
  try {
    return decodeURIComponent(path);
  } catch {
    // A lone `%` is a literal here, not a broken escape.
    return path;
  }
}

export type AgentPathKind = 'memory' | 'memo' | 'user-profile' | 'skill' | 'file';
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

export interface UserProfilePathInfo {
  kind: 'user-profile';
  /** Which of the three entities this path targets. */
  entity: UserProfileEntity;
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
  | UserProfilePathInfo
  | SkillPathInfo
  | FilePathInfo;

/**
 * Workspace-relative form: the canonical path with any root dropped, so a file
 * shown in the UI reads the same way it routes when clicked. The bare sandbox
 * root collapses to an empty string; the caller picks a label for that case.
 */
export function workspaceRelativePath(rawPath: string): string {
  return takeApart(rawPath).path.replace(/^\/+/, '');
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
export function classifyAgentPath(rawPath: string): AgentPathInfo {
  if (!rawPath) return { kind: 'file', rawPath };

  // `takeApart` peels the `__wsref__/<wsid>/` qualifier, so classification sees
  // the inner path and the workspace id at once. The original rawPath rides
  // through untouched, so display layers still show the full link; `kind:
  // 'file'` carries no id because Files-tab routing pipes it through
  // `computeAgentArtifactRouting`'s own `targetWorkspaceId` argument instead.
  const { workspaceId: crossWorkspaceId, path } = takeApart(rawPath, true);
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
  if (norm.startsWith(`${USER_PROFILE_DIR}/`)) {
    const tail = norm.slice(USER_PROFILE_DIR.length + 1);
    // Match the bare basename (no subdirs handled — UserDataBackend only
    // serves the three known files at the prefix).
    const entity = (Object.entries(USER_PROFILE_FILES) as [UserProfileEntity, string][])
      .find(([, filename]) => filename === tail)?.[0];
    if (entity) {
      return { kind: 'user-profile', entity, rawPath, crossWorkspaceId };
    }
    // Unknown user/profile path → fall through to generic file.
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
  /** When set, opens the Files tab on the user-profile JSON file (served virtually
   *  by workspace_files via UserDataBackend). Mutually exclusive with the others. */
  targetUserProfile: UserProfileEntity | null;
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
 * the new query (flash-mode regression guard).
 */
export function computeAgentArtifactRouting(
  rawPath: string,
  targetWorkspaceId?: string,
): AgentArtifactRouting {
  const info = classifyAgentPath(rawPath);
  // Caller-supplied wsid wins; otherwise fall back to the wsid extracted from
  // a `__wsref__/...` marker in the path itself.
  const embeddedWsid =
    info.kind === 'memory'
    || info.kind === 'memo'
    || info.kind === 'skill'
    || info.kind === 'user-profile'
      ? info.crossWorkspaceId
      : undefined;
  const resolvedWsid = targetWorkspaceId ?? embeddedWsid ?? null;

  const base: AgentArtifactRouting = {
    targetFile: null,
    targetMemoryKey: null,
    targetMemoryTier: null,
    targetMemoKey: null,
    targetUserProfile: null,
    clearWorkspaceId: false,
    setWorkspaceId: null,
    targetDirectory: null,
  };
  if (info.kind === 'memory') {
    if (info.tier === 'user') {
      // User memory is global to the user — workspace context is meaningless,
      // and any stale ws id from a flash `ws://` link must be cleared so the
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
    //  - Without one, clear filePanelWorkspaceId. Flash mode previously left
    //    it as-is, leaking a stale wsid from a prior cross-workspace click
    //    into the new memory list query.
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
  if (info.kind === 'user-profile') {
    // User-profile data is user-scoped (not workspace-scoped), so clear any
    // stale filePanelWorkspaceId from a prior cross-workspace click — same
    // pattern as user-tier memory.
    return {
      ...base,
      targetUserProfile: info.entity,
      targetFile: rawPath,
      clearWorkspaceId: true,
    };
  }
  // skill / file → Files tab; pass-through workspace id for cross-workspace links.
  const parts = takeApart(rawPath, true);
  if (parts.directory) {
    return {
      ...base,
      targetDirectory: parts.path.replace(/^\/+|\/+$/g, ''),
      setWorkspaceId: resolvedWsid,
    };
  }
  return {
    ...base,
    targetFile: rawPath,
    setWorkspaceId: resolvedWsid,
  };
}
