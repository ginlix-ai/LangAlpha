// Source of truth for memory/memo dirs: src/ptc_agent/core/paths.py.
// SKILLS_DIR is hardcoded here; the same '.agents/skills' literal is
// duplicated across several backend files (sandbox builder, agent config
// loader, skills middleware) — there's no shared constant yet, so any
// rename has to touch the literal in each place.
export const MEMORY_USER_DIR = '.agents/user/memory';
export const MEMORY_WORKSPACE_DIR = '.agents/workspace/memory';
export const MEMO_USER_DIR = '.agents/user/memo';
export const SKILLS_DIR = '.agents/skills';
export const MEMORY_INDEX_FILENAME = 'memory.md';
export const MEMO_INDEX_FILENAME = 'memo.md';

// Sandbox roots the agent sometimes emits as either bare absolute or
// `file:///`-wrapped paths (see normalizeFileRefs.ts). Both `workspace` and
// `daytona` variants appear in the wild.
const SANDBOX_ROOT_PREFIXES = [
  'home/workspace/',
  'home/daytona/',
];
const FILE_PROTO_PREFIX = 'file:///';
const WSREF_PREFIX = '__wsref__/';

export type AgentPathKind = 'memory' | 'memo' | 'skill' | 'file';
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

export interface FilePathInfo {
  kind: 'file';
  rawPath: string;
}

export type AgentPathInfo =
  | MemoryPathInfo
  | MemoPathInfo
  | SkillPathInfo
  | FilePathInfo;

/**
 * Canonicalize the various path shapes the agent emits before classification:
 *   - leading `/`, `./`, double-slashes
 *   - `file:///home/(workspace|daytona)/...` → relative
 *   - `/home/(workspace|daytona)/...` → relative
 *   - trailing `?query` / `#fragment` stripped
 *
 * `__wsref__/<wsid>/...` is handled at the classifier layer (it needs to
 * extract the wsid before recursing on the inner path).
 */
function normalizePath(rawPath: string): string {
  let p = rawPath;

  // Strip query string and fragment first — they don't affect classification
  // but break suffix checks like `.endsWith('.md')`.
  p = p.split(/[?#]/)[0];

  // Unwrap file:/// prefix (markdown auto-link form).
  if (p.startsWith(FILE_PROTO_PREFIX)) {
    p = p.slice(FILE_PROTO_PREFIX.length);
  }

  // Strip leading slashes (covers absolute `/home/...` and `/.agents/...`),
  // iteratively strip leading `./`, and collapse double-slashes from path joins.
  p = p.replace(/^\/+/, '');
  while (p.startsWith('./')) {
    p = p.slice(2);
  }
  p = p.replace(/\/{2,}/g, '/');

  // Strip the sandbox-root prefix the agent sometimes emits.
  for (const prefix of SANDBOX_ROOT_PREFIXES) {
    if (p.startsWith(prefix)) {
      p = p.slice(prefix.length);
      break;
    }
  }

  return p;
}

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

  // Unwrap `__wsref__/<wsid>/<rest>` first. Handles a leading `/` before the
  // marker too (some agent variants emit `/__wsref__/...`).
  const wsrefStripped = rawPath.replace(/^\/+/, '');
  if (wsrefStripped.startsWith(WSREF_PREFIX)) {
    const tail = wsrefStripped.slice(WSREF_PREFIX.length);
    const slashIdx = tail.indexOf('/');
    if (slashIdx > 0) {
      const wsid = tail.slice(0, slashIdx);
      const inner = tail.slice(slashIdx + 1);
      const innerInfo = classifyAgentPath(inner);
      // Re-attach the original rawPath (so display layers show the full link)
      // and decorate with the extracted workspace id. `kind: 'file'` doesn't
      // need the marker — Files tab routing pipes setWorkspaceId through the
      // routing function's targetWorkspaceId arg.
      if (innerInfo.kind === 'memory' || innerInfo.kind === 'memo' || innerInfo.kind === 'skill') {
        return { ...innerInfo, rawPath, crossWorkspaceId: wsid };
      }
      return { ...innerInfo, rawPath };
    }
  }

  const norm = normalizePath(rawPath);

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
    };
  }
  if (norm.startsWith(`${MEMORY_WORKSPACE_DIR}/`)) {
    const key = norm.slice(MEMORY_WORKSPACE_DIR.length + 1);
    if (!key) {
      return { kind: 'file', rawPath };
    }
    return {
      kind: 'memory',
      tier: 'workspace',
      key,
      isIndex: key === MEMORY_INDEX_FILENAME,
      rawPath,
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
    };
  }
  if (norm.startsWith(`${SKILLS_DIR}/`)) {
    const tail = norm.slice(SKILLS_DIR.length + 1);
    const name = tail.split('/')[0] || '';
    return { kind: 'skill', name, rawPath };
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
    info.kind === 'memory' || info.kind === 'memo' || info.kind === 'skill'
      ? info.crossWorkspaceId
      : undefined;
  const resolvedWsid = targetWorkspaceId ?? embeddedWsid ?? null;

  const base: AgentArtifactRouting = {
    targetFile: null,
    targetMemoryKey: null,
    targetMemoryTier: null,
    targetMemoKey: null,
    clearWorkspaceId: false,
    setWorkspaceId: null,
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
  // skill / file → Files tab; pass-through workspace id for cross-workspace links.
  return {
    ...base,
    targetFile: rawPath,
    setWorkspaceId: resolvedWsid,
  };
}
