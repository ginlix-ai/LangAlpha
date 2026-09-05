/**
 * The `?detail=` deep link for the Plugins page detail overlays:
 * `detail=server:NAME | brokerage:NAME | skill:NAME | plugin:NAME`, plus `dws=<workspace_id>`
 * when the row is workspace-scoped. Names cannot contain ':' (validated
 * server-side), so the first colon splits unambiguously. Each tab renders
 * only its own kind; opening another kind's detail switches the tab in the
 * same navigation.
 */

export type DetailKind = 'server' | 'brokerage' | 'skill' | 'plugin';

export interface DetailRef {
  kind: DetailKind;
  name: string;
  workspaceId: string | null;
}

const KINDS: ReadonlySet<string> = new Set(['server', 'brokerage', 'skill', 'plugin']);

/**
 * The tab that renders each kind. A `?detail=` link that names no tab is
 * still unambiguous about which one it needs, and only that tab is mounted to
 * resolve it, so the page reads the tab out of the kind rather than opening
 * on its default and leaving the ref unanswered.
 */
export const DETAIL_KIND_TAB: Record<DetailKind, string> = {
  server: 'mcp',
  // Its own kind rather than `server:NAME` on the Connectors tab, even though
  // a connected brokerage IS a catalog row there: a brokerage has a detail
  // before it has a row, and the link has to land where it is listed.
  brokerage: 'brokerages',
  skill: 'skills',
  plugin: 'plugins',
};

export function parseDetail(params: URLSearchParams): DetailRef | null {
  const raw = params.get('detail');
  if (!raw) return null;
  const idx = raw.indexOf(':');
  if (idx <= 0) return null;
  const kind = raw.slice(0, idx);
  const name = raw.slice(idx + 1);
  if (!name || !KINDS.has(kind)) return null;
  return { kind: kind as DetailKind, name, workspaceId: params.get('dws') };
}

/** A copy of `params` with the detail keys set (or cleared, for null). */
export function withDetail(
  params: URLSearchParams,
  ref: { kind: DetailKind; name: string; workspaceId?: string | null } | null,
): URLSearchParams {
  const next = new URLSearchParams(params);
  next.delete('detail');
  next.delete('dws');
  if (ref) {
    next.set('detail', `${ref.kind}:${ref.name}`);
    if (ref.workspaceId) next.set('dws', ref.workspaceId);
  }
  return next;
}
