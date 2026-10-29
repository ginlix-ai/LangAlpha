import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { appendPathSuffix, getPreviewUrl } from '../../utils/api';
import type { PreviewSpec } from './types';

/**
 * A dev server the agent started in the sandbox, as the panel knows it.
 *
 * `url` is a short-lived signed URL, which is why it is state and never
 * storage: a preview tab persists as a port, and coming back to it mints a
 * fresh one. Everything else here is what that minting needs.
 */
export interface PreviewEntry {
  port: number;
  title?: string;
  /** A path suffix on the served app, e.g. `/timeline.html`. */
  path?: string;
  /** How the agent started the server; the backend replays it when the port is idle. */
  command?: string;
  url: string;
  /** When `url` was minted (ms since epoch); a reopen past `URL_FRESH_MS` mints again. */
  mintedAt?: number;
  loading: boolean;
  error: boolean;
  /** Bumped when the iframe must reload although the URL is unchanged. */
  reloadToken: number;
}

/**
 * How long a minted URL is trusted on a reopen. The signed URL outlives this,
 * but a tab left in the background for longer is more likely to come back to a
 * dead link than to save a request, and the re-mint is one call.
 */
export const URL_FRESH_MS = 10 * 60_000;

/** A caller that names nothing keeps what the entry already knows — the tree
 *  reopens an app by port alone and must not blank the agent's own labels. */
function merge(spec: PreviewSpec, prev?: PreviewEntry): PreviewEntry {
  return {
    url: '', loading: false, error: false, reloadToken: 0,
    ...prev,
    port: spec.port,
    ...(spec.title !== undefined && { title: spec.title }),
    ...(spec.path !== undefined && { path: spec.path }),
    ...(spec.command !== undefined && { command: spec.command }),
  };
}

const sameLabels = (a: PreviewEntry, b: PreviewEntry) =>
  a.title === b.title && a.path === b.path && a.command === b.command;

/**
 * The running apps this panel knows about, keyed by port.
 *
 * It lives beside the tabs rather than inside the viewer because two surfaces
 * read it: the tab showing one app, and the tree group listing them all. An
 * entry outlives its tab — closing a tab closes a view, it does not stop the
 * server — so the group stays the way back to an app already started.
 */
export function usePreviews(workspaceId: string) {
  const [map, setMap] = useState<Map<number, PreviewEntry>>(() => new Map());
  // Read by callbacks that must not take `map` as a dependency: they land in
  // FilePanel effect deps, and a new identity per URL arrival would re-run them.
  const mapRef = useRef(map);
  mapRef.current = map;
  // Per-port request counter: a slow answer for a port asked again since must
  // not land on top of the newer one.
  const seqRef = useRef<Map<number, number>>(new Map());
  const aliveRef = useRef(true);
  useEffect(() => () => { aliveRef.current = false; }, []);

  // A different workspace is a different set of servers, not more of these.
  const lastWorkspace = useRef(workspaceId);
  useEffect(() => {
    if (lastWorkspace.current === workspaceId) return;
    lastWorkspace.current = workspaceId;
    seqRef.current = new Map();
    setMap(new Map());
  }, [workspaceId]);

  const patch = useCallback((port: number, fields: (entry: PreviewEntry) => Partial<PreviewEntry>) => {
    setMap((prev) => {
      const entry = prev.get(port);
      if (!entry) return prev;
      const next = new Map(prev);
      next.set(port, { ...entry, ...fields(entry) });
      return next;
    });
  }, []);

  /** Mint a signed URL for a port and land it, unless a newer request overtook this one. */
  const mint = useCallback(async (spec: PreviewSpec, force: boolean) => {
    if (!workspaceId) return;
    const ws = workspaceId;
    const { port } = spec;
    const ticket = (seqRef.current.get(port) ?? 0) + 1;
    seqRef.current.set(port, ticket);
    patch(port, () => ({ loading: true, error: false }));
    const ask = () => getPreviewUrl(ws, port, spec.command, force);
    // A workspace switch resets the counters, so a ticket alone could pass
    // again in the next workspace on the same port.
    const stale = () => !aliveRef.current || lastWorkspace.current !== ws || seqRef.current.get(port) !== ticket;
    try {
      let result: { url: string };
      try {
        result = await ask();
      } catch (err: unknown) {
        // 503 is a stopped sandbox: the retry is what starts it back up, and it
        // can only succeed when we know how the server was started.
        const status = (err as { response?: { status?: number } })?.response?.status;
        if (status !== 503 || !spec.command) throw err;
        result = await ask();
      }
      if (stale()) return;
      patch(port, (entry) => ({
        url: appendPathSuffix(result.url, spec.path ?? entry.path),
        mintedAt: Date.now(),
        loading: false,
        error: false,
        // The token exists to reload a frame the URL alone would leave alone.
        // A first URL mounts the frame by itself, and bumping here would tear
        // that frame down and load the app a second time.
        reloadToken: entry.url ? entry.reloadToken + 1 : entry.reloadToken,
      }));
    } catch {
      if (stale()) return;
      patch(port, () => ({ url: '', loading: false, error: true }));
    }
  }, [workspaceId, patch]);

  /** Note an app without reaching for it — what a restored tab and the tree list need. */
  const register = useCallback((spec: PreviewSpec) => {
    setMap((prev) => {
      const before = prev.get(spec.port);
      const next = merge(spec, before);
      if (before && sameLabels(before, next)) return prev;
      const out = new Map(prev);
      out.set(spec.port, next);
      return out;
    });
  }, []);

  /** The agent published an app, or the reader asked for it again: always a fresh URL. */
  const open = useCallback((spec: PreviewSpec) => {
    register(spec);
    void mint(merge(spec, mapRef.current.get(spec.port)), false);
  }, [register, mint]);

  /**
   * Coming back to a preview tab. A URL minted recently is still good, an older
   * one is minted again before it is trusted, and a failure stays a failure
   * until Refresh is pressed: re-minting on every activation would hammer a
   * port nothing is listening on.
   */
  const ensure = useCallback((port: number) => {
    const entry = mapRef.current.get(port);
    if (!entry || entry.loading || entry.error) return;
    const fresh = entry.url && Date.now() - (entry.mintedAt ?? 0) < URL_FRESH_MS;
    if (fresh) return;
    void mint(entry, false);
  }, [mint]);

  /** Refresh: restart the process behind the port and take a new URL. */
  const refresh = useCallback((port: number) => {
    const entry = mapRef.current.get(port);
    if (entry) void mint(entry, true);
  }, [mint]);

  const previews = useMemo(() => [...map.values()].sort((a, b) => a.port - b.port), [map]);

  // One object until an entry changes: the panes and the tree's open handler
  // take it whole, and a new one per panel render re-rendered both.
  return useMemo(
    () => ({ previews, byPort: map, register, open, ensure, refresh }),
    [previews, map, register, open, ensure, refresh],
  );
}

export type PreviewsApi = ReturnType<typeof usePreviews>;
