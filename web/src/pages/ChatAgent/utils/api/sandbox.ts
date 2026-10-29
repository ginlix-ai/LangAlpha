/**
 * Sandbox stats, packages and preview endpoints.
 */
import { api } from '@/api/client';

export async function getSandboxStats(workspaceId: string) {
  const { data } = await api.get(`/api/v1/workspaces/${workspaceId}/sandbox/stats`);
  return data;
}

export async function installSandboxPackages(workspaceId: string, packages: string[]) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/packages`, { packages });
  return data;
}

export async function refreshWorkspace(workspaceId: string) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/refresh`);
  return data;
}

export async function getPreviewUrl(workspaceId: string, port: number, command?: string, force?: boolean) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/preview-url`, {
    port,
    ...(command && { command }),
    ...(force && { force: true }),
  });
  return data;
}

/**
 * A signed preview URL points at the app's root; a tool result may name a
 * page inside it, e.g. "/timeline.html?tab=2#top". The suffix is split the
 * way a browser reads it: the pathname setter would percent-encode a `?` or
 * `#` handed to it, and the app would be asked for a file by that name.
 */
export function appendPathSuffix(baseUrl: string, path?: string): string {
  if (!path) return baseUrl;
  try {
    const parsed = new URL(baseUrl);
    const hashAt = path.indexOf('#');
    const hash = hashAt >= 0 ? path.slice(hashAt) : '';
    const beforeHash = hashAt >= 0 ? path.slice(0, hashAt) : path;
    const queryAt = beforeHash.indexOf('?');
    const query = queryAt >= 0 ? beforeHash.slice(queryAt + 1) : '';
    const pathname = queryAt >= 0 ? beforeHash.slice(0, queryAt) : beforeHash;
    if (pathname) {
      parsed.pathname = parsed.pathname.replace(/\/+$/, '') + (pathname.startsWith('/') ? pathname : `/${pathname}`);
    }
    // Appended, not replaced: the signed URL's own query is what admits the request.
    for (const [k, v] of new URLSearchParams(query)) parsed.searchParams.append(k, v);
    if (hash) parsed.hash = hash;
    return parsed.toString();
  } catch {
    return baseUrl;
  }
}

export async function checkPreviewHealth(workspaceId: string, port: number) {
  const { data } = await api.post(`/api/v1/workspaces/${workspaceId}/sandbox/preview-health`, { port });
  return data as { reachable: boolean; checked_at: number };
}

// --- Thread Sharing ---
