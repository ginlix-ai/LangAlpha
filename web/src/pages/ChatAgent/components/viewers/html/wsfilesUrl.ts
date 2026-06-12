/**
 * Build the served-file URL for a workspace file.
 *
 * Path segments are URI-encoded but slashes are preserved, so a document at
 * `.../wsfiles/{ws}/results/report.html` resolves its relative `charts/x.png`
 * reference to `.../wsfiles/{ws}/results/charts/x.png` with no extra machinery.
 * Built off the same base as the axios client (api/client.ts).
 */
const API_BASE = import.meta.env.VITE_API_BASE_URL ?? '';

function encodePathSegments(filePath: string): string {
  return filePath
    .replace(/^\/+/, '')
    .split('/')
    .map(encodeURIComponent)
    .join('/');
}

export function buildWsfilesUrl(
  workspaceId: string,
  filePath: string,
  { injectTheme = false }: { injectTheme?: boolean } = {},
): string {
  const url = `${API_BASE}/api/v1/wsfiles/${encodeURIComponent(workspaceId)}/${encodePathSegments(filePath)}`;
  return injectTheme ? `${url}?inject=theme` : url;
}

/**
 * Build the public share serve URL for a shared report file.
 *
 * The workspace UUID never appears — access is scoped to the revocable share
 * token. Path-style, so a served document's relative subresources resolve under
 * the same token prefix.
 */
export function buildSharedServeUrl(
  shareToken: string,
  filePath: string,
  { injectTheme = false }: { injectTheme?: boolean } = {},
): string {
  const url = `${API_BASE}/api/v1/public/shared/${encodeURIComponent(shareToken)}/files/serve/${encodePathSegments(filePath)}`;
  return injectTheme ? `${url}?inject=theme` : url;
}
