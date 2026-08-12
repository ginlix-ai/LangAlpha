/**
 * Build the served-file URL for a workspace file.
 *
 * Path segments are URI-encoded but slashes are preserved, so a document at
 * `.../wsfiles/{ws}/results/report.html` resolves its relative `charts/x.png`
 * reference to `.../wsfiles/{ws}/results/charts/x.png` with no extra machinery.
 * Built off the same base as the axios client (api/client.ts).
 */
// Strip any trailing slash so a base like `https://host/` doesn't produce a
// double slash (`//api/v1/...`) once the `/api/v1/...` path is appended.
const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/+$/, '');

function encodePathSegments(filePath: string): string {
  return filePath
    .replace(/^\/+/, '')
    .split('/')
    .map(encodeURIComponent)
    .join('/');
}

export interface ServeUrlOptions {
  injectTheme?: boolean;
  format?: 'pdf';
  /** PDF only: render scale (server clamps to 0.5–2). Omitted from the URL at 1. */
  pdfScale?: number;
  /** PDF only: draw an 'N / total' footer in the page margin. */
  pdfPageNumbers?: boolean;
  /** PDF only: the "langalpha · <date>" footer. Server default is on; only
   *  an explicit false reaches the URL. */
  pdfBranding?: boolean;
}

/** The ?format=pdf query string, with the optional render knobs appended. */
export function pdfQuery(scale?: number, pageNumbers?: boolean, branding?: boolean): string {
  let q = 'format=pdf';
  if (scale != null && scale !== 1) q += `&scale=${scale}`;
  if (pageNumbers) q += '&page_numbers=true';
  if (branding === false) q += '&branding=false';
  return q;
}

export function buildWsfilesUrl(
  workspaceId: string,
  filePath: string,
  { injectTheme = false, format, pdfScale, pdfPageNumbers, pdfBranding }: ServeUrlOptions = {},
): string {
  const url = `${API_BASE}/api/v1/wsfiles/${encodeURIComponent(workspaceId)}/${encodePathSegments(filePath)}`;
  if (format === 'pdf') return `${url}?${pdfQuery(pdfScale, pdfPageNumbers, pdfBranding)}`;
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
  { injectTheme = false, format, pdfScale, pdfPageNumbers, pdfBranding }: ServeUrlOptions = {},
): string {
  const url = `${API_BASE}/api/v1/public/shared/${encodeURIComponent(shareToken)}/files/serve/${encodePathSegments(filePath)}`;
  if (format === 'pdf') return `${url}?${pdfQuery(pdfScale, pdfPageNumbers, pdfBranding)}`;
  return injectTheme ? `${url}?inject=theme` : url;
}
