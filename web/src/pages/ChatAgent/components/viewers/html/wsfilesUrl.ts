import { workspaceRelativePath } from '../../../utils/agentPaths';

// Strip any trailing slash so a base like `https://host/` doesn't produce a
// double slash (`//api/v1/...`) once the `/api/v1/...` path is appended.
// Built off the same base as the API client (api/client.ts).
const API_BASE = (import.meta.env.VITE_API_BASE_URL ?? '').replace(/\/+$/, '');

function encodePathSegments(filePath: string, root?: string | null): string {
  // A served path is read the same way every other reference is: a sandbox
  // root (`/home/workspace/x`, `file:///home/daytona/x`, or the computer's
  // own) names the workspace and comes off, rather than riding into the URL
  // as literal segments.
  return workspaceRelativePath(filePath, root)
    .split('/')
    .map(encodeURIComponent)
    .join('/');
}

export interface ServeUrlOptions {
  injectTheme?: boolean;
  /** The computer's root, which an absolute path may start with. */
  root?: string | null;
}

/** The ?format=pdf query string, with the optional render knobs appended. */
export function pdfQuery(scale?: number, pageNumbers?: boolean, branding?: boolean): string {
  let q = 'format=pdf';
  if (scale != null && scale !== 1) q += `&scale=${scale}`;
  if (pageNumbers) q += '&page_numbers=true';
  if (branding === false) q += '&branding=false';
  return q;
}

/**
 * A file under a serve prefix the server handed out: the owner's signed grant
 * (`/api/v1/wsfiles/g/<grant>/`) or a share's `frame_base`. The prefix is the
 * credential. A grant names its workspace, but the id alone no longer opens
 * anything, and the grant expires.
 *
 * Path segments are URI-encoded but slashes are preserved, so a document at
 * `<prefix>results/report.html` resolves its relative `charts/x.png` to
 * `<prefix>results/charts/x.png` with no extra machinery.
 */
export function buildServeUrl(
  prefix: string,
  filePath: string,
  { injectTheme = false, root }: ServeUrlOptions = {},
): string {
  const base = /^https?:\/\//i.test(prefix) ? prefix : `${API_BASE}${prefix}`;
  const url = `${base.replace(/\/+$/, '')}/${encodePathSegments(filePath, root)}`;
  return injectTheme ? `${url}?inject=theme` : url;
}
