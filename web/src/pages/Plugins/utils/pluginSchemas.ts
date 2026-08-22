/**
 * Client mirror of the plugin install source policy. The backend stays
 * authoritative (services/plugins/fetch.py + install.py); this exists so the
 * wizard can refuse an install that is certain to fail before uploading.
 */
import { validateRemoteUrl } from '@/pages/ChatAgent/components/mcp/mcpSchemas';

/** Mirrors MAX_PACKAGE_BYTES (services/plugins/fetch.py). */
export const MAX_PLUGIN_ZIP_BYTES = 20 * 1024 * 1024;

/** Zips arrive as application/zip, application/x-zip-compressed, or an empty
 * MIME depending on browser, so extension is the fallback check. */
const ZIP_MIME_TYPES = new Set([
  'application/zip',
  'application/x-zip-compressed',
  'application/octet-stream',
  '',
]);

export function isAcceptedPluginFile(file: File): boolean {
  if (!ZIP_MIME_TYPES.has(file.type)) return false;
  return file.name.toLowerCase().endsWith('.zip');
}

/** Returns null when the file can be submitted, else a reason key suffix. */
export function validatePluginZip(file: File): string | null {
  if (!isAcceptedPluginFile(file)) return 'notZip';
  if (file.size > MAX_PLUGIN_ZIP_BYTES) return 'tooLarge';
  return null;
}

/**
 * Returns null when the URL can be submitted, else a reason. Reuses the MCP
 * remote-URL SSRF ladder; the backend additionally normalizes forge repo
 * pages (github/gitlab/codeberg/bitbucket) to their archive endpoints, so a
 * plain repository URL is fine here.
 */
export function validatePluginSourceUrl(raw: string): string | null {
  return validateRemoteUrl(raw.trim());
}
