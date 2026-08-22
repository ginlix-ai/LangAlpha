/**
 * Client mirror of the plugin install source policy. The backend stays
 * authoritative (services/plugins/fetch.py + install.py); this exists so the
 * wizard can refuse an install that is certain to fail before uploading.
 */
import { validateRemoteUrl } from '@/pages/ChatAgent/components/mcp/mcpSchemas';

/** Mirrors MAX_PACKAGE_BYTES (services/plugins/fetch.py). */
export const MAX_PLUGIN_ZIP_BYTES = 64 * 1024 * 1024;

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
 * The remote-URL policy's refusals, restated as reason key suffixes. The
 * policy stays `validateRemoteUrl`'s to decide; what it returns is an English
 * sentence written for the MCP server form, and an install dialog must not
 * speak another surface's vocabulary or another user's language. Prefixes,
 * because two of the host refusals differ only in their tail.
 */
const URL_REASON_KEYS: ReadonlyArray<readonly [string, string]> = [
  ['url is required', 'urlRequired'],
  ['url must not contain secrets', 'hasPlaceholder'],
  ['url must use https', 'notHttps'],
  ['url must not contain userinfo', 'hasUserinfo'],
  ['url must include a host', 'badHost'],
  ['url host', 'badHost'],
];

/**
 * Returns null when the URL can be submitted, else a reason key suffix (as
 * `validatePluginZip` does, so both branches of the source step translate the
 * same way). Reuses the MCP remote-URL SSRF ladder; the backend additionally
 * normalizes forge repo pages (github/gitlab/codeberg/bitbucket) to their
 * archive endpoints, so a plain repository URL is fine here.
 */
export function validatePluginSourceUrl(raw: string): string | null {
  const reason = validateRemoteUrl(raw.trim());
  if (!reason) return null;
  const matched = URL_REASON_KEYS.find(([prefix]) => reason.startsWith(prefix));
  // An unrecognized refusal is still a refusal: fall back to the generic key
  // rather than leaking the policy's own sentence into this dialog.
  return matched ? matched[1] : 'badUrl';
}
