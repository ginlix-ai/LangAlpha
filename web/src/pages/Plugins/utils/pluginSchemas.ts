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

/**
 * The literal a declared key pattern opens with (`^acme_[A-Za-z0-9]{24}$`
 * gives `acme_`), or null when it opens with anything a prefix cannot say.
 * "Starts with acme_" is something a user can check against the key in front
 * of them; a regular expression is not.
 */
export function patternPrefix(pattern: string | null): string | null {
  if (!pattern?.startsWith('^') || pattern.includes('|')) return null;
  let prefix = '';
  for (let i = 1; i < pattern.length; i += 1) {
    const ch = pattern[i];
    if (ch === '\\') {
      const next = pattern[i + 1];
      // \d, \w and the like are classes, not the letter.
      if (next === undefined || /[A-Za-z0-9]/.test(next)) break;
      prefix += next;
      i += 1;
      continue;
    }
    if (/[A-Za-z0-9_:-]/.test(ch)) {
      prefix += ch;
      continue;
    }
    // A quantifier makes the character before it optional or repeated.
    if ('?*{'.includes(ch)) prefix = prefix.slice(0, -1);
    break;
  }
  return prefix.length >= 2 ? prefix : null;
}

export type KeyMismatch =
  | { kind: 'prefix'; prefix: string }
  /** Lengths count the whole value, prefix included; `max` null is open. */
  | { kind: 'length'; actual: number; min: number; max: number | null }
  | { kind: 'characters' }
  | { kind: 'format' };

/** `^`, a literal, one character class repeated, `$`: the shape nearly every
 *  key format takes, and one whose length can be read off it. Groups: the
 *  literal, the class, then `{n}` / `{n,}` / `{n,m}` or `+` / `*`. */
const KEY_SHAPE =
  /^\^((?:[A-Za-z0-9_:-]|\\[^A-Za-z0-9])*)(\[(?:\\.|[^\]\\])+\]|\\[dDwWsS]|\.)(?:\{(\d+)(?:(,)(\d*))?\}|([+*]))\$$/;

/**
 * What is off about a value its declared pattern refuses, in terms a user can
 * check against the key in front of them: the prefix it should open with,
 * its length, or a character the format does not allow. A pattern of any
 * other shape only says the format is off. Once the value has the prefix,
 * the prefix is not mentioned: pointing at the one part that is right sends
 * the user looking in the wrong place.
 *
 * Null when nothing is off, and when there is nothing to judge by: no value
 * yet, no pattern, or a pattern this engine cannot read, since a declaration
 * that does not compile here says nothing either way about the key.
 */
export function keyMismatch(value: string, pattern: string | null | undefined): KeyMismatch | null {
  if (!value || !pattern) return null;
  try {
    if (new RegExp(pattern).test(value)) return null;
  } catch {
    return null;
  }
  const prefix = patternPrefix(pattern);
  if (prefix && !value.startsWith(prefix)) return { kind: 'prefix', prefix };
  const shape = KEY_SHAPE.exec(pattern);
  if (!shape) return { kind: 'format' };
  const [, escaped, , low, comma, high, repeat] = shape;
  const literal = escaped.replace(/\\(.)/g, '$1');
  if (!value.startsWith(literal)) return { kind: 'format' };
  const fewest = repeat ? (repeat === '+' ? 1 : 0) : Number(low);
  const most = repeat || (comma && !high) ? null : comma ? Number(high) : fewest;
  const min = literal.length + fewest;
  const max = most === null ? null : literal.length + most;
  const actual = value.length;
  if (actual < min || (max !== null && actual > max)) {
    return { kind: 'length', actual, min, max };
  }
  return { kind: 'characters' };
}
