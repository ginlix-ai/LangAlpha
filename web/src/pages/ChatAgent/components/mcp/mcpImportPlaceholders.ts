import { mapImportServers, type ParsedImportServer } from './mcpImport';

/**
 * Placeholder credentials in a pasted config.
 *
 * Vendor docs hand out configs with `<your-api-key>` where the key goes, and
 * the same spelling appears above every server in the file. This finds each
 * one, asks for it, and substitutes the answer back into the raw payload before
 * it is sent.
 *
 * Asked once per server, not once per spelling: two vendors both documenting
 * `<your-api-key>` are two credentials, and collecting one of them for both
 * would send the first vendor's key to the second the moment its row was
 * switched on.
 */

export interface PlaceholderUse {
  server: string;
  where: 'headers' | 'env' | 'args';
  key: string;
}

export interface Placeholder {
  /** Identity of the one input that fills this: the server and the spelling. */
  id: string;
  /** The parsed server name, as the rest of the modal shows it. */
  server: string;
  literal: string;
  uses: PlaceholderUse[];
}

const ANGLE = /^<[^<>]{1,80}>$/;
// The same bracketed span, unanchored and global: `ANGLE` asks whether a value
// is nothing but one, this finds each one wherever it sits inside a value.
const ANGLE_SEGMENT = /<[^<>]{1,80}>/g;
// Hint words that read as a stand-in wherever they sit in the value:
// `your-api-key`, `REPLACE_ME`, `paste-token-here`.
const HINT_WORDS = /\b(your[-_ ]?|replace[-_ ]?me|change[-_ ]?me|todo|placeholder|insert[-_ ]|paste[-_ ])/i;
// Words that only read as a stand-in when they are the whole value. `xxx` is a
// blank; `sk-live-xxxx9f8e` is a key. `example` is a blank; a real token or a
// hostname that merely contains it is not.
const HINT_ALONE = /^(x{3,}|example)$/i;
const VAULT_REF = /^\$\{vault:[^}]+\}$/;

// Mirrors `looks_like_placeholder` in `src/ptc_agent/core/mcp_sanitize.py`, the
// rule the backend import uses to keep a stand-in out of its cross-server value
// dedupe. The vault-ref arm above has no counterpart there only because its
// callers have already excluded refs by the time they ask.
/** Whether a config value reads as a stand-in rather than a credential. */
export function isPlaceholder(value: string): boolean {
  const v = value.trim();
  if (!v || VAULT_REF.test(v)) return false;
  if (ANGLE.test(v)) return true;
  return v.length <= 64 && (HINT_ALONE.test(v) || HINT_WORDS.test(v));
}

/**
 * The key of the one input that fills this literal for this server, named by
 * the key the config itself wrote. The coerced name is a display name and is
 * not unique -- `acme-api` and `acme_api` both coerce to `acme_api` -- so
 * keying on it merged two vendors into a single input.
 */
export function placeholderId(configName: string, literal: string): string {
  return `${configName}\u0000${literal}`;
}

// Mirrors `_SECRET_KEY_RE` in `src/ptc_agent/core/mcp_sanitize.py`, the rule the
// backend import uses to pick which argv value it vaults. Key signal only, and
// deliberately so: the element after a flag is as often a package name, a port
// or a path, so `todo-mcp-server` after `-y` must not read as a stand-in. Only
// the flag naming the value is narrow enough to act on. Read a second time
// below against a bare positional, where it says the opposite thing: nothing
// there can be vaulted, so a positional that names a credential is the one
// shape not to ask about.
const SECRET_FLAG =
  /(secret|token|password|passwd|pwd|apikey|api[_-]?key|access[_-]?key|authorization|auth|bearer|credential|cred|private[_-]?key|\bpat\b|\bkey\b)/i;

/** The value half of `--flag=value` when a secret-looking flag names it. */
function secretFlagValue(arg: string): string | null {
  if (!arg.startsWith('-')) return null;
  const eq = arg.indexOf('=');
  if (eq === -1) return null;
  const value = arg.slice(eq + 1);
  return value && SECRET_FLAG.test(arg.slice(0, eq)) ? value : null;
}

/**
 * Index to credential value for the two argv shapes the backend vaults:
 * `--token VALUE` and `--token=VALUE`. A value that is itself a flag ends the
 * pair rather than filling it, the same way `iter_arg_flag_pairs` reads it.
 */
function secretArgValues(args: string[]): Map<number, string> {
  const found = new Map<number, string>();
  let prevFlag = '';
  args.forEach((arg, i) => {
    if (arg.startsWith('-')) {
      const paired = secretFlagValue(arg);
      if (paired !== null) found.set(i, paired);
      prevFlag = arg.includes('=') ? '' : arg;
      return;
    }
    if (prevFlag && SECRET_FLAG.test(prevFlag)) found.set(i, arg);
    prevFlag = '';
  });
  return found;
}

/** Every placeholder in the parsed servers, one entry per server that wrote it. */
export function findPlaceholders(servers: ParsedImportServer[]): Placeholder[] {
  const byId = new Map<string, Placeholder>();
  const note = (s: ParsedImportServer, literal: string, use: PlaceholderUse) => {
    const id = placeholderId(s.originalName, literal);
    const found = byId.get(id);
    if (found) found.uses.push(use);
    else byId.set(id, { id, server: s.name, literal, uses: [use] });
  };
  for (const s of servers) {
    if (s.error) continue;
    for (const [key, value] of Object.entries(s.headers)) {
      if (isPlaceholder(value)) note(s, value, { server: s.name, where: 'headers', key });
    }
    for (const [key, value] of Object.entries(s.env)) {
      if (isPlaceholder(value)) note(s, value, { server: s.name, where: 'env', key });
    }
    // An argv value earns the full stand-in predicate only where the backend
    // would vault it, so `REPLACE_ME` after `--token` is caught before the
    // import stores the placeholder itself as the credential. Every other
    // element is read for the angle spelling alone, which no real argument has,
    // and only while that spelling does not itself name a credential: no flag
    // names a bare positional, so the backend vaults nothing there, and asking
    // for `<your-api-key>` under copy that promises the vault would write the
    // typed key into `args` in plaintext. `<region>` and `<path>` are still
    // asked for; `<your-api-key>` imports exactly as written, and the user
    // moves it into the vault by editing the server once it is in.
    const secrets = secretArgValues(s.args);
    s.args.forEach((value, i) => {
      const secret = secrets.get(i);
      const literal = value.trim();
      const standIn =
        secret === undefined
          ? ANGLE.test(literal) && !SECRET_FLAG.test(literal)
          : isPlaceholder(secret);
      // The literal is what the substitution below matches, so for `--token=X`
      // it is the value half, not the whole element.
      if (standIn) note(s, secret ?? value, { server: s.name, where: 'args', key: `#${i + 1}` });
    });
  }
  return [...byId.values()];
}

/**
 * What the field is asking for, named by where the value goes: `fuyao_meta ·
 * X-api-key`. Deliberately not the literal, which is the config's own text and
 * sits unmasked beside a masked input, so a real token that read as a stand-in
 * would be printed in full.
 */
export function placeholderLabel(p: Placeholder): string {
  const keys = [...new Set(p.uses.map((u) => u.key))];
  return `${p.server} · ${keys.join(', ')}`;
}

/**
 * The typed value written into a collected literal.
 *
 * A vendor documents the whole value it wants sent, scheme included -- `Bearer
 * <your-api-key>` -- so the part the user supplies is the bracketed span, not
 * the field. Swapping the whole value sent `Authorization: <typed key>` without
 * the scheme, which every such server rejects. Each bracketed span is filled
 * and the text around it survives; a literal carrying none (`REPLACE_ME`,
 * `paste-token-here`) is the stand-in itself and is still replaced entire.
 */
function fillLiteral(literal: string, value: string): string {
  let matched = false;
  const out = literal.replace(ANGLE_SEGMENT, (_span, at: number) => {
    matched = true;
    // The field never shows the wrapper, so a user who pastes the value as
    // their vendor prints it, scheme and all, would send `Bearer Bearer ...`.
    // A typed value that repeats the text before the span drops that repeat.
    const lead = literal.slice(0, at).trimStart();
    return lead && value.toLowerCase().startsWith(lead.toLowerCase())
      ? value.slice(lead.length)
      : value;
  });
  return matched ? out : value;
}

/**
 * The raw payload with each filled placeholder replaced inside the server that
 * asked for it. Keys are never touched, an unfilled placeholder is left as it
 * was, and a spelling two servers share is replaced in each only by that
 * server's own answer.
 *
 * Only the three credential containers are filled, the same ones
 * `findPlaceholders` reads: a literal that happens to be the whole of some
 * other field, a description say, stays a placeholder rather than becoming a
 * plaintext copy of the secret in a field the import never sends to the vault.
 */
export function substitutePlaceholders(
  payload: unknown,
  values: Record<string, string>,
  servers: ParsedImportServer[],
): unknown {
  const filled = new Map(Object.entries(values).filter(([, v]) => v.trim() !== ''));
  if (filled.size === 0) return payload;
  // Inputs and payload are keyed alike, by the name written above the
  // definition, so two servers whose names coerce alike stay two servers.
  const known = new Set(servers.map((s) => s.originalName));
  return mapImportServers(payload, (rawName, def) => {
    if (!known.has(rawName) || !def || typeof def !== 'object' || Array.isArray(def)) return def;
    const fill = (node: unknown): unknown => {
      if (typeof node !== 'string') return node;
      const value = filled.get(placeholderId(rawName, node));
      return value === undefined ? node : fillLiteral(node, value);
    };
    const fillMap = (node: unknown): unknown =>
      node && typeof node === 'object' && !Array.isArray(node)
        ? Object.fromEntries(
            Object.entries(node as Record<string, unknown>).map(([k, v]) => [k, fill(v)]),
          )
        : node;
    const source = def as Record<string, unknown>;
    const out: Record<string, unknown> = { ...source };
    if ('headers' in source) out.headers = fillMap(source.headers);
    if ('env' in source) out.env = fillMap(source.env);
    // Matched by value inside the array, not by the recorded index: the index
    // counts parsed (string) args, so a non-string entry would shift it. An
    // `--token=VALUE` element is filled in its value half only, since dropping
    // the flag would rewrite the command rather than answer its credential.
    const fillArg = (node: unknown): unknown => {
      if (typeof node !== 'string') return node;
      const whole = filled.get(placeholderId(rawName, node));
      if (whole !== undefined) return fillLiteral(node, whole);
      const value = secretFlagValue(node);
      if (value === null) return node;
      const half = filled.get(placeholderId(rawName, value));
      return half === undefined ? node : `${node.slice(0, node.length - value.length)}${half}`;
    };
    if (Array.isArray(source.args)) out.args = source.args.map(fillArg);
    return out;
  });
}
