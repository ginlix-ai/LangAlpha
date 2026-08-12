import { z } from 'zod';

/**
 * Client-side validation for user-configured MCP servers — a mirror of the
 * backend Pydantic validators in `src/server/models/mcp_server.py` and the
 * SSRF/secret policy in `src/ptc_agent/core/mcp_sanitize.py`. Keep the two in
 * sync: the backend is authoritative (it re-validates everything), but matching
 * here gives instant feedback and avoids round-tripping obviously-bad input.
 *
 * Discriminated on `transport`:
 *   - stdio → command (allowlist, NO `bash`), args, env (no headers/url)
 *   - sse/http → url (https-only, SSRF-hardened), headers (no command/args/env)
 *
 * env/header values are either a single `${vault:NAME}` reference or a plain
 * literal — a bare `${VAR}`/`$VAR` host-env placeholder is rejected (it would
 * never resolve for a workspace server, exactly like the backend).
 */

// ---------------------------------------------------------------------------
// Shared constants — mirror the backend single-source-of-truth values.
// ---------------------------------------------------------------------------

export const NAME_RE = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;
export const ENV_KEY_RE = /^[A-Za-z_][A-Za-z0-9_-]{0,127}$/;

// Allowlist deliberately WITHOUT `bash` (or any shell) — running a user command
// is arbitrary code execution; this bounds it (backend plan §Security #4).
export const ALLOWED_COMMANDS = ['npx', 'uvx', 'uv', 'python', 'python3', 'node'] as const;
export type AllowedCommand = (typeof ALLOWED_COMMANDS)[number];

export const TRANSPORTS = ['stdio', 'sse', 'http'] as const;
export const EXPOSURE_MODES = ['summary', 'detailed'] as const;

export const DESCRIPTION_MAX = 512;
export const INSTRUCTION_MAX = 1024;

// `${vault:NAME}` reference — must be a FULL match for the value to count as a
// reference (mirrors `VAULT_REF_RE.fullmatch` on the backend).
const VAULT_REF_FULL_RE = /^\$\{vault:[A-Za-z_][A-Za-z0-9_]{0,127}\}$/;
// Extract vault names from a value (global, for ref collection / display).
const VAULT_REF_GLOBAL_RE = /\$\{vault:([A-Za-z_][A-Za-z0-9_]{0,127})\}/g;
// A bare host-env placeholder like `${VAR}` or `$VAR` — never resolves.
const BARE_ENV_RE = /\$\{?[A-Za-z_][A-Za-z0-9_]*\}?/;

// ---------------------------------------------------------------------------
// Value-level validation (env + headers)
// ---------------------------------------------------------------------------

/** True iff `value` is a single full `${vault:NAME}` ref OR a clean literal. */
export function isValidSecretValue(value: string): boolean {
  if (VAULT_REF_FULL_RE.test(value)) return true;
  // A malformed vault ref (`${vault:` present but not a full match) is invalid.
  if (value.includes('${vault:')) return false;
  // Any other `${...}` / `$VAR` token is a host-env placeholder → reject.
  if (BARE_ENV_RE.test(value)) return false;
  return true;
}

/**
 * Validate a single stdio `arg` string. Embedded `${vault:NAME}` refs ARE legal
 * here (bulk import writes `--flag=${vault:NAME}`). After stripping every
 * well-formed vault ref, a leftover `${vault:` is a malformed ref, and a leftover
 * bare `${VAR}`/`$VAR` is a host-env placeholder that would never resolve for a
 * workspace server — both rejected (mirrors the backend Pydantic arg validator).
 * Returns null when valid, else the reason string.
 */
export function validateArg(arg: string): string | null {
  const stripped = (arg ?? '').replace(VAULT_REF_GLOBAL_RE, '');
  if (stripped.includes('${vault:')) {
    return 'malformed vault reference; use the exact form ${vault:NAME}';
  }
  if (BARE_ENV_RE.test(stripped)) {
    return 'looks like a host-env placeholder; use ${vault:NAME} for secrets or a plain literal';
  }
  return null;
}

/** Collect the sorted, de-duplicated vault names referenced by a value map. */
export function collectVaultRefs(mapping: Record<string, string> | undefined): string[] {
  const names = new Set<string>();
  for (const value of Object.values(mapping ?? {})) {
    for (const m of (value ?? '').matchAll(VAULT_REF_GLOBAL_RE)) {
      names.add(m[1]);
    }
  }
  return [...names].sort();
}

const secretMapSchema = (kind: 'env' | 'header') =>
  z.record(z.string(), z.string()).superRefine((mapping, ctx) => {
    for (const [key, value] of Object.entries(mapping)) {
      if (!ENV_KEY_RE.test(key)) {
        ctx.addIssue({
          code: 'custom',
          message: `${kind} name "${key}" is invalid`,
          path: [key],
        });
      }
      if (!isValidSecretValue(value)) {
        ctx.addIssue({
          code: 'custom',
          message: value.includes('${vault:')
            ? `malformed vault reference; use the exact form \${vault:NAME}`
            : `looks like a host-env placeholder; use \${vault:NAME} for secrets or a plain literal`,
          path: [key],
        });
      }
    }
  });

// ---------------------------------------------------------------------------
// URL policy (sse/http) — SSRF hardening, mirrors `validate_remote_url`.
// ---------------------------------------------------------------------------

/**
 * Returns null if the URL passes the policy, else a reason string.
 *
 * Policy: https only, no userinfo, no secrets/placeholders, and host must not
 * be localhost / *.local / *.internal / *.localhost nor a literal private,
 * loopback, link-local (incl. 169.254.169.254 metadata), reserved, multicast,
 * or unspecified IP.
 */
export function validateRemoteUrl(raw: string): string | null {
  if (!raw) return 'url is required for sse/http transports';
  // Brace forms only (`${vault:NAME}`, `${VAR}`, unclosed `${`): bare `$word`
  // is a legitimate URL convention (OData `/$batch`, `?$filter=`) and is inert
  // downstream. Mirrors validate_remote_url in models/mcp_server.py.
  if (raw.includes('${')) {
    return 'url must not contain secrets or placeholders; put credentials in headers';
  }

  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return 'url is not a valid URL';
  }

  if (parsed.protocol !== 'https:') return 'url must use https://';
  if (parsed.username || parsed.password) return 'url must not contain userinfo credentials';

  const host = parsed.hostname;
  if (!host) return 'url must include a host';
  const hostL = host.toLowerCase().replace(/\.+$/, '');

  if (
    hostL === 'localhost' ||
    hostL.endsWith('.local') ||
    hostL.endsWith('.internal') ||
    hostL.endsWith('.localhost')
  ) {
    return `url host "${host}" is not allowed`;
  }

  if (isDisallowedIp(hostL.replace(/^\[|\]$/g, ''))) {
    return `url host "${host}" resolves to a disallowed IP range`;
  }
  return null;
}

/**
 * Canonicalize a non-canonical numeric IPv4 host (decimal/hex/octal integer or
 * short-dotted form) to dotted-quad, mirroring the C `inet_aton` the sandbox
 * resolver uses — e.g. "2130706433", "0x7f000001", "0177.0.0.1", "127.1" all →
 * "127.0.0.1". Returns null for anything that isn't a pure numeric IPv4 form
 * (real hostnames, IPv6, malformed input), which then falls through unchanged.
 */
function inetAtonToV4(host: string): string | null {
  if (!/^[0-9a-fA-FxX.]+$/.test(host)) return null;
  const parts = host.split('.');
  if (parts.length === 0 || parts.length > 4) return null;
  const nums: number[] = [];
  for (const p of parts) {
    let n: number;
    if (/^0[xX][0-9a-fA-F]+$/.test(p)) n = parseInt(p, 16);
    else if (/^0[0-7]+$/.test(p)) n = parseInt(p, 8);
    else if (/^[0-9]+$/.test(p)) n = parseInt(p, 10);
    else return null;
    if (!Number.isFinite(n) || n < 0) return null;
    nums.push(n);
  }
  const n = nums.length;
  let value: number;
  if (n === 1) {
    value = nums[0];
  } else {
    // Leading parts are single octets; the final part fills the remaining bytes.
    for (let i = 0; i < n - 1; i++) if (nums[i] > 255) return null;
    if (nums[n - 1] > Math.pow(256, 4 - (n - 1)) - 1) return null;
    value = nums[n - 1];
    for (let i = n - 2; i >= 0; i--) value += nums[i] * Math.pow(256, 3 - i);
  }
  if (value < 0 || value > 0xffffffff) return null;
  return [(value >>> 24) & 255, (value >>> 16) & 255, (value >>> 8) & 255, value & 255].join('.');
}

/** Block loopback / private / link-local / metadata / reserved literal IPs. */
function isDisallowedIp(host: string): boolean {
  // Canonicalize non-canonical numeric forms (integer/hex/octal/short-dotted)
  // the resolver would accept, so e.g. 2130706433 / 0x7f000001 can't slip past.
  const canon = inetAtonToV4(host);
  if (canon) host = canon;
  // IPv4 dotted-quad
  const v4 = host.match(/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/);
  if (v4) {
    const octets = v4.slice(1).map(Number);
    const [a, b] = octets;
    if (octets.some((o) => o > 255)) return true; // malformed → reject
    if (a === 0) return true; // unspecified / "this host"
    if (a === 10) return true; // private
    if (a === 127) return true; // loopback
    if (a === 169 && b === 254) return true; // link-local incl. 169.254.169.254
    if (a === 172 && b >= 16 && b <= 31) return true; // private
    if (a === 192 && b === 168) return true; // private
    if (a === 100 && b >= 64 && b <= 127) return true; // CGNAT (shared)
    if (a >= 224) return true; // multicast (224-239) + reserved/future (240-255)
    return false;
  }
  // IPv6 — block loopback, unspecified, ULA (fc/fd), link-local (fe80::).
  if (host.includes(':')) {
    const h = host.toLowerCase();
    if (h === '::1' || h === '::') return true;
    if (h.startsWith('fc') || h.startsWith('fd')) return true;
    if (h.startsWith('fe80')) return true;
    // IPv4-mapped (::ffff:a.b.c.d) — re-check the embedded v4.
    const mapped = h.match(/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/);
    if (mapped) return isDisallowedIp(mapped[1]);
    return false;
  }
  return false;
}

// ---------------------------------------------------------------------------
// The server-definition schema — discriminated on transport.
// ---------------------------------------------------------------------------

const nameField = z
  .string()
  .regex(NAME_RE, 'name must be 1-64 chars: letter/underscore then letters/digits/underscores');

const descriptionField = z.string().max(DESCRIPTION_MAX).default('');
const instructionField = z.string().max(INSTRUCTION_MAX).default('');
const exposureField = z.enum(EXPOSURE_MODES).default('summary');
// Off (default) = tool discovery runs secret-less. On = resolve vault secrets
// during discovery (for servers that need auth even to list tools).
const discoveryUsesSecretsField = z.boolean().optional().default(false);

const urlField = z.string().superRefine((url, ctx) => {
  const reason = validateRemoteUrl(url);
  if (reason) ctx.addIssue({ code: 'custom', message: reason });
});

// stdio args: literals + embedded `${vault:NAME}` refs are fine; a malformed
// vault ref or a bare host-env placeholder is rejected (mirrors env/header).
const argsField = z
  .array(z.string())
  .default([])
  .superRefine((args, ctx) => {
    args.forEach((arg, i) => {
      const reason = validateArg(arg);
      if (reason) ctx.addIssue({ code: 'custom', message: reason, path: [i] });
    });
  });

const stdioSchema = z.object({
  name: nameField,
  transport: z.literal('stdio'),
  command: z.enum(ALLOWED_COMMANDS, {
    message: `command must be one of: ${ALLOWED_COMMANDS.join(', ')}`,
  }),
  args: argsField,
  env: secretMapSchema('env').default({}),
  description: descriptionField,
  instruction: instructionField,
  tool_exposure_mode: exposureField,
  discovery_uses_secrets: discoveryUsesSecretsField,
});

const sseSchema = z.object({
  name: nameField,
  transport: z.literal('sse'),
  url: urlField,
  headers: secretMapSchema('header').default({}),
  description: descriptionField,
  instruction: instructionField,
  tool_exposure_mode: exposureField,
  discovery_uses_secrets: discoveryUsesSecretsField,
});

const httpSchema = z.object({
  name: nameField,
  transport: z.literal('http'),
  url: urlField,
  headers: secretMapSchema('header').default({}),
  description: descriptionField,
  instruction: instructionField,
  tool_exposure_mode: exposureField,
  discovery_uses_secrets: discoveryUsesSecretsField,
});

// Per-transport schema lookup for the form-level validator (which validates the
// chosen branch directly to keep error paths simple in the modal).
const SCHEMA_BY_TRANSPORT = {
  stdio: stdioSchema,
  sse: sseSchema,
  http: httpSchema,
} as const;

/** Validate a raw form object, returning either ok or the list of errors. */
export function validateMcpServer(input: unknown):
  | { ok: true }
  | { ok: false; errors: Array<{ path: string; message: string }> } {
  const transport = (input as { transport?: keyof typeof SCHEMA_BY_TRANSPORT })?.transport;
  const schema = (transport && SCHEMA_BY_TRANSPORT[transport]) || stdioSchema;
  const result = schema.safeParse(input);
  if (result.success) return { ok: true };
  return {
    ok: false,
    errors: result.error.issues.map((i) => ({
      path: i.path.map(String).join('.'),
      message: i.message,
    })),
  };
}
