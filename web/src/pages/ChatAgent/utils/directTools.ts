/**
 * Direct MCP tools reach the agent as JSON tools named `mcp__<server>__<tool>`,
 * where `<server>` is the user's MCP server row (a brokerage such as `moomoo`).
 * Everything the chat needs to present one lives here: the name split, the
 * argument summary with account ids masked, and the result envelope, which is
 * a JSON string of MCP content blocks whose text is usually JSON again.
 */

import { humanizeKey } from './structuredResult';

export const DIRECT_TOOL_PREFIX = 'mcp__';

/**
 * A tool name reduced to what two spellings of it have in common. Mirrors the
 * server's `fold_tool_name` (NFKC normalize, strip, casefold) and must stay in
 * step with it: the server reads a per-tool override by this key and refuses
 * a map holding two keys that fold together, so any client write against the
 * override map has to match its spellings the same way. `toLowerCase` stands
 * in for Unicode casefold; the two agree on every name a server has sent.
 */
export function foldToolName(name: string): string {
  return name.normalize('NFKC').trim().toLowerCase();
}

export interface DirectToolName {
  server: string;
  tool: string;
}

export function isDirectToolName(name: string | null | undefined): name is string {
  return typeof name === 'string' && name.startsWith(DIRECT_TOOL_PREFIX) && name.length > DIRECT_TOOL_PREFIX.length;
}

export function parseDirectToolName(name: string | null | undefined): DirectToolName | null {
  if (!isDirectToolName(name)) return null;
  const rest = name.slice(DIRECT_TOOL_PREFIX.length);
  const sep = rest.indexOf('__');
  if (sep <= 0 || sep === rest.length - 2) return { server: '', tool: rest.replace(/^__/, '') };
  return { server: rest.slice(0, sep), tool: rest.slice(sep + 2) };
}

/**
 * The vendor's own server and tool, preferring the stamp the backend puts on
 * the result over the tool name.
 *
 * The name is derived from the pair and gives way to a digest when it cannot
 * hold both, so parsing it back is lossy by construction. The stamp only
 * arrives with the result, so a call still in flight has the name and nothing
 * else, which is why the parse stays as the fallback rather than being removed.
 */
export function directToolIdentity(
  name: string | null | undefined,
  artifact?: unknown,
): DirectToolName | null {
  const stamp = (artifact as { direct_mcp?: unknown } | null | undefined)?.direct_mcp;
  if (stamp && typeof stamp === 'object') {
    const { server, tool } = stamp as { server?: unknown; tool?: unknown };
    if (typeof server === 'string' && typeof tool === 'string' && server && tool) {
      return { server, tool };
    }
  }
  return parseDirectToolName(name);
}

/** The display name for a direct tool, or null for any other tool. */
export function directToolDisplayName(
  name: string | null | undefined,
  artifact?: unknown,
): string | null {
  const parsed = directToolIdentity(name, artifact);
  return parsed ? humanizeKey(parsed.tool) : null;
}

// Every spelling a vendor has used for "which account": account_id, acc_id and
// acct_id, the number forms (account_number, rhs_account_number, acc_no), the
// bare `account`, and each of those in camelCase. One expression rather than a
// list, because the surfaces that mask this field all read it from here and a
// spelling missing from one of them is the whole account on screen.
const ACCOUNT_KEY = /(^|_)acc(oun)?t?(_?(id|no|num|number))?$/i;

export function isAccountIdKey(key: string): boolean {
  return ACCOUNT_KEY.test(key);
}

const MASK = '••••';

export function maskAccountId(value: unknown): string {
  const s = String(value ?? '');
  if (s.length <= 4) return MASK;
  return `${MASK}${s.slice(-4)}`;
}

/**
 * The masked form of one field, or null when the field is not an account id to
 * mask.
 *
 * Only a scalar is an id. `account` is one of the spellings, and a key by that
 * name can hold the whole account record, which `maskAccountId` would reduce
 * to four dots and the tail of "[object Object]", losing it rather than
 * protecting it. A record is masked field by field instead.
 */
export function maskedAccountValue(key: string, value: unknown): string | null {
  if (!isAccountIdKey(key)) return null;
  if (typeof value === 'number') return maskAccountId(value);
  return typeof value === 'string' && value !== '' ? maskAccountId(value) : null;
}

/** Mask every account id in `value`, at any depth, before it is serialized. */
export function maskAccountIdsDeep(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(maskAccountIdsDeep);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>).map(([k, v]) => [
        k,
        maskedAccountValue(k, v) ?? maskAccountIdsDeep(v),
      ]),
    );
  }
  return value;
}

function shortValue(value: unknown): string {
  if (value == null) return String(value);
  if (typeof value === 'string') return value;
  if (typeof value === 'number' || typeof value === 'boolean') return String(value);
  try {
    const s = JSON.stringify(maskAccountIdsDeep(value));
    return s.length > 40 ? `${s.slice(0, 37)}...` : s;
  } catch {
    return String(value);
  }
}

/**
 * One line of `key value` pairs for the collapsed row. Account ids are masked,
 * as they are everywhere the chat draws a direct call: the id is worth nothing
 * to the person reading their own screen and everything to anyone looking over
 * their shoulder.
 */
export function summarizeDirectToolArgs(
  args: Record<string, unknown> | null | undefined,
  { maxEntries = 4 }: { maxEntries?: number } = {},
): string | null {
  if (!args) return null;
  const entries = Object.entries(args).filter(([, v]) => v !== undefined);
  if (entries.length === 0) return null;
  const parts = entries.slice(0, maxEntries).map(([k, v]) =>
    `${k} ${maskedAccountValue(k, v) ?? shortValue(v)}`,
  );
  const more = entries.length - maxEntries;
  if (more > 0) parts.push(`+${more}`);
  return parts.join(' · ');
}

export interface DirectToolBlock {
  type: string;
  text: string;
  /** The text parsed as JSON, when it is JSON. */
  json?: unknown;
}

export type DirectToolResult =
  | { kind: 'refused'; reason: string }
  | { kind: 'rejected'; reason: string }
  | { kind: 'blocks'; blocks: DirectToolBlock[] }
  | { kind: 'json'; json: unknown }
  | { kind: 'text'; text: string }
  | { kind: 'empty' };

const REFUSED_PREFIX = 'Refused:';
const REJECTED_PREFIX = 'User rejected the tool call';

function tryParseJson(text: string): { ok: true; value: unknown } | { ok: false } {
  const trimmed = text.trim();
  if (!trimmed || !(trimmed.startsWith('{') || trimmed.startsWith('['))) return { ok: false };
  try {
    return { ok: true, value: JSON.parse(trimmed) };
  } catch {
    return { ok: false };
  }
}

/**
 * MCP's own content-block vocabulary, spelled out rather than inferred from the
 * presence of a `type` field.
 *
 * A broker answers most questions with an array of records, and those records
 * have their own `type`: an option is a `call`, an order is a `limit`, a
 * position is `equity`. Read as content blocks they are stripped to `type` and
 * a `text` they never had, and the panel shows a row of empty code fences where
 * the answer was.
 */
const MCP_BLOCK_TYPES = new Set(['text', 'image', 'audio', 'resource', 'resource_link']);

function isContentBlock(v: unknown): v is { type: string; text?: unknown } {
  if (!v || typeof v !== 'object') return false;
  const type = (v as { type?: unknown }).type;
  return typeof type === 'string' && MCP_BLOCK_TYPES.has(type);
}

/**
 * Read a direct tool's result body, and only call it a refusal when the call
 * actually failed.
 *
 * Both refusal prefixes are prose the relay writes, so a vendor whose own
 * successful payload opens with the same words reads identically. On a
 * brokerage surface that is a live answer rendered as a connector notice, so
 * `isFailed: false` short-circuits the prose rules the way the wire status
 * already short-circuits them at ingress. Left undefined the prose still
 * decides, for the callers that have no status to offer.
 */
export function parseDirectToolResult(
  content: unknown,
  isFailed?: boolean,
): DirectToolResult {
  if (content == null) return { kind: 'empty' };
  if (typeof content !== 'string') {
    if (Array.isArray(content) && content.every(isContentBlock)) return blocksFrom(content);
    return { kind: 'json', json: content };
  }
  const text = content.trim();
  if (!text) return { kind: 'empty' };
  if (isFailed !== false) {
    if (text.startsWith(REFUSED_PREFIX)) {
      return { kind: 'refused', reason: text.slice(REFUSED_PREFIX.length).trim() };
    }
    if (text.startsWith(REJECTED_PREFIX)) {
      const m = text.match(/with reason:\s*([\s\S]*)$/);
      return { kind: 'rejected', reason: (m?.[1] ?? '').trim() };
    }
  }
  const parsed = tryParseJson(text);
  if (!parsed.ok) return { kind: 'text', text };
  if (Array.isArray(parsed.value) && parsed.value.length > 0 && parsed.value.every(isContentBlock)) {
    return blocksFrom(parsed.value);
  }
  return { kind: 'json', json: parsed.value };
}

function blocksFrom(raw: Array<{ type: string; text?: unknown }>): DirectToolResult {
  const blocks: DirectToolBlock[] = raw.map((b) => {
    // An image or a resource carries its payload in its own fields and has no
    // `text` at all, so the block itself is what there is to show; dropping to
    // an empty string would render it as a blank fence.
    if (b.text == null) return { type: b.type, text: '', json: b };
    const text = typeof b.text === 'string' ? b.text : JSON.stringify(b.text);
    const inner = tryParseJson(text);
    return inner.ok ? { type: b.type, text, json: inner.value } : { type: b.type, text };
  });
  return { kind: 'blocks', blocks };
}

/** The rejection feedback the user typed, read back out of a reject result. */
export function directToolRejectionReason(reason: string): string {
  const m = reason.match(/^User rejected this action with the following feedback:\s*([\s\S]*)$/);
  return (m?.[1] ?? reason).trim();
}
