import { NAME_RE, TRANSPORTS, EXPOSURE_MODES } from './mcpSchemas';

/**
 * Client-side parser for the de-facto-standard `mcpServers` JSON blob (Claude
 * Desktop / Cursor / etc.), mirroring `parse_mcp_servers_payload` in
 * `src/server/models/mcp_server.py`. The backend re-parses authoritatively on
 * bulk import; this powers the add-modal "paste-to-fill" and the import-modal
 * count preview so users get instant feedback.
 *
 * Shape accepted: `{ "mcpServers": { "<name>": { ... } } }`, a bare
 * `{ name: def }` map, or a single self-naming server object.
 */

type Transport = (typeof TRANSPORTS)[number];
type Exposure = (typeof EXPOSURE_MODES)[number];

export interface ParsedImportServer {
  originalName: string;
  name: string;
  renamed: boolean;
  transport: Transport;
  command: string;
  args: string[];
  url: string;
  env: Record<string, string>;
  headers: Record<string, string>;
  description: string;
  instruction: string;
  toolExposureMode: Exposure;
  /** Set when the entry can't be normalized (uncoercible name / no transport). */
  error?: string;
}

export interface ParseResult {
  servers: ParsedImportServer[];
  /** Top-level failure (not valid JSON, or no servers found). */
  error?: string;
}

const TRANSPORT_ALIASES: Record<string, Transport> = {
  stdio: 'stdio',
  http: 'http',
  streamablehttp: 'http',
  streamable: 'http',
  sse: 'sse',
};

/** Coerce an arbitrary server key into a legal MCP name (mirrors backend). */
export function coerceMcpName(raw: string): { name: string | null; renamed: boolean } {
  if (!raw || typeof raw !== 'string') return { name: null, renamed: false };
  let cand = raw.replace(/[^0-9A-Za-z_]/g, '_');
  if (/^[0-9]/.test(cand)) cand = `_${cand}`;
  cand = cand.slice(0, 64);
  if (!cand || !NAME_RE.test(cand)) return { name: null, renamed: false };
  return { name: cand, renamed: cand !== raw };
}

/** Map a standard-config `type`/`transport` value to our transport enum. */
export function normalizeTransport(
  raw: unknown,
  hasCommand: boolean,
  hasUrl: boolean,
): Transport | null {
  if (typeof raw === 'string' && raw.trim()) {
    const key = raw.toLowerCase().replace(/[^a-z]/g, '');
    return TRANSPORT_ALIASES[key] ?? null;
  }
  if (hasCommand && !hasUrl) return 'stdio';
  if (hasUrl && !hasCommand) return 'http';
  return null;
}

function asStringMap(v: unknown): Record<string, string> {
  if (!v || typeof v !== 'object' || Array.isArray(v)) return {};
  const out: Record<string, string> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
    if (typeof val === 'string') out[k] = val;
  }
  return out;
}

function normalizeEntry(rawName: string, body: unknown): ParsedImportServer {
  const blank: ParsedImportServer = {
    originalName: rawName,
    name: rawName,
    renamed: false,
    transport: 'stdio',
    command: '',
    args: [],
    url: '',
    env: {},
    headers: {},
    description: '',
    instruction: '',
    toolExposureMode: 'summary',
  };

  const { name, renamed } = coerceMcpName(rawName);
  if (name === null) {
    return { ...blank, error: 'name could not be normalized to a valid identifier' };
  }
  if (!body || typeof body !== 'object' || Array.isArray(body)) {
    return { ...blank, name, renamed, error: 'server definition must be a JSON object' };
  }

  const def = body as Record<string, unknown>;
  const rawType = def.type ?? def.transport ?? def.transportType;
  const transport = normalizeTransport(rawType, !!def.command, !!def.url);
  if (transport === null) {
    const hint = rawType ? ` (type=${JSON.stringify(rawType)})` : '';
    return { ...blank, name, renamed, error: `could not determine transport${hint}` };
  }

  const exposure = EXPOSURE_MODES.includes(def.tool_exposure_mode as Exposure)
    ? (def.tool_exposure_mode as Exposure)
    : 'summary';

  return {
    ...blank,
    name,
    renamed,
    transport,
    command: transport === 'stdio' && typeof def.command === 'string' ? def.command : '',
    args:
      transport === 'stdio' && Array.isArray(def.args)
        ? def.args.filter((a): a is string => typeof a === 'string')
        : [],
    url: transport !== 'stdio' && typeof def.url === 'string' ? def.url : '',
    env: transport === 'stdio' ? asStringMap(def.env) : {},
    headers: transport !== 'stdio' ? asStringMap(def.headers) : {},
    description: typeof def.description === 'string' ? def.description : '',
    instruction: typeof def.instruction === 'string' ? def.instruction : '',
    toolExposureMode: exposure,
  };
}

// The three shapes a config can be written in, read once so the parse below and
// `mapImportServers` can never disagree about which object is which server.
const SERVERS_MAP_KEYS = ['mcpServers', 'mcp_servers', 'servers'];
const SELF_NAMING_FIELDS = ['command', 'url', 'type', 'transport', 'args', 'headers', 'env'];

function selfNaming(obj: Record<string, unknown>): boolean {
  return typeof obj.name === 'string' && SELF_NAMING_FIELDS.some((k) => k in obj);
}

/** Find the `{ name: def }` map inside a parsed config object. */
function unwrapServersMap(payload: unknown): Record<string, unknown> {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return {};
  const obj = payload as Record<string, unknown>;
  for (const key of SERVERS_MAP_KEYS) {
    const inner = obj[key];
    if (inner && typeof inner === 'object' && !Array.isArray(inner)) {
      return inner as Record<string, unknown>;
    }
  }
  if (selfNaming(obj)) return { [obj.name as string]: obj };
  return obj;
}

/**
 * A copy of the payload with `fn` applied to each raw server definition, left
 * in the shape the config was written in. The caller edits one server's values
 * without having to know which of the three shapes it is looking at, and the
 * payload keeps every field this parser does not model.
 */
export function mapImportServers(
  payload: unknown,
  fn: (rawName: string, def: unknown) => unknown,
): unknown {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return payload;
  const obj = payload as Record<string, unknown>;
  const mapEach = (map: Record<string, unknown>) =>
    Object.fromEntries(Object.entries(map).map(([k, v]) => [k, fn(k, v)]));
  for (const key of SERVERS_MAP_KEYS) {
    const inner = obj[key];
    if (inner && typeof inner === 'object' && !Array.isArray(inner)) {
      return { ...obj, [key]: mapEach(inner as Record<string, unknown>) };
    }
  }
  if (selfNaming(obj)) return fn(obj.name as string, obj);
  return mapEach(obj);
}

/** Normalize an already-parsed JSON object into server entries. */
export function normalizeMcpServers(payload: unknown): ParseResult {
  const map = unwrapServersMap(payload);
  const servers = Object.entries(map).map(([k, v]) => normalizeEntry(k, v));
  if (servers.length === 0) {
    return { servers: [], error: 'No MCP servers found. Expected {"mcpServers": { … }}.' };
  }
  return { servers };
}

/** Parse raw JSON text from a textarea into normalized server entries. */
export function parseMcpServersJson(text: string): ParseResult {
  const trimmed = (text || '').trim();
  if (!trimmed) return { servers: [], error: 'Paste a JSON config first.' };
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return { servers: [], error: 'Not valid JSON.' };
  }
  return normalizeMcpServers(parsed);
}
