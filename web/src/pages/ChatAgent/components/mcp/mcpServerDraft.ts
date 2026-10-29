import { commandLine, parseEntry, shellSplit, suggestName } from './mcpEntry';
import { isPlaceholder, placeholderArgIndexes } from './mcpImportPlaceholders';
import {
  headersToMap,
  kvsToMap,
  mapToHeaderRows,
  mapToKVs,
  nextRowId,
  savedHeaderRows,
  sentHeader,
  type HeaderRow,
  type KvRow,
} from './McpKeyValueEditor';
import { EXPOSURE_MODES, collectVaultRefs, validateRemoteUrl } from './mcpSchemas';
import type { McpServerDraft, McpServerInput, McpTransport } from '../../utils/api';

/**
 * The add/edit form's whole state, and the pure transitions that move it.
 *
 * One typed value, four shapes. The form used to keep the transport, the URL,
 * the command, the args, the env rows and the header rows as six independent
 * pieces of state that every input had to write coherently, and a malformed
 * JSON paste wrote only some of them: the field showed the broken text while
 * the payload still held the config before it, and Save sent that. A shape
 * that holds nothing submittable is what makes that state unreachable rather
 * than merely unlikely.
 *
 * Everything the form sends is DERIVED from here -- the payload, the suggested
 * name, the validation target, the probe's address -- so there is no second
 * copy to keep in step.
 */

type Exposure = (typeof EXPOSURE_MODES)[number];

/** What a pasted config filled in, kept so the form can say so and adopt the
 *  fields that live outside the draft. */
export interface FilledFrom {
  /** The key the config used, quoted in the note under the field. */
  from: string;
  /** How many more servers that config held. */
  more: number;
  /** The legal name coerced from that key, offered as this server's name. */
  name: string;
  /** The prompt-tuning fields it carried, absent where it carried none. */
  meta: Partial<Pick<DraftMeta, 'description' | 'instruction' | 'exposure'>>;
}

/**
 * `entry` is the raw text of the one field, stored rather than derived: the
 * command line a user types has to survive the space between two words, and a
 * round trip through argv would eat it as they typed.
 *
 * Every shape holds both row sets; the payload sends the ones its kind can
 * send, headers for a remote server and env for a local one, so flipping the
 * transport still cannot smuggle headers into a stdio payload. Holding them
 * everywhere is what makes a passing reclassification harmless: `h` on the way
 * to `https://…` reads as a command, and a shape that dropped the header rows
 * there would lose the credential the user had already pasted in.
 */
export type Draft =
  | { kind: 'empty'; entry: string; headers: HeaderRow[]; env: KvRow[] }
  | {
      kind: 'remote';
      entry: string;
      transport: 'http' | 'sse';
      /** A transport chosen under Advanced holds against what the field looks
       *  like. Only sse needs it: http is what any address reads as anyway. */
      transportPinned: boolean;
      headers: HeaderRow[];
      env: KvRow[];
      filled?: FilledFrom;
    }
  | { kind: 'stdio'; entry: string; headers: HeaderRow[]; env: KvRow[]; filled?: FilledFrom }
  | { kind: 'invalid'; entry: string; note: string; headers: HeaderRow[]; env: KvRow[] };

/** The form fields that are not part of the server's address. */
export interface DraftMeta {
  name: string;
  description: string;
  instruction: string;
  exposure: Exposure;
  discoveryUsesSecrets: boolean;
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

/**
 * Hydrate from the row being edited. Prefer the stored env/header reference
 * maps (real keys + `${vault:NAME}` ref or literal values) so an unrelated
 * edit re-saves the existing config intact: a PUT replaces the whole config,
 * and the refs-only fallback can only recover the value, not the name it sat
 * under, so it seeds blank keys the user has to relabel before the form will
 * save.
 */
function hydrateEnv(map: Record<string, string> | undefined, refs: string[]): KvRow[] {
  if (map && Object.keys(map).length > 0) return mapToKVs(map);
  return (refs ?? []).map((name) => ({ id: nextRowId(), key: '', value: `\${vault:${name}}` }));
}

function hydrateHeaders(map: Record<string, string> | undefined, refs: string[]): HeaderRow[] {
  if (map && Object.keys(map).length > 0) return savedHeaderRows(map);
  return (refs ?? []).map((name) => ({
    id: nextRowId(),
    choice: 'custom' as const,
    customName: '',
    inner: `\${vault:${name}}`,
  }));
}

export function initialDraft(initial?: McpServerDraft | null): Draft {
  if (!initial) return { kind: 'empty', entry: '', headers: [], env: [] };
  if (initial.transport === 'stdio') {
    return {
      kind: 'stdio',
      entry: commandLine(initial.command, initial.args ?? []),
      headers: [],
      env: hydrateEnv(initial.env, initial.env_refs),
    };
  }
  return {
    kind: 'remote',
    entry: initial.url ?? '',
    transport: initial.transport === 'sse' ? 'sse' : 'http',
    // An sse server stays sse through an edit of its address. Without the pin
    // the first keystroke in the field re-detected it as http and quietly
    // changed the transport of a server the user only meant to re-point.
    transportPinned: initial.transport === 'sse',
    headers: hydrateHeaders(initial.headers, initial.header_refs),
    env: [],
  };
}

// ---------------------------------------------------------------------------
// Transitions
// ---------------------------------------------------------------------------

/** The field is the source: a URL makes a remote server, a command line a
 *  local one, a pasted JSON config fills everything, and a broken one fills
 *  nothing. */
export function entryChanged(draft: Draft, raw: string): Draft {
  const parsed = parseEntry(raw);
  if (parsed.kind === 'empty') {
    return { kind: 'empty', entry: raw, headers: draft.headers, env: draft.env };
  }
  if (parsed.kind === 'json-error' || parsed.kind === 'command-error') {
    return {
      kind: 'invalid',
      entry: raw,
      note: parsed.error,
      headers: draft.headers,
      env: draft.env,
    };
  }
  if (parsed.kind === 'json') {
    const s = parsed.server;
    const filled: FilledFrom = {
      from: s.originalName,
      more: parsed.more,
      name: s.name,
      meta: {
        ...(s.description ? { description: s.description } : {}),
        ...(s.instruction ? { instruction: s.instruction } : {}),
        exposure: s.toolExposureMode,
      },
    };
    // The field shows the line the config amounts to, never the JSON. A pasted
    // config is the whole definition, so its rows replace the ones in hand.
    if (s.transport === 'stdio') {
      return {
        kind: 'stdio',
        entry: commandLine(s.command, s.args),
        headers: mapToHeaderRows(s.headers),
        env: mapToKVs(s.env),
        filled,
      };
    }
    return {
      kind: 'remote',
      entry: s.url,
      transport: s.transport,
      transportPinned: false,
      headers: mapToHeaderRows(s.headers),
      env: mapToKVs(s.env),
      filled,
    };
  }
  if (parsed.kind === 'remote') {
    const keepsSse =
      draft.kind === 'remote' && draft.transportPinned && draft.transport === 'sse';
    return {
      kind: 'remote',
      entry: raw,
      transport: keepsSse ? 'sse' : 'http',
      transportPinned: keepsSse,
      headers: draft.headers,
      env: draft.env,
    };
  }
  return { kind: 'stdio', entry: raw, headers: draft.headers, env: draft.env };
}

/** Advanced overrides what the field was read as. The text is left alone: an
 *  address held as a command is still the line the user typed. */
export function transportPinned(draft: Draft, next: McpTransport): Draft {
  if (next === 'stdio') {
    return { kind: 'stdio', entry: draft.entry, headers: draft.headers, env: draft.env };
  }
  return {
    kind: 'remote',
    entry: draft.entry,
    transport: next,
    transportPinned: true,
    headers: draft.headers,
    env: draft.env,
    // A pasted config's name rides along: "Use HTTP" on a refused SSE entry
    // changes how it connects, not what the user called it.
    filled: draft.kind === 'remote' ? draft.filled : undefined,
  };
}

export function headersChanged(draft: Draft, headers: HeaderRow[]): Draft {
  return { ...draft, headers };
}

export function envChanged(draft: Draft, env: KvRow[]): Draft {
  return { ...draft, env };
}

/** Editing the argument list rewrites the field, which is where args live. */
export function argsChanged(draft: Draft, args: string[]): Draft {
  if (draft.kind !== 'stdio') return draft;
  return { ...draft, entry: commandLine(draftArgv(draft).command, args) };
}

// ---------------------------------------------------------------------------
// Derivation
// ---------------------------------------------------------------------------

export function draftTransport(draft: Draft): McpTransport | null {
  if (draft.kind === 'remote') return draft.transport;
  if (draft.kind === 'stdio') return 'stdio';
  return null;
}

/** The command and its arguments, read off the field the way a shell would. */
export function draftArgv(draft: Draft): { command: string; args: string[] } {
  if (draft.kind !== 'stdio') return { command: '', args: [] };
  const [command = '', ...args] = shellSplit(draft.entry);
  return { command, args };
}

export function draftUrl(draft: Draft): string {
  return draft.kind === 'remote' ? draft.entry.trim() : '';
}

export function draftHeaderMap(draft: Draft): Record<string, string> {
  return draft.kind === 'remote' ? headersToMap(draft.headers) : {};
}

/**
 * An authenticated remote server needs its header even to list tools, so
 * discovery must resolve secrets; the toggle is forced on for it (the backend
 * enforces the same, this just keeps the UI honest).
 */
export function discoverySecretsForced(draft: Draft): boolean {
  return draft.kind === 'remote' && collectVaultRefs(draftHeaderMap(draft)).length > 0;
}

/** The name to propose while the user has not typed one. */
export function draftSuggestedName(draft: Draft): string {
  if (draft.kind === 'remote' || draft.kind === 'stdio') {
    if (draft.filled) return draft.filled.name;
  }
  const { command, args } = draftArgv(draft);
  return suggestName(draftTransport(draft), draftUrl(draft), command, args);
}

/**
 * The first free spelling of a suggested name: `brave_search` while it is
 * free, then `brave_search_2`, `_3`... cut to fit the 64-character limit.
 * The backend refuses a taken name only after the whole check has run, so the
 * form settles it while the name is still a suggestion.
 */
export function freeName(base: string, taken: { has(name: string): boolean }): string {
  if (!base || !taken.has(base)) return base;
  for (let n = 2; n < 1000; n += 1) {
    const suffix = `_${n}`;
    const candidate = `${base.slice(0, 64 - suffix.length)}${suffix}`;
    if (!taken.has(candidate)) return candidate;
  }
  return base;
}

/** Whether an edited local command changed what is run, which is what makes
 *  the save start it again. A description edit saves without a check. */
export function commandChanged(body: McpServerInput | null, initial: McpServerDraft | null | undefined): boolean {
  if (!body || !initial) return true;
  const same = (a: unknown, b: unknown) => JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
  return (
    body.transport !== initial.transport ||
    !same(body.command, initial.command) ||
    !same(body.args ?? [], initial.args ?? []) ||
    !same(body.env ?? {}, initial.env ?? {})
  );
}

/**
 * The address the pre-save check runs against, or null when there is nothing
 * checkable: a local command has no host-side check, and an address the URL
 * policy already refuses is not worth a round trip. Nor is SSE, which every
 * save's check refuses: the check dials streamable HTTP, so the 405 an SSE
 * server answers it with is the transport talking, not a second fault.
 */
export function probeTarget(draft: Draft): { url: string; headers: Record<string, string> } | null {
  const url = draftUrl(draft);
  if (draft.kind !== 'remote' || draft.transport === 'sse' || validateRemoteUrl(url) !== null) return null;
  return { url, headers: draftHeaderMap(draft) };
}

/** Why the address policy refuses a remote address, by what the user changes. */
export type RemoteUrlProblem = 'scheme' | 'local' | 'userinfo' | 'secret' | 'malformed';

/**
 * Which fix a refused address needs, read in `validateRemoteUrl`'s own order
 * and only once it has refused, so this can never refuse what the policy (and
 * the backend's `validate_remote_url`, which it mirrors) would let through. An
 * `http://` address that https alone would not rescue is reported for what
 * still blocks it: told to switch the scheme, the user of `http://localhost`
 * would only meet the next refusal.
 */
export function remoteUrlProblem(url: string): RemoteUrlProblem | null {
  if (!url || validateRemoteUrl(url) === null) return null;
  if (url.includes('${')) return 'secret';
  let parsed: URL;
  try {
    parsed = new URL(url);
  } catch {
    return 'malformed';
  }
  if (parsed.protocol !== 'https:') {
    if (parsed.protocol !== 'http:') return 'scheme';
    return remoteUrlProblem(`https:${url.slice('http:'.length)}`) ?? 'scheme';
  }
  if (parsed.username || parsed.password) return 'userinfo';
  return 'local';
}

/**
 * The row that would be silently dropped on save: `kvsToMap` and `headersToMap`
 * discard a blank key, so the schema never sees it. The refs-only hydration
 * seeds exactly that shape, and without this guard saving an edited legacy row
 * erased every entry it could not name.
 */
export function draftBlankKeyPath(draft: Draft): 'headers' | 'env' | null {
  if (draft.kind === 'remote') {
    const blank = draft.headers.some(
      (row) => row.choice === 'custom' && !row.customName.trim() && row.inner.trim(),
    );
    return blank ? 'headers' : null;
  }
  if (draft.kind === 'stdio') {
    return draft.env.some((row) => !row.key.trim() && row.value.trim()) ? 'env' : null;
  }
  return null;
}

/**
 * The row another row would silently overwrite: both maps are last-wins, so two
 * filled rows sharing a name fold into one entry before `validateMcpServer`
 * ever sees them, and Save drops the credential on the losing row without
 * saying so. Header names fold case-insensitively because the wire reads them
 * that way; env names are case-sensitive and `Path` is not `PATH`.
 */
export function draftDuplicateKeyPath(draft: Draft): 'headers' | 'env' | null {
  const repeats = (names: string[]) => new Set(names).size < names.length;
  if (draft.kind === 'remote') {
    // Read off what each row sends, since a saved row sends even an empty value.
    const names = draft.headers.flatMap((row) => {
      const sent = sentHeader(row);
      return sent ? [sent.key.toLowerCase()] : [];
    });
    return repeats(names) ? 'headers' : null;
  }
  if (draft.kind === 'stdio') {
    // `kvsToMap` keeps an empty value on purpose, so a blank-valued second row
    // still overwrites the one before it.
    const keys = draft.env.filter((row) => row.key.trim()).map((row) => row.key.trim());
    return repeats(keys) ? 'env' : null;
  }
  return null;
}

/**
 * The values still holding a vendor's stand-in, `<your-api-key>` or
 * `YOUR_TOKEN`, read with the same rule Import asks about. A pasted config is
 * the usual source, and saved as written the stand-in became the credential:
 * the save's check then failed against it, or a local command started with it
 * and failed later. Only the rows this draft would send are read.
 */
export interface DraftStandIns {
  headers: Set<string>;
  env: Set<string>;
  args: Set<number>;
}

/**
 * `saved` is the edited connector as it was opened. A value it already holds
 * is the user's, whatever it reads like, so it never blocks the save: a stand-in
 * rule that misread a stored value would otherwise lock even a description edit.
 */
export function draftStandIns(draft: Draft, saved: Draft | null = null): DraftStandIns {
  const out: DraftStandIns = { headers: new Set(), env: new Set(), args: new Set() };
  const keptHeaders = new Set(saved?.headers.map((row) => row.inner));
  const keptEnv = new Set(saved?.env.map((row) => row.value));
  const keptArgs = new Set(saved ? draftArgv(saved).args : []);
  if (draft.kind === 'remote') {
    for (const row of draft.headers) {
      if (!keptHeaders.has(row.inner) && isPlaceholder(row.inner)) out.headers.add(row.id);
    }
  }
  if (draft.kind === 'stdio') {
    for (const row of draft.env) {
      if (!keptEnv.has(row.value) && isPlaceholder(row.value)) out.env.add(row.id);
    }
    const { args } = draftArgv(draft);
    for (const i of placeholderArgIndexes(args)) if (!keptArgs.has(args[i])) out.args.add(i);
  }
  return out;
}

export function hasStandIns(s: DraftStandIns): boolean {
  return s.headers.size + s.env.size + s.args.size > 0;
}

/** What Save sends, or null while the field holds nothing a server can be
 *  made of. There is no other payload: a shape with no server in it has none. */
export function draftPayload(draft: Draft, meta: DraftMeta): McpServerInput | null {
  const base = {
    name: meta.name.trim(),
    description: meta.description,
    instruction: meta.instruction,
    tool_exposure_mode: meta.exposure,
    discovery_uses_secrets: meta.discoveryUsesSecrets || discoverySecretsForced(draft),
  };
  if (draft.kind === 'remote') {
    return { ...base, transport: draft.transport, url: draftUrl(draft), headers: draftHeaderMap(draft) };
  }
  if (draft.kind === 'stdio') {
    const { command, args } = draftArgv(draft);
    return { ...base, transport: 'stdio', command, args, env: kvsToMap(draft.env) };
  }
  return null;
}

// ---------------------------------------------------------------------------
// The form
// ---------------------------------------------------------------------------

/**
 * The draft with what the form holds beside it: the prompt fields, and where
 * the automatic check may send a key. One value moved by one reducer, so a
 * server the form opens on goes in exactly as a paste of it does.
 */
export interface FormState {
  draft: Draft;
  meta: Omit<DraftMeta, 'name'>;
  /**
   * The addresses a key may go to without the user saying so again: ones
   * committed with a key already in the form, ones that asked for a key
   * themselves, and an edit's saved address, since that pairing exists.
   */
  trusted: readonly string[];
  /** Whether the headers are finished for now. Typing a key unsettles them;
   *  leaving its field, choosing a stored key or keeping a typed one settles
   *  them, and that is when a trusted address is asked again. */
  headersSettled: boolean;
}

export type FormAction =
  | { type: 'entry'; raw: string }
  | { type: 'headers'; rows: HeaderRow[] }
  /** Any other change to the draft: env, args, a transport chosen. */
  | { type: 'draft'; change: (draft: Draft) => Draft }
  | { type: 'meta'; meta: Partial<FormState['meta']> }
  | { type: 'settle' }
  /** The address field left, or Enter pressed in it. */
  | { type: 'commit' }
  /** The user, or the server's own answer about a key, vouches for this address. */
  | { type: 'trust'; url: string };

function trusting(trusted: readonly string[], url: string): readonly string[] {
  return trusted.includes(url) ? trusted : [...trusted, url];
}

export function formReducer(state: FormState, action: FormAction): FormState {
  switch (action.type) {
    case 'entry': {
      const draft = entryChanged(state.draft, action.raw);
      const filled = draft.kind === 'remote' || draft.kind === 'stdio' ? draft.filled : undefined;
      if (!filled) return { ...state, draft };
      // A pasted config also carries the fields that live outside the draft,
      // and its address arrives whole with its key beside it: nothing about
      // it is half-typed, so it is checked as it stands.
      const remote = draft.kind === 'remote';
      return {
        draft,
        meta: { ...state.meta, ...filled.meta },
        trusted: remote ? trusting(state.trusted, draftUrl(draft)) : state.trusted,
        headersSettled: state.headersSettled || remote,
      };
    }
    case 'headers':
      return { ...state, draft: headersChanged(state.draft, action.rows), headersSettled: false };
    case 'draft':
      return { ...state, draft: action.change(state.draft) };
    case 'meta':
      return { ...state, meta: { ...state.meta, ...action.meta } };
    case 'settle':
      return { ...state, headersSettled: true };
    case 'commit': {
      const target = probeTarget(state.draft);
      if (!target) return state;
      // A commit made before any credential existed is not consent to send
      // one: clicking "Add header" is itself what leaves the address field,
      // so the host still being typed is committed a beat before the key.
      const keyed = Object.keys(target.headers).length > 0;
      return {
        ...state,
        trusted: keyed ? trusting(state.trusted, target.url) : state.trusted,
        headersSettled: true,
      };
    }
    case 'trust':
      return { ...state, trusted: trusting(state.trusted, action.url) };
  }
}

/** A server written out elsewhere, as the one-server config a paste of it is. */
function asConfig(server: McpServerInput): string {
  const def: Record<string, unknown> = { type: server.transport };
  if (server.transport === 'stdio') {
    def.command = server.command ?? '';
    def.args = server.args ?? [];
    if (server.env && Object.keys(server.env).length > 0) def.env = server.env;
  } else {
    def.url = server.url ?? '';
    if (server.headers && Object.keys(server.headers).length > 0) def.headers = server.headers;
  }
  if (server.description) def.description = server.description;
  if (server.instruction) def.instruction = server.instruction;
  if (server.tool_exposure_mode) def.tool_exposure_mode = server.tool_exposure_mode;
  return JSON.stringify({ mcpServers: { [server.name]: def } });
}

/**
 * The form as it opens: on the row being edited, or on a new server somebody
 * already wrote out (an import entry being fixed, a save being tried again),
 * which goes in as a paste, so it takes the same reading and the form says
 * where it came from.
 */
export function initialForm(initial?: McpServerDraft | null, prefill?: McpServerInput | null): FormState {
  const opened: FormState = {
    draft: initialDraft(initial),
    meta: {
      description: initial?.description ?? '',
      instruction: initial?.instruction ?? '',
      exposure: EXPOSURE_MODES.find((m) => m === initial?.tool_exposure_mode) ?? 'summary',
      discoveryUsesSecrets: initial?.discovery_uses_secrets ?? false,
    },
    trusted: initial?.url ? [initial.url] : [],
    headersSettled: true,
  };
  return !initial && prefill ? formReducer(opened, { type: 'entry', raw: asConfig(prefill) }) : opened;
}

/** Where a refusal is said: the one place on the form that fixes it. */
export type RefusalSpot = 'probe' | 'sse' | 'entry' | 'name' | 'headers' | 'env' | 'box';

const SPOTS = ['probe', 'sse', 'entry', 'name', 'headers', 'env'] as const;

/**
 * The first place on the form that answers for a refusal. The live check is
 * the save's own check asked again of exactly this form, so while it answers,
 * or once it has refused, its panel says the refusal in the form's words.
 * Otherwise a problem the form can see for itself is the place, with the
 * refusal's own words as a detail under it, and only a refusal the form cannot
 * place stands on its own.
 */
export function placeRefusal(
  faults: Partial<Record<Exclude<RefusalSpot, 'box'>, boolean>>,
): RefusalSpot {
  return SPOTS.find((spot) => faults[spot]) ?? 'box';
}
