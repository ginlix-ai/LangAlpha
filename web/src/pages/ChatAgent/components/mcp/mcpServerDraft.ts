import { commandLine, parseEntry, shellSplit, suggestName } from './mcpEntry';
import {
  headerRowName,
  headersToMap,
  kvsToMap,
  mapToHeaderRows,
  mapToKVs,
  nextRowId,
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
  if (map && Object.keys(map).length > 0) return mapToHeaderRows(map);
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
 * The address the pre-save check runs against, or null when there is nothing
 * checkable: a local command has no host-side check, and an address the URL
 * policy already refuses is not worth a round trip.
 */
export function probeTarget(draft: Draft): { url: string; headers: Record<string, string> } | null {
  const url = draftUrl(draft);
  if (draft.kind !== 'remote' || validateRemoteUrl(url) !== null) return null;
  return { url, headers: draftHeaderMap(draft) };
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
    // `headersToMap` skips a row with no value, and a scheme word is only
    // written in front of a value that exists, so an empty `inner` is exactly
    // the row it drops.
    const names = draft.headers
      .filter((row) => headerRowName(row).trim() && row.inner !== '')
      .map((row) => headerRowName(row).trim().toLowerCase());
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
