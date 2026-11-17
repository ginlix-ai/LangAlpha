/**
 * Reads a Bash call's `command` as the `la` (alias `langalpha`) invocation it
 * is, so the step can be named for what it did ("Deleted thread") instead of
 * by the model's own description. Exact or nothing: the command must be one
 * plain invocation, with no shell syntax that could change what runs, and its
 * words must parse under the CLI's own grammar and command table
 * (`laGrammar.generated`, written from the CLI). Anything else is null, and
 * the step keeps the generic rendering it always had.
 */
import {
  bind, LA_COMMANDS, LA_GLOBAL_FLAGS, parse, select, workspaceRef, type Parsed, type WorkspaceRef,
} from './laGrammar.generated';

export type LaStepKind =
  | 'listWorkspaces' | 'createWorkspace' | 'archiveComputer' | 'deleteWorkspace'
  | 'listThreads' | 'showThread' | 'readThread' | 'startThread' | 'sendToThread'
  | 'followThread' | 'retryThread' | 'cancelThread' | 'archiveThread' | 'deleteThread'
  | 'listComputers' | 'listModels' | 'listAutomations';

/** What the step acted on, as far as the command itself says. */
export type LaTarget =
  | { type: 'workspace'; ref: WorkspaceRef }
  | { type: 'thread'; id: string };

export interface LaStep {
  kind: LaStepKind;
  target: LaTarget | null;
  /** A new workspace's description, as the command gave it. */
  description?: string;
}

export interface LaStepLabel {
  kind: LaStepKind;
  /** Which operand or flag names the step's object. */
  target?: { type: 'workspace' | 'thread' | 'workspaceName'; from: string };
}

/** The commands an agent runs to manage its workspaces and threads, by their
 *  words, and what each step reads as. A command left out renders generically. */
export const LA_STEP_LABELS: Readonly<Record<string, LaStepLabel>> = {
  'workspaces list': { kind: 'listWorkspaces' },
  'workspaces create': { kind: 'createWorkspace', target: { type: 'workspaceName', from: 'name' } },
  // The CLI archives the stopped workspace's computer, and every workspace on it.
  'workspaces archive': { kind: 'archiveComputer', target: { type: 'workspace', from: 'ws' } },
  'workspaces delete': { kind: 'deleteWorkspace', target: { type: 'workspace', from: 'ws' } },
  'threads list': { kind: 'listThreads', target: { type: 'workspace', from: 'workspace' } },
  'threads show': { kind: 'showThread', target: { type: 'thread', from: 'id' } },
  'threads output': { kind: 'readThread', target: { type: 'thread', from: 'id' } },
  'threads new': { kind: 'startThread', target: { type: 'workspace', from: 'workspace' } },
  'threads send': { kind: 'sendToThread', target: { type: 'thread', from: 'id' } },
  'threads follow': { kind: 'followThread', target: { type: 'thread', from: 'id' } },
  'threads retry': { kind: 'retryThread', target: { type: 'thread', from: 'id' } },
  'threads cancel': { kind: 'cancelThread', target: { type: 'thread', from: 'id' } },
  'threads archive': { kind: 'archiveThread', target: { type: 'thread', from: 'id' } },
  'threads delete': { kind: 'deleteThread', target: { type: 'thread', from: 'id' } },
  'computers list': { kind: 'listComputers' },
  'models list': { kind: 'listModels' },
  'automations list': { kind: 'listAutomations' },
};

/** The binary by name, or by a plain path to it. */
const PROGRAM = /^(?:[A-Za-z0-9._/-]*\/)?(?:la|langalpha)$/;

/**
 * Splits a command into words under POSIX quoting, or null when anything in
 * it is shell syntax rather than a plain word: an operator, a redirection, an
 * expansion, a glob, a comment, or a command on another line.
 */
export function shellWords(command: string): string[] | null {
  const words: string[] = [];
  let word = '';
  let inWord = false;
  let i = 0;
  const n = command.length;
  while (i < n) {
    const c = command[i]!;
    if (c === ' ' || c === '\t') {
      if (inWord) words.push(word);
      word = '';
      inWord = false;
      i++;
      continue;
    }
    if (c === '\\') {
      if (i + 1 >= n) return null;
      const next = command[i + 1]!;
      i += 2;
      // A backslash before a newline joins the lines.
      if (next === '\n') continue;
      word += next;
      inWord = true;
      continue;
    }
    if (c === "'") {
      const end = command.indexOf("'", i + 1);
      if (end === -1) return null;
      word += command.slice(i + 1, end);
      inWord = true;
      i = end + 1;
      continue;
    }
    if (c === '"') {
      i++;
      let closed = false;
      while (i < n) {
        const d = command[i]!;
        if (d === '"') { closed = true; i++; break; }
        if (d === '$' || d === '`') return null;
        if (d === '\\' && i + 1 < n) {
          const next = command[i + 1]!;
          if (next === '\n') { i += 2; continue; }
          if (next === '"' || next === '\\' || next === '$' || next === '`') {
            word += next;
            i += 2;
            continue;
          }
        }
        word += d;
        i++;
      }
      if (!closed) return null;
      inWord = true;
      continue;
    }
    // A comment starts only at a word's start; a tilde expands there and
    // after an `=`, so it is refused anywhere.
    if (c === '#' && !inWord) return null;
    if ('\n\r|&;<>()$`*?[]{}!~'.includes(c)) return null;
    word += c;
    inWord = true;
    i++;
  }
  if (inWord) words.push(word);
  return words;
}

/** The `la` step a Bash command runs, or null when it is anything else. */
export function parseLaCommand(command: string): LaStep | null {
  const words = shellWords(command.trim());
  if (!words || words.length === 0 || !PROGRAM.test(words[0]!)) return null;
  try {
    const { command: spec, rest } = select(LA_COMMANDS, LA_GLOBAL_FLAGS, words.slice(1));
    const label = spec && LA_STEP_LABELS[spec.words.join(' ')];
    if (!spec || !label) return null;
    const parsed = parse(rest, { ...LA_GLOBAL_FLAGS, ...spec.flags });
    // Help and the version print and exit; the command itself never runs.
    if (parsed.values.has('help') || parsed.values.has('version')) return null;
    const step: LaStep = { kind: label.kind, target: targetOf(label, bind(spec, parsed), parsed.values) };
    const description = label.kind === 'createWorkspace' ? parsed.values.get('description') : undefined;
    return typeof description === 'string' && description.trim() ? { ...step, description: description.trim() } : step;
  } catch {
    // The CLI would refuse the call.
    return null;
  }
}

function targetOf(label: LaStepLabel, operands: Map<string, string>, values: Parsed['values']): LaTarget | null {
  if (!label.target) return null;
  const raw = operands.get(label.target.from) ?? values.get(label.target.from);
  if (typeof raw !== 'string' || raw === '') return null;
  if (label.target.type === 'thread') return { type: 'thread', id: raw };
  // A new workspace's name is only ever a name, whatever it looks like.
  if (label.target.type === 'workspaceName') return { type: 'workspace', ref: { name: raw } };
  return { type: 'workspace', ref: workspaceRef(raw) };
}

/** A tool call's `la` step: only a Bash call whose `command` is one. */
/**
 * Whether a Bash call's result is a command that failed. Bash's result always
 * carries an artifact, so the stream never marks one failed; its `ERROR:`
 * envelope, on every nonzero exit, is what says so.
 */
export function bashFailed(content: unknown): boolean {
  return typeof content === 'string' && /^error:/i.test(content.trimStart());
}

export function laStepOf(toolName: string | undefined, args: Readonly<Record<string, unknown>> | null | undefined): LaStep | null {
  if (toolName !== 'Bash' || typeof args?.command !== 'string') return null;
  return parseLaCommand(args.command);
}

/** An object a step named by id. */
export interface LaLookup {
  type: 'workspace' | 'thread';
  id: string;
}

/** The id a step's object is looked up by, when the command named it by id. */
export function laLookupId(step: LaStep | null): LaLookup | null {
  const target = step?.target;
  if (!target) return null;
  if (target.type === 'thread') return { type: 'thread', id: target.id };
  return 'id' in target.ref ? { type: 'workspace', id: target.ref.id } : null;
}
