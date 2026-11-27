// What every `la drive` command shares: the Drive's answers as types, the
// route prefix, the flags that name a space and a reason, and how a refusal is
// put in the CLI's words.
import type { Flag, Flags } from "../args.js";
import { CliError } from "../errors.js";
import { p } from "../http.js";
import { type Command, command, type Ctx } from "./command.js";
import { type Parsed, operandSpec } from "../grammar.js";

/** A person, or the agent (with its name and thread) that acted for one. */
export interface Who {
  kind: string;
  agent?: string | null;
  thread_id?: string | null;
}

export interface Memo {
  marked: boolean;
  status: "draft" | "final" | null;
  tickers: string[];
  final_seq: number | null;
}

export interface Entry {
  id: string;
  path: string;
  name: string;
  kind: "file" | "folder";
  size: number | null;
  sha256: string | null;
  is_text: boolean | null;
  version: number | null;
  modified_at: string;
  modified_by: Who | null;
  /** Null for a file that never was a memo; `marked: false` for one that was. */
  memo: Memo | null;
}

export interface Version {
  seq: number;
  size: number;
  created_at: string;
  actor: Who;
  note: string | null;
  restored_from: number | null;
  /** The check-out this version was made under; consecutive versions sharing one are a single change. */
  change_id?: string | null;
  /** When this old version goes; null for the current one and one kept without a limit. */
  expires_at?: string | null;
  /** Whether this version's bytes are text; absent from a server that does not say. */
  is_text?: boolean;
}

export const SPACE: Flag = { type: "string", value: "SPACE", help: "the Drive space (default: personal)" };
export const NOTE: Flag = {
  type: "string",
  value: "WHY",
  help: "why you made the change, in one line; kept with the version (a memo is changed after `la drive checkout`)",
};

export const base = (ctx: Pick<Ctx, "api" | "args">) => p`/api/v1/drive/${ctx.args.str("space") ?? "personal"}`;

export async function stat(ctx: Pick<Ctx, "api" | "args">, path: string): Promise<Entry> {
  return ctx.api.json<Entry>("GET", `${base(ctx)}/entry`, { query: { path } });
}

/** The longest note the server takes. */
const NOTE_MAX = 500;

/** A word as a shell reads it back whole, quoted only when it has to be: how a command these print names a path. */
export const sh = (word: string) => (/^[\w@%+=:,./-]+$/.test(word) ? word : `'${word.replace(/'/g, `'\\''`)}'`);

/** The note the caller gave (the `--note` flag, or `checkout` and `checkin`'s reason words), cut to the
 *  server's limit with a line on stderr when it was cut. */
export function noteOf(text: string | undefined): string | undefined {
  if (text === undefined) return undefined;
  const chars = [...text];
  if (chars.length <= NOTE_MAX) return text;
  process.stderr.write(`The note was longer than ${NOTE_MAX} characters; it was cut to fit.\n`);
  return chars.slice(0, NOTE_MAX).join("");
}

/** ``e`` with the sentence ``say`` gives for its refusal kind, keeping its exit code and facts; ``e`` itself when it gives none. */
export function reword(e: unknown, say: (reason: string) => string | undefined): unknown {
  if (!(e instanceof CliError) || !e.reason) return e;
  const message = say(e.reason);
  return message === undefined ? e : new CliError(message, e.code, { status: e.status, detail: e.detail, reason: e.reason });
}

/** A version's note, with where it was restored from. */
export const versionNote = (v: Pick<Version, "note" | "restored_from">) =>
  v.restored_from ? [`restored from v${v.restored_from}`, v.note].filter(Boolean).join(": ") : v.note;

export const when = (iso: string) => iso.slice(0, 16).replace("T", " ");

/** A person, or the agent that wrote it, with its name when the server has it. */
export function by(who: Who | null | undefined): string {
  if (!who) return "-";
  if (who.kind !== "agent") return who.kind;
  return who.agent ? `agent ${who.agent}` : "agent";
}

/** A Drive file as the agent's computer shows it, in the file mount: `.agents/drive/<space>/<path>`,
 *  alone or after the folders of a path that starts as a filesystem one (`/`, `~/`, `./`, `../`).
 *  Any other path holding those folders is a Drive path that has them, so it stays as typed: a
 *  command must not act on another file than the one named. With nothing after the space's slash it
 *  is the space's root. */
const MOUNT_FORM = /^(?:(?:~|\.{0,2})\/(?:.*?\/)?)?\.agents\/drive\/([^/]+)\/(.*)$/;

/** The operands `la drive` commands name their Drive files by. */
const DRIVE_PATHS = ["path", "from", "to"];

/** ``parsed`` with each Drive path in the mount's form as the Drive path it stands for, and the space it
 *  names as `--space` when none was given. A path whose space disagrees with `--space` stays as typed
 *  and the server answers. */
export function fromMount(parsed: Parsed, operands: readonly string[]): Parsed {
  const words = [...parsed.operands];
  let space = parsed.values.get("space");
  operands.forEach((spec, i) => {
    const m = DRIVE_PATHS.includes(operandSpec(spec).name) && words[i] !== undefined ? MOUNT_FORM.exec(words[i]!) : null;
    if (!m || (space !== undefined && space !== m[1])) return;
    space = m[1];
    words[i] = m[2]!;
  });
  return { operands: words, values: space === undefined ? parsed.values : new Map(parsed.values).set("space", space) };
}

/** A list the server answers a page at a time, read on until ``limit`` items or its end: `items` and
 *  the cursor of what remains, null at the end. ``read`` asks for one page of at most ``size``. */
export async function paged<T>(
  limit: number,
  size: number,
  read: (limit: number, cursor?: string) => Promise<{ items: T[]; next?: string | null }>,
): Promise<{ items: T[]; next: string | null }> {
  const items: T[] = [];
  let next: string | null = null;
  do {
    const page = await read(Math.min(limit - items.length, size), next ?? undefined);
    items.push(...page.items);
    next = page.next ?? null;
  } while (next !== null && items.length < limit);
  return { items, next };
}

/** A `la drive` command: ``command``, taking its Drive paths in the mount's form too. */
export function driveCommand<const O extends readonly string[] = [], F extends Flags = {}>(c: Command<O, F>): Command<O, F> {
  return command({ ...c, prepare: (parsed) => fromMount(parsed, c.operands ?? []) });
}
