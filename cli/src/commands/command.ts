import { createInterface } from "node:readline";
import { Args, asUsage, type Flag, type Flags, type Parsed, parse } from "../args.js";
import { CliError, Exit, usage } from "../errors.js";
import { bind as bindOperands, flagName, operandSpec, type Selection as GrammarSelection, select as selectIn } from "../grammar.js";
import type { Api, Query } from "../http.js";
import { print } from "../output.js";

export type Row = Record<string, unknown>;

export const GLOBAL = {
  server: { type: "string", value: "URL", help: "the LangAlpha server (else LANGALPHA_URL, the one you signed in to, or this build's default)" },
  token: {
    type: "string",
    value: "T",
    help: "a token for this call (else LANGALPHA_TOKEN, LANGALPHA_TOKEN_FILE, your sign-in)",
  },
  json: { type: "bool", help: "machine-readable output; turn streams print one JSON event per line" },
  help: { type: "bool", short: "h", help: "show the commands, or one command's options" },
  version: { type: "bool", help: "print the version" },
} satisfies Flags;

export type GlobalFlag = keyof typeof GLOBAL;

export interface Ctx<F extends string = string, O extends string = string, P extends string = string> {
  api: Api;
  args: Args<F | GlobalFlag, O, P>;
  json: boolean;
}

type RequiredOperand<S> = S extends `${string}?` ? never : S extends `${infer N}...` ? N : S;
type OptionalOperand<S> = S extends `${infer N}...?` ? N : S extends `${infer N}?` ? N : never;

/** A command, declared once: the parser accepts exactly what it declares and
 *  the help lists exactly that, so the two cannot drift apart. */
export interface Command<O extends readonly string[] = readonly string[], F extends Flags = Flags> {
  words: readonly string[];
  summary: string;
  /** All required, in order, but a last one ending in ``?``, which may be
   *  left out; a last one ending in ``...`` takes the rest of the words,
   *  joined, so a message needs no quotes (``...?`` may also be left out). */
  operands?: O;
  flags?: F;
  /** Rewrites the parsed words before they are bound, for a command whose operands
   *  may be named in another form than the one it takes. */
  prepare?(parsed: Parsed): Parsed;
  run(ctx: Ctx<Extract<keyof F, string>, RequiredOperand<O[number]>, OptionalOperand<O[number]>>): Promise<number | void>;
}

/** Infers the operand and flag names, so ``run`` is typed by its own spec. */
export function command<const O extends readonly string[] = [], F extends Flags = {}>(c: Command<O, F>): Command<O, F> {
  const optional = (c.operands ?? []).findIndex((o) => operandSpec(o).optional);
  if (optional !== -1 && optional !== c.operands!.length - 1) {
    throw new Error(`${c.words.join(" ")}: optional operand <${c.operands![optional]}> must be last`);
  }
  return c;
}

export const WORKSPACE: Flag = { type: "string", short: "w", value: "WS", help: "a workspace id, its exact name, or home" };
export const MODEL: Flag = { type: "string", short: "m", value: "MODEL", help: "the model to run the turn on" };
export const YES: Flag = { type: "bool", short: "y", help: "skip the confirmation" };

export type Selection = GrammarSelection<Command>;

/** Find the command ``argv`` names. Its words come first; only global
 *  options may precede them. */
export function select(commands: readonly Command[], argv: readonly string[]): Selection {
  return selectIn(commands, GLOBAL, argv);
}

export function parseFor(command: Command | undefined, argv: readonly string[]): Parsed {
  return parse(argv, { ...GLOBAL, ...command?.flags });
}

export function bind(command: Command, parsed: Parsed): Args {
  const prepared = command.prepare?.(parsed) ?? parsed;
  return new Args(prepared.values, asUsage(() => bindOperands(command, prepared)));
}

export function synopsis(c: Command): string {
  const parts = [...c.words, ...(c.operands ?? []).map((o) => {
      const { name, rest, optional } = operandSpec(o);
      const shown = name + (rest ? "..." : "");
      return optional ? `[${shown}]` : `<${shown}>`;
    })];
  for (const [name, flag] of Object.entries(c.flags ?? {})) {
    parts.push(flag.required ? flagName(name, flag) : `[${flagName(name, flag)}]`);
  }
  return parts.join(" ");
}

export function flagList(flags: Flags): string {
  const rows = Object.entries(flags).map(([name, f]) => [
    `${f.short ? `-${f.short}, ` : ""}--${name}${f.type === "bool" ? "" : ` ${f.value ?? "VALUE"}`}`,
    f.help ?? "",
  ]);
  const width = Math.max(...rows.map(([left]) => left!.length));
  return rows.map(([left, help]) => `  ${left!.padEnd(width)}   ${help}`.trimEnd()).join("\n");
}

/** At a terminal, ask; with no one there, a delete must say ``--yes``. */
async function confirm(args: Args, warning: string): Promise<void> {
  if (args.has("yes")) return;
  if (!process.stdin.isTTY) throw usage(`${warning}: pass --yes to confirm`);
  const rl = createInterface({ input: process.stdin, output: process.stderr });
  const answer = await new Promise<string>((resolve) => rl.question(`${warning}. Continue? [y/N] `, resolve));
  rl.close();
  if (!/^y(es)?$/i.test(answer.trim())) throw new CliError("Cancelled", Exit.error);
}

export interface Removal {
  path: string;
  /** The removed thing's id, under its field name: ``{thread_id: id}``. */
  key: Record<string, string>;
  warning: string;
  done: string;
  query?: Query;
}

export async function remove(ctx: Ctx, { path, key, warning, done, query }: Removal): Promise<void> {
  await confirm(ctx.args, warning);
  await ctx.api.call("DELETE", path, { query });
  print(ctx.json, { ...key, deleted: true }, () => done);
}
