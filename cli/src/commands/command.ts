import { createInterface } from "node:readline";
import { Args, type Flag, type Flags, type Parsed, parse } from "../args.js";
import { CliError, Exit, usage } from "../errors.js";
import type { Api, Query } from "../http.js";
import { print } from "../output.js";

export type Row = Record<string, unknown>;

export const GLOBAL = {
  server: { type: "string", value: "URL", help: "the LangAlpha server (default: the one you signed in to)" },
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

export interface Ctx<F extends string = string, O extends string = string> {
  api: Api;
  args: Args<F | GlobalFlag, O>;
  json: boolean;
}

type OperandName<S> = S extends `${infer N}...` ? N : S;

/** A command, declared once: the parser accepts exactly what it declares and
 *  the help lists exactly that, so the two cannot drift apart. */
export interface Command<O extends readonly string[] = readonly string[], F extends Flags = Flags> {
  words: readonly string[];
  summary: string;
  /** All required, in order; a last one ending in ``...`` takes the rest of
   *  the words, joined, so a message needs no quotes. */
  operands?: O;
  flags?: F;
  run(ctx: Ctx<Extract<keyof F, string>, OperandName<O[number]>>): Promise<number | void>;
}

/** Infers the operand and flag names, so ``run`` is typed by its own spec. */
export function command<const O extends readonly string[] = [], F extends Flags = {}>(c: Command<O, F>): Command<O, F> {
  return c;
}

export const WORKSPACE: Flag = { type: "string", short: "w", value: "WS", help: "a workspace id, its exact name, or home" };
export const MODEL: Flag = { type: "string", short: "m", value: "MODEL", help: "the model to run the turn on" };
export const YES: Flag = { type: "bool", short: "y", help: "skip the confirmation" };

export interface Selection {
  command?: Command;
  /** Everything after the leading global options. */
  words: readonly string[];
  /** What is left to parse against the command's flags and GLOBAL. */
  rest: readonly string[];
}

/** Find the command ``argv`` names. Its words come first; only global
 *  options may precede them. */
export function select(commands: readonly Command[], argv: readonly string[]): Selection {
  let i = 0;
  while (i < argv.length && argv[i]!.startsWith("-") && argv[i] !== "--") i += takesValue(argv[i]!) ? 2 : 1;
  const words = argv.slice(i);
  const command = commands.find((c) => c.words.every((w, k) => words[k] === w));
  const rest = command ? words.slice(command.words.length) : [];
  return { command, words, rest: [...argv.slice(0, i), ...rest] };
}

export function parseFor(command: Command | undefined, argv: readonly string[]): Parsed {
  return parse(argv, { ...GLOBAL, ...command?.flags });
}

function takesValue(arg: string): boolean {
  const name = arg.slice(2);
  return arg.startsWith("--") && Object.hasOwn(GLOBAL, name) && (GLOBAL as Flags)[name]!.type !== "bool";
}

export function bind(command: Command, parsed: Parsed): Args {
  const specs = command.operands ?? [];
  const operands = new Map<string, string>();
  specs.forEach((spec, i) => {
    const rest = spec.endsWith("...");
    const name = rest ? spec.slice(0, -3) : spec;
    const value = rest ? parsed.operands.slice(i).join(" ").trim() : parsed.operands[i];
    if (!value) throw usage(`Missing <${name}>`);
    operands.set(name, value);
  });
  const extra = parsed.operands[specs.length];
  if (extra !== undefined && !specs.at(-1)?.endsWith("...")) throw usage(`Unexpected argument "${extra}"`);
  for (const [name, flag] of Object.entries(command.flags ?? {})) {
    if (flag.required && !parsed.values.has(name)) throw usage(`Missing ${flagName(name, flag)}`);
  }
  return new Args(parsed.values, operands);
}

function flagName(name: string, flag: Flag): string {
  const text = flag.short ? `-${flag.short}` : `--${name}`;
  return flag.type === "bool" ? text : `${text} ${flag.value ?? "VALUE"}`;
}

export function synopsis(c: Command): string {
  const parts = [...c.words, ...(c.operands ?? []).map((o) => `<${o}>`)];
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
