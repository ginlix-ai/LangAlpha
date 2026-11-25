// How the CLI reads its words, as pure functions over plain spec data: which
// command an argv names, its options and operands, and what a workspace
// reference means. It imports nothing, because the web app reads `la`
// commands by these same rules: scripts/web-grammar.ts copies this file into
// the web app's generated grammar beside the command table.

export interface FlagSpec {
  type: "bool" | "string" | "int";
  short?: string;
  /** The value's placeholder in help: ``--name N``. */
  value?: string;
  required?: boolean;
}

export type FlagSpecs = Readonly<Record<string, FlagSpec>>;

export interface CommandSpec {
  words: readonly string[];
  /** In order; a last one ending in ``...`` takes the rest of the words,
   *  joined, so a message needs no quotes. All are required but a last one
   *  ending in ``?``, which may be left out. */
  operands?: readonly string[];
  flags?: FlagSpecs;
}

export interface Parsed {
  operands: string[];
  values: Map<string, string | number | true>;
}

/** Words that do not read under the grammar. The CLI reports it as a usage error. */
export class GrammarError extends Error {}

/** Only the flags given are accepted, so a typo is refused rather than
 *  taken as a value-less flag that swallows the next word. */
export function parse(argv: readonly string[], flags: FlagSpecs): Parsed {
  const operands: string[] = [];
  const values = new Map<string, string | number | true>();
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!;
    if (arg === "--") {
      operands.push(...argv.slice(i + 1));
      break;
    }
    let name: string;
    let value: string | undefined;
    if (arg.startsWith("--")) {
      const eq = arg.indexOf("=");
      name = eq === -1 ? arg.slice(2) : arg.slice(2, eq);
      value = eq === -1 ? undefined : arg.slice(eq + 1);
      if (!Object.hasOwn(flags, name)) throw new GrammarError(`Unknown option --${name}`);
    } else if (arg.length > 1 && arg.startsWith("-") && !/^-\d/.test(arg)) {
      const short = Object.keys(flags).find((k) => flags[k]!.short === arg.slice(1));
      if (!short) throw new GrammarError(`Unknown option ${arg}`);
      name = short;
    } else {
      operands.push(arg);
      continue;
    }
    const flag = flags[name]!;
    if (flag.type === "bool") {
      if (value !== undefined) throw new GrammarError(`--${name} takes no value`);
      values.set(name, true);
      continue;
    }
    if (value === undefined) {
      value = argv[++i];
      if (value === undefined) throw new GrammarError(`--${name} needs a value`);
    }
    if (flag.type === "int") {
      const n = Number(value);
      if (value.trim() === "" || !Number.isInteger(n)) throw new GrammarError(`--${name} must be a whole number`);
      values.set(name, n);
    } else {
      values.set(name, value);
    }
  }
  return { operands, values };
}

export interface Selection<C extends CommandSpec> {
  command?: C;
  /** Everything after the leading global options. */
  words: readonly string[];
  /** What is left to parse against the command's flags and the global ones. */
  rest: readonly string[];
}

/** Find the command ``argv`` names. Its words come first; only global
 *  options may precede them. */
export function select<C extends CommandSpec>(commands: readonly C[], globals: FlagSpecs, argv: readonly string[]): Selection<C> {
  let i = 0;
  while (i < argv.length && argv[i]!.startsWith("-") && argv[i] !== "--") i += takesValue(globals, argv[i]!) ? 2 : 1;
  const words = argv.slice(i);
  const command = commands.find((c) => c.words.every((w, k) => words[k] === w));
  const rest = command ? words.slice(command.words.length) : [];
  return { command, words, rest: [...argv.slice(0, i), ...rest] };
}

function takesValue(globals: FlagSpecs, arg: string): boolean {
  const name = arg.slice(2);
  return arg.startsWith("--") && Object.hasOwn(globals, name) && globals[name]!.type !== "bool";
}

/** Each operand by name, once the call is one the command accepts. */
export function bind(command: CommandSpec, parsed: Parsed): Map<string, string> {
  const specs = command.operands ?? [];
  const operands = new Map<string, string>();
  specs.forEach((spec, i) => {
    const rest = spec.endsWith("...");
    const optional = spec.endsWith("?");
    const name = rest ? spec.slice(0, -3) : optional ? spec.slice(0, -1) : spec;
    const value = rest ? parsed.operands.slice(i).join(" ").trim() : parsed.operands[i];
    if (!value && optional) return;
    if (!value) throw new GrammarError(`Missing <${name}>`);
    operands.set(name, value);
  });
  if (parsed.values.has("wait") && parsed.values.has("detach")) throw new GrammarError("Pass --wait or --detach, not both");
  const extra = parsed.operands[specs.length];
  if (extra !== undefined && !specs.at(-1)?.endsWith("...")) throw new GrammarError(`Unexpected argument "${extra}"`);
  for (const [name, flag] of Object.entries(command.flags ?? {})) {
    if (flag.required && !parsed.values.has(name)) throw new GrammarError(`Missing ${flagName(name, flag)}`);
  }
  return operands;
}

export function flagName(name: string, flag: FlagSpec): string {
  const text = flag.short ? `-${flag.short}` : `--${name}`;
  return flag.type === "bool" ? text : `${text} ${flag.value ?? "VALUE"}`;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** How a command names a workspace: Home, its id, or its exact name. */
export type WorkspaceRef = { home: true } | { id: string } | { name: string };

export function workspaceRef(ref: string): WorkspaceRef {
  if (ref.toLowerCase() === "home") return { home: true };
  return UUID.test(ref) ? { id: ref } : { name: ref };
}
