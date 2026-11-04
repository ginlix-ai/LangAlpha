import { usage } from "./errors.js";

export interface Flag {
  type: "bool" | "string" | "int";
  short?: string;
  /** The value's placeholder in help: ``--name N``. */
  value?: string;
  required?: boolean;
  help?: string;
}

export type Flags = Record<string, Flag>;

export interface Parsed {
  operands: string[];
  values: Map<string, string | number | true>;
}

/** Only the flags given are accepted, so a typo is refused rather than
 *  taken as a value-less flag that swallows the next word. */
export function parse(argv: readonly string[], flags: Flags): Parsed {
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
      if (!Object.hasOwn(flags, name)) throw usage(`Unknown option --${name}`);
    } else if (arg.length > 1 && arg.startsWith("-") && !/^-\d/.test(arg)) {
      const short = Object.keys(flags).find((k) => flags[k]!.short === arg.slice(1));
      if (!short) throw usage(`Unknown option ${arg}`);
      name = short;
    } else {
      operands.push(arg);
      continue;
    }
    const flag = flags[name]!;
    if (flag.type === "bool") {
      if (value !== undefined) throw usage(`--${name} takes no value`);
      values.set(name, true);
      continue;
    }
    if (value === undefined) {
      value = argv[++i];
      if (value === undefined) throw usage(`--${name} needs a value`);
    }
    if (flag.type === "int") {
      const n = Number(value);
      if (value.trim() === "" || !Number.isInteger(n)) throw usage(`--${name} must be a whole number`);
      values.set(name, n);
    } else {
      values.set(name, value);
    }
  }
  return { operands, values };
}

/** A command's parsed arguments. The type parameters are the names its spec
 *  declares, so a handler cannot read a flag or operand it never declared. */
export class Args<F extends string = string, O extends string = string> {
  constructor(
    private readonly values: Map<string, string | number | true>,
    private readonly operands: Map<string, string>,
  ) {}

  has(name: F): boolean {
    return this.values.has(name);
  }

  str(name: F): string | undefined {
    const v = this.values.get(name);
    return typeof v === "string" ? v : undefined;
  }

  int(name: F): number | undefined {
    const v = this.values.get(name);
    return typeof v === "number" ? v : undefined;
  }

  op(name: O): string {
    return this.operands.get(name)!;
  }
}
