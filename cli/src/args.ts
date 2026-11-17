import { usage } from "./errors.js";
import { type FlagSpec, GrammarError, type Parsed, parse as parseWords } from "./grammar.js";

export type { Parsed } from "./grammar.js";

export interface Flag extends FlagSpec {
  help?: string;
}

export type Flags = Record<string, Flag>;

/** ``read``, with the grammar's refusal reported as a usage error. */
export function asUsage<T>(read: () => T): T {
  try {
    return read();
  } catch (e) {
    throw e instanceof GrammarError ? usage(e.message) : e;
  }
}

export function parse(argv: readonly string[], flags: Flags): Parsed {
  return asUsage(() => parseWords(argv, flags));
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
