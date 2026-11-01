import { describe, expect, it, vi } from "vitest";
import { type Flags, parse } from "../src/args.js";
import { bind, parseFor, select } from "../src/commands/command.js";
import { sshTarget } from "../src/commands/computers.js";
import { COMMANDS } from "../src/commands/index.js";

const FLAGS: Flags = {
  workspace: { type: "string", short: "w" },
  wait: { type: "bool" },
  model: { type: "string" },
  limit: { type: "int" },
};

describe("parse", () => {
  it("keeps a message after a boolean flag as an operand", () => {
    const p = parse(["-w", "home", "--wait", "what moved", "--model=x"], FLAGS);
    expect(p.operands).toEqual(["what moved"]);
    expect(p.values.get("workspace")).toBe("home");
    expect(p.values.get("wait")).toBe(true);
    expect(p.values.get("model")).toBe("x");
  });

  it("takes everything after -- as operands", () => {
    expect(parse(["t", "--", "--json"], FLAGS).operands).toEqual(["t", "--json"]);
  });

  it("refuses a valued flag with no value", () => {
    expect(() => parse(["-w"], FLAGS)).toThrow(/needs a value/);
  });

  it("refuses a flag it was not given, rather than letting it swallow a word", () => {
    expect(() => parse(["--detatch", "hello"], FLAGS)).toThrow(/Unknown option --detatch/);
    expect(() => parse(["-x"], FLAGS)).toThrow(/Unknown option -x/);
    expect(() => parse(["--toString"], FLAGS)).toThrow(/Unknown option/);
  });

  it("refuses a number flag that is not a whole number", () => {
    expect(() => parse(["--limit", "ten"], FLAGS)).toThrow(/whole number/);
    expect(parse(["--limit", "10"], FLAGS).values.get("limit")).toBe(10);
  });
});

describe("a command's spec", () => {
  const run = (...argv: string[]) => {
    const { command, rest } = select(COMMANDS, argv);
    if (!command) throw new Error("no command");
    return bind(command, parseFor(command, rest));
  };

  it("accepts only the flags its command declares", () => {
    expect(() => run("threads", "send", "t1", "--detatch", "hi")).toThrow(/Unknown option --detatch/);
    expect(() => run("threads", "list", "--wait")).toThrow(/Unknown option --wait/);
    expect(run("threads", "send", "t1", "--detach", "what", "moved").op("message")).toBe("what moved");
  });

  it("takes global options before the command words", () => {
    expect(run("--server", "http://x", "threads", "show", "t1").str("server")).toBe("http://x");
  });

  it("asks for what is required and refuses what is extra", () => {
    expect(() => run("threads", "new", "hi")).toThrow(/Missing -w WS/);
    expect(() => run("threads", "send", "t1")).toThrow(/Missing <message>/);
    expect(() => run("threads", "show", "t1", "t2")).toThrow(/Unexpected argument "t2"/);
  });
});

describe("sshTarget", () => {
  it("reads user, host and port", () => {
    expect(sshTarget("ana@box.lan")).toEqual({ username: "ana", host: "box.lan", port: 22 });
    expect(sshTarget("ana@10.0.0.2:2222")).toEqual({ username: "ana", host: "10.0.0.2", port: 2222 });
    expect(sshTarget("ana@[::1]:22")).toEqual({ username: "ana", host: "::1", port: 22 });
  });

  it("refuses a target with no user", () => {
    expect(() => sshTarget("box.lan")).toThrow(/user@host/);
  });
});

describe("the name hints use", () => {
  const load = async (name: string | undefined) => {
    vi.resetModules();
    vi.stubEnv("LANGALPHA_CLI_NAME", name ?? "");
    return (await import("../src/name.js")).CLI_NAME;
  };

  it("is la only when la is what ran, since a shell may alias la to ls", async () => {
    expect(await load("la")).toBe("la");
    expect(await load("langalpha")).toBe("langalpha");
    expect(await load(undefined)).toBe("langalpha");
    vi.unstubAllEnvs();
  });
});
