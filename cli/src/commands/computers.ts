import { CliError, Exit, usage } from "../errors.js";
import { p } from "../http.js";
import { print, printable, table } from "../output.js";
import { type Command, command, type Ctx, remove, type Row, YES } from "./command.js";

/** ``user@host`` or ``user@host:port``. */
export function sshTarget(spec: string): { username: string; host: string; port: number } {
  const m = /^([^@\s]+)@(\[[^\]]+\]|[^:\s]+)(?::(\d+))?$/.exec(spec);
  if (!m) throw usage("--ssh takes user@host or user@host:port");
  return { username: m[1]!, host: m[2]!.replace(/^\[|\]$/g, ""), port: m[3] ? Number(m[3]) : 22 };
}

const add = command({
  words: ["computers", "add"],
  summary: "add a computer; with --ssh, a machine LangAlpha logs in to",
  flags: {
    name: { type: "string", value: "N" },
    kind: { type: "string", value: "K", help: "the sandbox provider (default: the server's)" },
    tier: { type: "string", value: "T", help: "the resource tier" },
    ssh: { type: "string", value: "USER@HOST[:PORT]", help: "a machine that accepts the server's SSH key" },
    root: { type: "string", value: "DIR", help: "with --ssh: where the computer's files go on the machine" },
    "host-key-fingerprint": { type: "string", value: "FP", help: "with --ssh: the host key to expect" },
    yes: { ...YES, help: "with --ssh: trust the host key the machine shows" },
  },
  async run(ctx) {
    const { api, args, json } = ctx;
    const ssh = args.str("ssh");
    if (ssh) return addSsh(ctx, ssh);
    const body = { name: args.str("name"), kind: args.str("kind"), resource_tier: args.str("tier") };
    const computer = await api.json<Row>("POST", "/api/v1/computers", { body });
    print(json, computer, () => `Added computer ${computer.name} (${computer.computer_id}).`);
  },
});

/** Add a machine over SSH: the server logs in with its own key, which the
 *  machine must already accept, reports the host key to pin, then adds it. */
async function addSsh(ctx: Ctx, spec: string): Promise<void> {
  const { api, args, json } = ctx;
  const target = sshTarget(spec);
  let probe: Row;
  try {
    probe = await api.json<Row>("POST", "/api/v1/computers/ssh/probe", { body: target });
  } catch (e) {
    if (!(e instanceof CliError) || e.reason !== "auth_failed") throw e;
    const key = await api.json<Row>("GET", "/api/v1/computers/ssh/key");
    throw new CliError(
      `${e.message}\nTo add it, run this there as ${target.username}, then retry:\n\n  ${key.authorized_keys_command}\n`,
      Exit.error,
      e,
    );
  }
  if (probe.supported === false) {
    const problems = Array.isArray(probe.problems) ? (probe.problems as string[]).join("; ") : "";
    throw new CliError(`This machine cannot run a computer: ${problems}`, Exit.error);
  }
  // What works but needs the owner's attention, Node for the CLI among them.
  for (const problem of Array.isArray(probe.problems) ? (probe.problems as string[]) : []) {
    process.stderr.write(printable(`Note: ${problem}\n`));
  }
  // Trust on first use, as `ssh` asks: the key the probe met is the one pinned.
  const pinned = args.str("host-key-fingerprint");
  if (pinned ? pinned !== probe.fingerprint : !args.has("yes")) {
    throw usage(
      pinned
        ? `The machine's host key is ${probe.fingerprint}, not ${pinned}`
        : `The machine's host key is ${probe.fingerprint}. Pass --yes, or --host-key-fingerprint ${probe.fingerprint}, to add it`,
    );
  }
  const suggested = typeof probe.suggested_root_dir === "string" ? probe.suggested_root_dir : undefined;
  const body = {
    kind: "ssh",
    name: args.str("name"),
    ssh: { ...target, host_key: probe.host_key, root_dir: args.str("root") ?? suggested },
  };
  const computer = await api.json<Row>("POST", "/api/v1/computers", { body });
  print(json, computer, () => `Added computer ${computer.name} (${computer.computer_id}) on ${spec}.`);
}

const power = (action: "start" | "stop") =>
  command({
    words: ["computers", action],
    summary: `${action} a computer and every workspace on it`,
    operands: ["id"],
    async run({ api, args, json }) {
      const result = await api.json<Row>("POST", p`/api/v1/computers/${args.op("id")}/${action}`);
      print(json, result, () => `Computer ${result.status ?? (action === "start" ? "starting" : "stopping")}.`);
    },
  });

export const computers: Command[] = [
  command({
    words: ["computers", "list"],
    summary: "your computers",
    async run({ api, json }) {
      const page = await api.json<{ computers: Row[] }>("GET", "/api/v1/computers");
      print(json, page.computers, () => table(page.computers, ["computer_id", "name", "kind", "status", "resource_tier"]));
    },
  }),
  add,
  power("start"),
  power("stop"),
  command({
    words: ["computers", "remove"],
    summary: "remove a computer and delete its machine",
    operands: ["id"],
    flags: { yes: YES },
    async run(ctx) {
      const id = ctx.args.op("id");
      await remove(ctx, {
        path: p`/api/v1/computers/${id}`,
        key: { computer_id: id },
        warning: "Removing a computer deletes its machine",
        done: "Computer removed.",
      });
    },
  }),
];
