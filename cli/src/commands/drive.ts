import { createWriteStream, existsSync, mkdtempSync, readdirSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import { pipeline } from "node:stream/promises";
import { CliError, Exit, usage } from "../errors.js";
import { p } from "../http.js";
import { print, printable, table } from "../output.js";
import type { Flag } from "../args.js";
import { type Command, command, type Ctx, type Row } from "./command.js";
import { bytesOf, type Entry, findSpace, getFile, maxUpload, putFile } from "./driveTransfer.js";

interface Who {
  kind: string;
  agent?: string | null;
  thread_id?: string | null;
}

interface Version {
  seq: number;
  size: number;
  created_at: string;
  actor: Who;
  note: string | null;
  restored_from: number | null;
  /** When this old version goes; null for the current one and one kept without a limit. */
  expires_at?: string | null;
  /** Whether this version's bytes are text; absent from a server that does not say. */
  is_text?: boolean;
}


interface TrashItem {
  trash_id: string;
  entry: Entry;
  entries: number;
  deleted_at: string;
}

/** The most trash items one answer lists (the route's own ceiling). */
const TRASH_LIST_MAX = 1000;

const SPACE: Flag = { type: "string", value: "SPACE", help: "the Drive space (default: personal)" };
const REV: Flag = { type: "int", value: "N", help: "an older version of the file, its number from `drive history`" };

const base = (ctx: Pick<Ctx, "api" | "args">) => p`/api/v1/drive/${ctx.args.str("space") ?? "personal"}`;

const when = (iso: string) => iso.slice(0, 16).replace("T", " ");

/** A person, or the agent that wrote it, with its name when the server has it. */
function by(who: Who | null | undefined): string {
  if (!who) return "-";
  if (who.kind !== "agent") return who.kind;
  return who.agent ? `agent ${who.agent}` : "agent";
}

const entryRow = (e: Entry, name: "name" | "path") => ({
  [name]: e.kind === "folder" ? `${e[name]}/` : e[name],
  kind: e.kind,
  size: bytesOf(e.size),
  modified: when(e.modified_at),
  by: by(e.modified_by),
});

async function stat(ctx: Pick<Ctx, "api" | "args">, path: string): Promise<Entry> {
  return ctx.api.json<Entry>("GET", `${base(ctx)}/entry`, { query: { path } });
}

function fileOnly(e: Entry, verb: string): void {
  if (e.kind === "folder") throw new CliError(`${e.path} is a folder; ${verb} takes a file`, Exit.usage);
}

/** Standard input spooled to a file, so a pipe of any size is sent as a file is. */
async function spoolStdin(dir: string): Promise<string> {
  const file = join(dir, "stdin");
  await pipeline(process.stdin, createWriteStream(file));
  return file;
}

/** Every file under ``dir`` with its path relative to it, in a fixed order. */
function filesUnder(dir: string, rel = ""): string[] {
  const found: string[] = [];
  for (const e of readdirSync(join(dir, rel), { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
    const at = rel ? `${rel}/${e.name}` : e.name;
    if (e.isDirectory()) found.push(...filesUnder(dir, at));
    else if (e.isFile()) found.push(at);
  }
  return found;
}

export const drive: Command[] = [
  command({
    words: ["drive", "ls"],
    summary: "list a Drive folder (the root when no path is given); use -r for everything under it",
    operands: ["path?"],
    flags: {
      recursive: { type: "bool", short: "r", help: "list everything under the folder, not just its children" },
      space: SPACE,
    },
    async run(ctx) {
      const { args, json } = ctx;
      const recursive = args.has("recursive");
      const { entries } = await ctx.api.json<{ entries: Entry[] }>("GET", `${base(ctx)}/entries`, {
        query: { path: args.opt("path") ?? "", recursive: recursive || undefined },
      });
      const name = recursive ? "path" : "name";
      print(json, entries, () =>
        table(entries.map((e) => entryRow(e, name)) as Row[], [name, "kind", "size", "modified", "by"]),
      );
    },
  }),
  command({
    words: ["drive", "cat"],
    summary: "print a text file from the Drive; a binary file is refused, use `drive get` for it",
    operands: ["path"],
    flags: { rev: REV, space: SPACE },
    async run(ctx) {
      const path = ctx.args.op("path");
      const entry = await stat(ctx, path);
      fileOnly(entry, "cat");
      const rev = ctx.args.int("rev");
      // The entry describes the current version; an older one is judged by its own record in the
      // history, or by its bytes when the server's answer does not say.
      let known = rev === undefined ? entry.is_text : undefined;
      if (rev !== undefined) {
        const { versions } = await ctx.api.json<{ versions: Version[] }>("GET", `${base(ctx)}/versions`, { query: { path } });
        known = versions.find((v) => v.seq === rev)?.is_text;
      }
      if (known === false) {
        throw new CliError(`${entry.path} is a binary file; download it with: la drive get ${entry.path}`);
      }
      const bytes = await ctx.api.bytes("GET", `${base(ctx)}/content`, { query: { path, seq: rev } });
      if (!(rev !== undefined && known === true) && bytes.includes(0)) {
        throw new CliError(`${entry.path} is a binary file; download it with: la drive get ${entry.path}`);
      }
      // An escape sequence in a file anyone with Drive access can write must
      // not reach a terminal; a pipe gets the bytes as they are.
      if (process.stdout.isTTY) process.stdout.write(printable(Buffer.from(bytes).toString("utf8")));
      else process.stdout.write(bytes);
    },
  }),
  command({
    words: ["drive", "get"],
    summary: "download a Drive file's bytes to a local file (default: its name here); use for binary files",
    operands: ["path"],
    flags: {
      output: { type: "string", short: "o", value: "FILE", help: "where to write it (default: the file's name, in this directory)" },
      rev: REV,
      force: { type: "bool", help: "overwrite the local file if it exists" },
      space: SPACE,
    },
    async run(ctx) {
      const path = ctx.args.op("path");
      const entry = await stat(ctx, path);
      fileOnly(entry, "get");
      const done = await getFile({
        api: ctx.api,
        base: base(ctx),
        path,
        rev: ctx.args.int("rev"),
        out: ctx.args.str("output") ?? basename(entry.path),
        force: ctx.args.has("force"),
        quiet: ctx.json,
      });
      print(ctx.json, done, () => `Saved ${entry.path} to ${done.output} (${bytesOf(done.size)}).`);
    },
  }),
  command({
    words: ["drive", "put"],
    summary: "upload a local file, or every file under a folder, to a Drive path; any size, and run again to continue an interrupted upload; FILE - reads stdin",
    operands: ["file", "path"],
    flags: {
      note: { type: "string", value: "WHY", help: "why you changed it; shown in the file's history" },
      new: { type: "bool", help: "fail if the path already exists" },
      "if-match": { type: "string", value: "SHA256", help: "fail if the file changed since this content hash (sha256 in `ls --json`)" },
      space: SPACE,
    },
    async run(ctx) {
      const { args, json } = ctx;
      const file = args.op("file");
      const dest = args.op("path");
      const ifMatch = args.str("if-match");
      if (args.has("new") && ifMatch !== undefined) throw usage("Pass --new or --if-match, not both");
      const spool = file === "-" ? mkdtempSync(join(tmpdir(), "la-drive-")) : undefined;
      try {
        const source = spool ? await spoolStdin(spool) : file;
        if (!existsSync(source)) throw new CliError(`No such file: ${file}`, Exit.notFound);
        const folder = statSync(source).isDirectory();
        if (folder && ifMatch !== undefined) throw usage("--if-match names one file's hash; it does not apply to a folder");
        if (!folder && !statSync(source).isFile()) throw usage(`${file} is not a file`);
        const limit = await maxUpload(ctx.api, args.str("space"));
        const sends = folder
          ? filesUnder(source).map((rel) => ({ from: join(source, rel), to: `${dest.replace(/\/+$/, "")}/${rel}` }))
          : [{ from: source, to: dest }];
        if (folder && sends.length === 0) throw usage(`${file} holds no files`);
        const saved: Entry[] = [];
        for (const one of sends) {
          const entry = await putFile({
            api: ctx.api,
            base: base(ctx),
            file: one.from,
            size: statSync(one.from).size,
            path: one.to,
            note: args.str("note"),
            ifMatch,
            ifNone: args.has("new"),
            limit,
            quiet: json,
          }).catch((e) => {
            throw folder && e instanceof CliError ? new CliError(`${one.to}: ${e.message}`, e.code, e) : e;
          });
          saved.push(entry);
          if (!json) print(false, entry, () => `Saved ${entry.path} (version ${entry.version}, ${bytesOf(entry.size)}).`);
        }
        if (json) print(true, folder ? saved : saved[0], () => "");
      } finally {
        if (spool) rmSync(spool, { recursive: true, force: true });
      }
    },
  }),
  command({
    words: ["drive", "mkdir"],
    summary: "create a Drive folder, with any folders above it",
    operands: ["path"],
    flags: { space: SPACE },
    async run(ctx) {
      const entry = await ctx.api.json<Entry>("POST", `${base(ctx)}/folders`, { body: { path: ctx.args.op("path") } });
      print(ctx.json, entry, () => `Created ${entry.path}/.`);
    },
  }),
  command({
    words: ["drive", "mv"],
    summary: "rename or move a Drive file, or a folder with everything in it",
    operands: ["from", "to"],
    flags: { space: SPACE },
    async run(ctx) {
      const entry = await ctx.api.json<Entry>("POST", `${base(ctx)}/move`, {
        body: { from: ctx.args.op("from"), to: ctx.args.op("to") },
      });
      print(ctx.json, entry, () => `Moved ${ctx.args.op("from")} to ${entry.path}.`);
    },
  }),
  command({
    words: ["drive", "rm"],
    summary: "move a Drive file or folder to the trash (kept 14 days; undo with `drive trash restore`)",
    operands: ["path"],
    flags: { space: SPACE },
    async run(ctx) {
      const path = ctx.args.op("path");
      const { trash_id } = await ctx.api.json<{ trash_id: string }>("POST", `${base(ctx)}/trash`, { body: { path } });
      print(ctx.json, { trash_id }, () => `Moved ${path} to the trash. Undo: la drive trash restore ${trash_id}`);
    },
  }),
  command({
    words: ["drive", "history", "clear"],
    summary: "delete a file's earlier versions for good, keeping its current content; frees the room they take",
    operands: ["path"],
    flags: {
      before: { type: "int", value: "N", help: "drop only the versions older than version N" },
      space: SPACE,
    },
    async run(ctx) {
      const path = ctx.args.op("path");
      const cleared = await ctx.api.json<{ dropped: number; freed_bytes: number }>("POST", `${base(ctx)}/versions/clear`, {
        body: { path, before: ctx.args.int("before") },
      });
      print(ctx.json, cleared, () =>
        cleared.dropped === 0
          ? `${path} had no earlier versions to drop.`
          : `Dropped ${cleared.dropped} earlier version${cleared.dropped === 1 ? "" : "s"} of ${path}, freeing ${bytesOf(cleared.freed_bytes)}.`,
      );
    },
  }),
  command({
    words: ["drive", "history"],
    summary: "list a file's versions, newest first, with who changed it, why, and when each earlier one goes",
    operands: ["path"],
    flags: { space: SPACE },
    async run(ctx) {
      const path = ctx.args.op("path");
      const listing = await ctx.api.json<{ versions: Version[]; history_bytes: number; too_large_for_history?: boolean }>(
        "GET",
        `${base(ctx)}/versions`,
        { query: { path } },
      );
      const { versions } = listing;
      const rows = versions.map((v, i) => ({
        version: v.seq,
        when: when(v.created_at),
        who: by(v.actor),
        thread: v.actor.thread_id,
        size: bytesOf(v.size),
        goes: i === 0 ? "current" : v.expires_at ? when(v.expires_at) : "kept",
        note: v.restored_from ? [`restored from v${v.restored_from}`, v.note].filter(Boolean).join(": ") : v.note,
      }));
      print(ctx.json, listing, () => {
        const lines = [table(rows, ["version", "when", "who", "thread", "size", "goes", "note"])];
        if (listing.too_large_for_history) {
          lines.push(`${path} is too large to keep earlier versions: saving over it replaces it.`);
        } else if (versions.length > 1) {
          lines.push(`Earlier versions take ${bytesOf(listing.history_bytes)}. Drop them with: la drive history clear ${path}`);
        }
        return lines.join("\n");
      });
    },
  }),
  command({
    words: ["drive", "usage"],
    summary: "show how much of the Drive's space is used; the trash is shown apart and does not count",
    flags: { space: SPACE },
    async run(ctx) {
      const space = await findSpace(ctx.api, ctx.args.str("space"));
      const u = space.usage;
      print(ctx.json, space, () =>
        [
          u.max_bytes == null ? `${bytesOf(u.bytes)} used.` : `${bytesOf(u.bytes)} of ${bytesOf(u.max_bytes)} used.`,
          `${u.entries} of ${u.max_entries} files and folders, the trash's included.`,
          `The trash holds ${bytesOf(u.trash_bytes ?? 0)}, which does not count toward the space; ` +
            `it keeps items ${space.trash_days} days, and the oldest go early if it grows past the space.`,
        ].join("\n"),
      );
    },
  }),
  command({
    words: ["drive", "restore"],
    summary: "make an older version the file's current content (recorded as a new version)",
    operands: ["path"],
    flags: { rev: { ...REV, required: true }, space: SPACE },
    async run(ctx) {
      const entry = await stat(ctx, ctx.args.op("path"));
      fileOnly(entry, "restore");
      const seq = ctx.args.int("rev")!;
      const restored = await ctx.api.json<Entry>("POST", `${base(ctx)}/versions/restore`, { body: { id: entry.id, seq } });
      print(ctx.json, restored, () => `Restored ${restored.path} to version ${seq} (now version ${restored.version}).`);
    },
  }),
  command({
    words: ["drive", "trash", "restore"],
    summary: "put a trashed file or folder back, by the id `drive trash` shows",
    operands: ["trash_id"],
    flags: { space: SPACE },
    async run(ctx) {
      const { entries } = await ctx.api.json<{ entries: Entry[] }>("POST", `${base(ctx)}/trash/restore`, {
        body: { trash_id: ctx.args.op("trash_id") },
      });
      print(ctx.json, entries, () => {
        const top = entries.reduce<Entry | undefined>((a, e) => (!a || e.path.length < a.path.length ? e : a), undefined);
        return `Restored ${top?.path ?? "nothing"}${entries.length > 1 ? ` and ${entries.length - 1} more` : ""}.`;
      });
    },
  }),
  command({
    words: ["drive", "trash"],
    summary: "list what is in the Drive trash, with the id to restore it by",
    flags: { space: SPACE },
    async run(ctx) {
      const { items, more } = await ctx.api.json<{ items: TrashItem[]; more?: boolean }>("GET", `${base(ctx)}/trash`, {
        query: { limit: TRASH_LIST_MAX },
      });
      if (more) {
        process.stderr.write(`The trash holds more than these ${items.length} items; older ones are not listed.\n`);
      }
      const rows = items.map((t) => ({
        trash_id: t.trash_id,
        path: t.entry.kind === "folder" ? `${t.entry.path}/` : t.entry.path,
        kind: t.entry.kind,
        entries: t.entries,
        deleted: when(t.deleted_at),
      }));
      print(ctx.json, items, () => table(rows, ["trash_id", "path", "kind", "entries", "deleted"]));
    },
  }),
];
