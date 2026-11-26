import { usage } from "../errors.js";
import type { Flag } from "../args.js";
import { print, table } from "../output.js";
import type { Command, Ctx, Row } from "./command.js";
import { base, by, driveCommand, type Entry, NOTE, noteOf, paged, reword, sh, SPACE, type Version, versionNote, type Who, when } from "./driveApi.js";

type Status = "draft" | "final";
const STATUSES = ["draft", "final"] as const;

interface VersionItem extends Version {
  type: "version";
  added: number | null;
  removed: number | null;
}

interface EventItem {
  type: "event";
  kind: "mark" | "unmark" | "update" | "clear";
  created_at: string;
  actor: Who;
  note: string | null;
  status_before: Status | null;
  status_after: Status | null;
  tickers_before: string[] | null;
  tickers_after: string[] | null;
  final_seq: number | null;
  cleared: number | null;
}

/** What checking a memo out or in answers. */
interface Change {
  change_id: string;
  path: string;
  reason: string;
  continued: boolean;
  versions: number;
  first_seq: number | null;
  last_seq: number | null;
  head_seq: number | null;
  adopted_seq: number | null;
}

const TICKERS: Flag = { type: "string", value: "A,B", help: "the tickers it is about, comma-separated" };

/** ``text`` when it is one of ``allowed``, else a usage error naming them. */
function choice<T extends string>(flag: string, text: string | undefined, allowed: readonly T[]): T | undefined {
  if (text === undefined) return undefined;
  if (!(allowed as readonly string[]).includes(text)) throw usage(`--${flag} takes ${allowed.join(", ")}`);
  return text as T;
}

/** `A, b` as `["A", "b"]`; an empty string is an empty list, which clears them. */
const tickers = (text: string | undefined): string[] | undefined =>
  text === undefined ? undefined : text.split(",").map((t) => t.trim()).filter(Boolean);

async function setMemo(
  ctx: Pick<Ctx, "api" | "args">,
  path: string,
  body: Record<string, unknown>,
  /** What to say when the file is not a memo. */
  notMemo?: string,
): Promise<Entry> {
  return ctx.api.json<Entry>("POST", `${base(ctx)}/memo`, { body: { path, ...body, note: noteOf(ctx.args.str("note")) } }).catch((e) => {
    throw reword(e, (reason) => (reason === "not_memo" ? notMemo : undefined));
  });
}

const lines = (v: VersionItem) => (v.added == null || v.removed == null ? "-" : `+${v.added} -${v.removed}`);

const tickerText = (t: string[]) => t.join(",");

function eventText(e: EventItem): string {
  if (e.kind === "mark") {
    const parts = [e.status_after ? `status ${e.status_after}` : "", e.tickers_after?.length ? `tickers ${tickerText(e.tickers_after)}` : ""].filter(Boolean);
    return `marked as a memo${parts.length ? ` (${parts.join(", ")})` : ""}`;
  }
  if (e.kind === "unmark") return "unmarked, versions kept";
  if (e.kind === "clear") return `history cleared (${e.cleared ?? 0} dropped)`;
  const parts: string[] = [];
  if (e.status_before !== e.status_after) {
    const pin = e.final_seq != null ? ` (pins v${e.final_seq})` : "";
    parts.push(`status ${e.status_before ?? "none"} -> ${e.status_after ?? "none"}${pin}`);
  } else if (e.final_seq != null) parts.push(`pins v${e.final_seq}`);
  const before = tickerText(e.tickers_before ?? []);
  const after = tickerText(e.tickers_after ?? []);
  if (before !== after) parts.push(`tickers ${before || "none"} -> ${after || "none"}`);
  return parts.join(", ") || "updated";
}

type RecordItem = VersionItem | EventItem;

/** Most items read past --limit to finish the change a page was cut in; past it the change is listed without totals. */
const CHANGE_READ_MAX = 1000;

/** ``page`` with the change its last item belongs to read on to its first version, since the record
 *  totals a change from the versions it shows; ``more`` is whether anything older is left unshown, and
 *  ``cut`` whether the read stopped inside that change, whose versions shown are then not all of it. */
async function wholeLastChange(
  page: { items: RecordItem[]; next: string | null },
  read: (limit: number, before?: string) => Promise<{ items: RecordItem[]; next?: string | null }>,
): Promise<{ items: RecordItem[]; more: boolean; cut: boolean }> {
  const last = page.items.at(-1);
  if (page.next === null || last?.type !== "version" || !last.change_id) return { items: page.items, more: page.next !== null, cut: false };
  const items = [...page.items];
  const same = (x: RecordItem) => x.type === "version" && x.change_id === last.change_id;
  let next: string | null = page.next;
  while (next !== null && items.length - page.items.length < CHANGE_READ_MAX) {
    const more = await read(200, next);
    const stop = more.items.findIndex((x) => !same(x));
    if (stop !== -1) {
      items.push(...more.items.slice(0, stop));
      return { items, more: true, cut: false };
    }
    items.push(...more.items);
    next = more.items.length ? (more.next ?? null) : null;
  }
  return { items, more: next !== null, cut: next !== null };
}

export const driveMemo: Command[] = [
  driveCommand({
    words: ["drive", "memo", "mark"],
    summary: "make a Drive file a memo, which keeps every version with who made it and why; any file format",
    operands: ["path"],
    flags: {
      status: { type: "string", value: "draft|final", help: "its status; final pins the current version" },
      tickers: TICKERS,
      note: NOTE,
      space: SPACE,
    },
    async run(ctx) {
      const path = ctx.args.op("path");
      const st = choice("status", ctx.args.str("status"), STATUSES);
      const entry = await setMemo(ctx, path, { memo: true, status: st, tickers: tickers(ctx.args.str("tickers")) });
      print(ctx.json, entry, () => {
        const m = entry.memo;
        const parts = [m?.status ? `status ${m.status}` : "", m?.tickers?.length ? `tickers ${tickerText(m.tickers)}` : ""].filter(Boolean);
        return `Marked ${entry.path} as a memo${parts.length ? ` (${parts.join(", ")})` : ""}.`;
      });
    },
  }),
  driveCommand({
    words: ["drive", "memo", "unmark"],
    summary: "stop treating a file as a memo; its versions are kept, and later saves keep only the copy from before the last save",
    operands: ["path"],
    flags: { note: NOTE, space: SPACE },
    async run(ctx) {
      const path = ctx.args.op("path");
      const entry = await setMemo(ctx, path, { memo: false }, `${path} is not a memo.`);
      print(ctx.json, entry, () => `Unmarked ${entry.path}; its versions are kept.`);
    },
  }),
  driveCommand({
    words: ["drive", "memo", "set"],
    summary: "change a memo's status or tickers; --status none clears the status, --tickers \"\" clears the tickers",
    operands: ["path"],
    flags: {
      status: { type: "string", value: "draft|final|none", help: "its status; final pins the current version" },
      tickers: TICKERS,
      note: NOTE,
      space: SPACE,
    },
    async run(ctx) {
      const { args } = ctx;
      const given = choice("status", args.str("status"), [...STATUSES, "none"] as const);
      const st = given === "none" ? null : given;
      const list = tickers(args.str("tickers"));
      if (st === undefined && list === undefined) throw usage("Pass --status or --tickers");
      const path = args.op("path");
      const entry = await setMemo(ctx, path, { status: st, tickers: list }, `${path} is not a memo. Mark it first: la drive memo mark ${sh(path)}`);
      print(ctx.json, entry, () => {
        const m = entry.memo;
        const lines: string[] = [];
        if (st === "final") lines.push(`Status final, pinned at version ${m?.final_seq ?? entry.version}.`);
        else if (st === "draft") lines.push("Status draft.");
        else if (st === null) lines.push("Status cleared.");
        if (list !== undefined) lines.push(m?.tickers?.length ? `Tickers ${tickerText(m.tickers)}.` : "Tickers cleared.");
        return lines.join("\n");
      });
    },
  }),
  driveCommand({
    words: ["drive", "memos"],
    summary: "list every memo in the Drive, whatever folder it is in, with its status and tickers",
    flags: {
      status: { type: "string", value: "draft|final", help: "only memos with this status" },
      ticker: { type: "string", value: "T", help: "only memos about this ticker" },
      sort: { type: "string", value: "revised|name|path|status", help: "the order (default: revised, newest first)" },
      limit: { type: "int", value: "N", help: "the most memos to list (default 100)" },
      space: SPACE,
    },
    async run(ctx) {
      const { args } = ctx;
      const sort = choice("sort", args.str("sort"), ["revised", "name", "path", "status"] as const);
      const status = choice("status", args.str("status"), STATUSES);
      type Memo = { entry: Entry; versions: number };
      const found = await paged<Memo>(args.int("limit") ?? 100, 500, async (limit, cursor) => {
        const page = await ctx.api.json<{ memos: Memo[]; next?: string | null }>("GET", `${base(ctx)}/memos`, {
          // Names and paths read A to Z; the server's default order is newest or last first.
          query: { sort, order: sort === "name" || sort === "path" ? "asc" : undefined, status, ticker: args.str("ticker"), limit, cursor },
        });
        return { items: page.memos, next: page.next };
      });
      const answer = { memos: found.items, next: found.next };
      if (answer.next && !ctx.json) {
        process.stderr.write(`More memos than these ${answer.memos.length} exist; pass --limit to see more.\n`);
      }
      const rows = answer.memos.map(({ entry: e, versions }) => {
        const m = e.memo;
        // `final v6`, starred when the file has been saved since.
        const pinned = m?.status === "final" && m.final_seq != null ? `final v${m.final_seq}${(e.version ?? 0) > m.final_seq ? "*" : ""}` : (m?.status ?? "-");
        return {
          path: e.path,
          status: pinned,
          tickers: m?.tickers?.length ? tickerText(m.tickers) : "-",
          revised: when(e.modified_at),
          by: by(e.modified_by),
          thread: e.modified_by?.thread_id,
          versions,
        };
      });
      print(ctx.json, answer, () => table(rows as Row[], ["path", "status", "tickers", "revised", "by", "thread", "versions"]));
    },
  }),
  driveCommand({
    words: ["drive", "record"],
    summary: "show a file's record, newest first: each change with who, why and lines changed (a check-out's versions as one entry), and each status or mark change",
    operands: ["path"],
    flags: { limit: { type: "int", value: "N", help: "the most items to list (default 50)" }, space: SPACE },
    async run(ctx) {
      const path = ctx.args.op("path");
      const read = (limit: number, before?: string) =>
        ctx.api.json<{ items: (VersionItem | EventItem)[]; next?: string | null }>("GET", `${base(ctx)}/record`, { query: { path, limit, before } });
      const answer = await paged<VersionItem | EventItem>(ctx.args.int("limit") ?? 50, 200, read);
      // --json is the server's items up to --limit, each carrying its change_id, so it totals nothing to finish.
      if (ctx.json) {
        print(true, answer, () => "");
        return;
      }
      const { items, more, cut } = await wholeLastChange(answer, read);
      if (more) process.stderr.write("Older items remain; pass --limit to see more.\n");
      const rows: Row[] = [];
      for (let n = 0; n < items.length; n++) {
        const i = items[n]!;
        if (i.type !== "version") {
          rows.push({ version: "--", when: when(i.created_at), who: by(i.actor), thread: i.actor.thread_id, change: eventText(i), note: i.note });
          continue;
        }
        // Consecutive versions of one change are one entry, its versions listed under it.
        let end = n + 1;
        while (i.change_id && end < items.length && (items[end] as VersionItem).type === "version" && (items[end] as VersionItem).change_id === i.change_id) end++;
        const group = items.slice(n, end) as VersionItem[];
        if (group.length === 1) {
          rows.push({ version: `v${i.seq}`, when: when(i.created_at), who: by(i.actor), thread: i.actor.thread_id, change: lines(i), note: versionNote(i) });
          continue;
        }
        const oldest = group[group.length - 1]!;
        const known = group.filter((v) => v.added != null && v.removed != null);
        const day = (v: VersionItem) => when(v.created_at).slice(0, 10);
        rows.push({
          version: `v${oldest.seq} to v${i.seq}`,
          when: day(oldest) === day(i) ? when(i.created_at) : `${when(oldest.created_at)} to ${when(i.created_at)}`,
          who: by(i.actor),
          thread: i.actor.thread_id,
          // A total is of the whole change: none where a version lacks its counts or older ones were not read.
          change: known.length === group.length && !(cut && end === items.length) ? `+${known.reduce((x, v) => x + v.added!, 0)} -${known.reduce((x, v) => x + v.removed!, 0)}` : "-",
          note: i.note,
        });
        // A subagent's save can join the main agent's change, so a child shows its actor where it differs from the header's;
        // the reason is the header's, and a restore is said on the version that is one.
        for (const v of group)
          rows.push({
            version: `  v${v.seq}`,
            when: when(v.created_at),
            who: by(v.actor) === by(i.actor) ? "" : by(v.actor),
            thread: v.actor.thread_id === i.actor.thread_id ? "" : v.actor.thread_id,
            change: lines(v),
            note: versionNote({ note: null, restored_from: v.restored_from }),
          });
        n = end - 1;
      }
      print(false, answer, () => table(rows, ["version", "when", "who", "thread", "change", "note"]));
    },
  }),
  driveCommand({
    words: ["drive", "checkout"],
    summary:
      "check a memo out before you change it: the words after the path are one line on what you are changing and why (after -- when they start with a dash); --continue carries on with this conversation's last change to it instead",
    operands: ["path", "reason...?"],
    flags: { continue: { type: "bool", help: "carry on with this conversation's earlier change to the memo instead of starting a new one" }, space: SPACE },
    async run(ctx) {
      const path = ctx.args.op("path");
      const text = ctx.args.opt("reason");
      const carry = ctx.args.has("continue");
      const forms = `la drive checkout ${sh(path)} '<what you are changing and why>'  or  la drive checkout ${sh(path)} --continue`;
      if (text && carry) throw usage(`Pass a reason or --continue, not both: ${forms}`);
      if (!text && !carry) throw usage(`Say what you are changing and why, or pass --continue: ${forms}`);
      const done = await ctx.api.json<Change>("POST", `${base(ctx)}/memo/checkout`, {
        body: carry ? { path, continue: true } : { path, reason: noteOf(text) },
      });
      print(ctx.json, done, () => {
        // Either form adopts a save the chat made through the mount with no check-out.
        const adopted = done.adopted_seq != null ? `v${done.adopted_seq}, the save already made, now has this reason.\n` : "";
        if (carry) return `${adopted}Carrying on: "${done.reason}". Edits to the memo are saved under it.`;
        return `${adopted}Checked out. Edit the memo where it is; each edit is saved to it under this reason. Later edits that are part of this change need no new check-out, and a different change to the memo gets its own. The check-out ends once this conversation has been quiet for an hour; la drive checkout ${sh(path)} --continue then carries the same change on.`;
      });
    },
  }),
  driveCommand({
    words: ["drive", "checkin"],
    summary:
      "reword the reason of your finished change to a memo, for every version made under it, and end the change; for when the change deserves a more exact reason than it started with, since a check-out otherwise ends by itself an hour after the conversation goes quiet",
    operands: ["path", "reason...?"],
    flags: { space: SPACE },
    async run(ctx) {
      const path = ctx.args.op("path");
      const text = ctx.args.opt("reason");
      const done = await ctx.api.json<Change>("POST", `${base(ctx)}/memo/checkin`, {
        body: text ? { path, reason: noteOf(text) } : { path },
      });
      print(ctx.json, done, () =>
        done.first_seq == null || done.last_seq == null
          ? `Checked in: ${path}, no change made.`
          : done.first_seq === done.last_seq
            ? `Checked in: ${path} v${done.first_seq}.`
            : `Checked in: ${path} v${done.first_seq} to v${done.last_seq}.`,
      );
    },
  }),
  driveCommand({
    words: ["drive", "undo"],
    summary: "put a file back as it was before its last save (recorded as a new version; run again to redo)",
    operands: ["path"],
    flags: { note: NOTE, space: SPACE },
    async run(ctx) {
      const path = ctx.args.op("path");
      const entry = await ctx.api.json<Entry>("POST", `${base(ctx)}/undo`, { body: { path, note: noteOf(ctx.args.str("note")) } });
      print(ctx.json, entry, () => `Put back ${entry.path} as it was before its last save (now version ${entry.version}).`);
    },
  }),
];
