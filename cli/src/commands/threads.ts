import { p } from "../http.js";
import { print, table } from "../output.js";
import { CliError, Exit, usage } from "../errors.js";
import { isOrder, pendingInterrupt, turn } from "../turn.js";
import { type Command, command, MODEL, remove, type Row, WORKSPACE, YES } from "./command.js";
import { resolveWorkspace } from "./workspaces.js";

const WAIT = { type: "bool", help: "print only the answer, once the turn ends" } as const;
const DETACH = { type: "bool", help: "return once the turn has started, with its thread and run" } as const;

/** What ``threads output`` says on stderr after ``Thread <id>`` when the
 *  thread is still running, with ``--json`` too. An agent polling from a
 *  shell reads it, and its loop guard counts each such read as a poll
 *  (``LA_STILL_RUNNING`` in src/ptc_agent/agent/limits.py, held to this line
 *  by a test): reword both together. */
export const STILL_RUNNING = "is still running; this is its text so far.";

const answer = (verdict: "approve" | "reject") =>
  command({
    words: ["threads", verdict],
    summary:
      verdict === "approve"
        ? "approve what the agent is waiting on, or answer its question with --message"
        : "reject what the agent is waiting on, or skip its question",
    operands: ["id"],
    flags: {
      message: { type: "string", value: "M", help: verdict === "approve" ? "the answer" : "why, for the agent" },
      interrupt: { type: "string", value: "I", help: "which request, when the thread waits on several" },
      wait: WAIT,
      detach: DETACH,
    },
    async run(ctx) {
      const { api, args } = ctx;
      const thread = args.op("id");
      const interrupt = await pendingInterrupt(api, thread, args.str("interrupt"));
      if (isOrder(interrupt)) {
        throw new CliError(`Thread ${thread} is waiting on an order, which only the LangAlpha app approves or rejects`, Exit.auth);
      }
      const decision = { type: verdict, message: args.str("message") };
      const decisions = interrupt.action_requests.map(() => decision);
      return turn(ctx, {
        method: "POST",
        path: p`/api/v1/threads/${thread}/messages`,
        thread,
        body: { messages: [], hitl_response: { [interrupt.interrupt_id]: { decisions } } },
      });
    },
  });

export const threads: Command[] = [
  command({
    words: ["threads", "list"],
    summary: "your threads, newest first",
    flags: {
      workspace: WORKSPACE,
      archived: { type: "bool", help: "archived threads instead (needs --workspace)" },
      limit: { type: "int", value: "N", help: "at most this many (default 20)" },
      offset: { type: "int", value: "N", help: "skip the N newest, for the next page" },
    },
    async run({ api, args, json }) {
      const ref = args.str("workspace");
      const offset = args.int("offset") ?? 0;
      // The server keeps no list of archived threads across workspaces.
      if (args.has("archived") && !ref) throw usage("--archived lists one workspace's threads: name it with --workspace");
      const page = await api.json<{ threads: Row[]; total?: number }>("GET", "/api/v1/threads", {
        query: {
          workspace_id: ref ? await resolveWorkspace(api, ref) : undefined,
          archived: args.has("archived") || undefined,
          limit: args.int("limit") ?? 20,
          offset: offset || undefined,
        },
      });
      // The thread's own llm_model is empty while it follows the account
      // default, so the model shown is the one its latest turn ran on.
      const rows = page.threads.map((t) => ({ ...t, model: t.latest_run_model }));
      print(json, page.threads, () =>
        table(rows, ["thread_id", "title", "current_status", "model", "workspace_id", "updated_at"]),
      );
      // On stderr, so --json stays the rows: a page is not every thread, and
      // a thread looked for and not on it may be on the next.
      const shown = offset + page.threads.length;
      if (typeof page.total === "number" && page.threads.length > 0 && shown < page.total) {
        process.stderr.write(
          `Showing threads ${offset + 1}-${shown} of ${page.total}, newest first; pass --offset ${shown} for the next ones.\n`,
        );
      }
    },
  }),
  command({
    words: ["threads", "show"],
    summary: "a thread's details",
    operands: ["id"],
    async run({ api, args, json }) {
      const thread = await api.json<Row>("GET", p`/api/v1/threads/${args.op("id")}`);
      print(json, thread, () =>
        Object.entries(thread)
          .filter(([, v]) => v !== null && typeof v !== "object")
          .map(([k, v]) => `${k.padEnd(20)} ${v}`)
          .join("\n"),
      );
    },
  }),
  command({
    words: ["threads", "output"],
    summary: "the text a thread's latest turns wrote",
    operands: ["id"],
    flags: {
      turns: { type: "int", value: "N", help: "this many of the latest turns (default 1); 0 for the recent history" },
    },
    async run({ api, args, json }) {
      const id = args.op("id");
      const out = await api.json<{ thread_id: string; workspace_id: string | null; status: string; text: string }>(
        "GET",
        p`/api/v1/threads/${id}/output`,
        { query: { turns: args.int("turns") } },
      );
      if (out.status === "running") process.stderr.write(`Thread ${id} ${STILL_RUNNING}\n`);
      print(json, out, () => out.text);
    },
  }),
  command({
    words: ["threads", "new"],
    summary: "start a thread in a workspace with a message, and run its turn",
    operands: ["message..."],
    flags: { workspace: { ...WORKSPACE, required: true }, model: MODEL, wait: WAIT, detach: DETACH },
    async run(ctx) {
      const { api, args } = ctx;
      const body = {
        workspace_id: await resolveWorkspace(api, args.str("workspace")!),
        messages: [{ role: "user", content: args.op("message") }],
        platform: "cli",
        llm_model: args.str("model"),
      };
      return turn(ctx, { method: "POST", path: "/api/v1/threads/messages", body });
    },
  }),
  command({
    words: ["threads", "send"],
    summary: "send a message to a thread, and run its turn",
    operands: ["id", "message..."],
    flags: { model: MODEL, wait: WAIT, detach: DETACH },
    async run(ctx) {
      const { args } = ctx;
      const thread = args.op("id");
      const body = { messages: [{ role: "user", content: args.op("message") }], platform: "cli", llm_model: args.str("model") };
      return turn(ctx, { method: "POST", path: p`/api/v1/threads/${thread}/messages`, thread, body });
    },
  }),
  command({
    words: ["threads", "follow"],
    summary: "stream the turn a thread is running",
    operands: ["id"],
    async run(ctx) {
      const thread = ctx.args.op("id");
      return turn(ctx, { method: "GET", path: p`/api/v1/threads/${thread}/messages/stream`, thread });
    },
  }),
  command({
    words: ["threads", "retry"],
    summary: "run a thread's failed turn again",
    operands: ["id"],
    flags: { model: MODEL, wait: WAIT, detach: DETACH },
    async run(ctx) {
      const { api, args } = ctx;
      const thread = args.op("id");
      const model = args.str("model");
      // Without a body the server finds the workspace itself; with one it
      // wants the workspace named, so a model takes a read of the thread.
      const body = model
        ? { workspace_id: (await api.json<Row>("GET", p`/api/v1/threads/${thread}`)).workspace_id, llm_model: model }
        : undefined;
      return turn(ctx, { method: "POST", path: p`/api/v1/threads/${thread}/retry`, thread, body });
    },
  }),
  answer("approve"),
  answer("reject"),
  command({
    words: ["threads", "cancel"],
    summary: "stop the turn a thread is running",
    operands: ["id"],
    flags: {
      run: { type: "string", value: "RUN_ID", help: "stop only this turn, so a stop meant for it never ends a later one" },
    },
    async run({ api, args, json }) {
      const run = args.str("run");
      // The server reads an empty id as none named, which stops whatever runs.
      if (run === "") throw usage("--run needs a run id");
      const result = await api.json<Row>("POST", p`/api/v1/threads/${args.op("id")}/cancel`, {
        query: { run_id: run },
      });
      // Said here, not only in the prompt: an agent told to wait for the report
      // read the thread at once and reported the stop twice. A stop that found
      // nothing to stop has no report coming, and the server says why.
      print(json, result, () =>
        result.cancelled === false
          ? String(result.message ?? "Nothing was cancelled.")
          : "Cancel requested. A report-back, if one is coming, says how far the turn got.");
    },
  }),
  command({
    words: ["threads", "archive"],
    summary: "archive a thread",
    operands: ["id"],
    async run({ api, args, json }) {
      const thread = await api.json<Row>("PATCH", p`/api/v1/threads/${args.op("id")}`, { body: { archived: true } });
      print(json, thread, () => "Thread archived.");
    },
  }),
  command({
    words: ["threads", "delete"],
    summary: "delete a thread and its conversation",
    operands: ["id"],
    flags: { yes: YES },
    async run(ctx) {
      const id = ctx.args.op("id");
      await remove(ctx, {
        path: p`/api/v1/threads/${id}`,
        key: { thread_id: id },
        warning: "Deleting a thread deletes its conversation",
        done: "Thread deleted.",
      });
    },
  }),
];
