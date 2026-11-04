import { p } from "../http.js";
import { print, table } from "../output.js";
import { CliError, Exit, usage } from "../errors.js";
import { isOrder, pendingInterrupt, turn } from "../turn.js";
import { type Command, command, MODEL, remove, type Row, WORKSPACE, YES } from "./command.js";
import { resolveWorkspace } from "./workspaces.js";

const WAIT = { type: "bool", help: "print only the answer, once the turn ends" } as const;
const DETACH = { type: "bool", help: "return once the turn has started, with its thread and run" } as const;

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
    },
    async run({ api, args, json }) {
      const ref = args.str("workspace");
      // The server keeps no list of archived threads across workspaces.
      if (args.has("archived") && !ref) throw usage("--archived lists one workspace's threads: name it with --workspace");
      const page = await api.json<{ threads: Row[] }>("GET", "/api/v1/threads", {
        query: {
          workspace_id: ref ? await resolveWorkspace(api, ref) : undefined,
          archived: args.has("archived") || undefined,
          limit: args.int("limit") ?? 20,
        },
      });
      print(json, page.threads, () =>
        table(page.threads, ["thread_id", "title", "current_status", "workspace_id", "updated_at"]),
      );
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
    async run({ api, args, json }) {
      const result = await api.json<Row>("POST", p`/api/v1/threads/${args.op("id")}/cancel`);
      print(json, result, () => "Cancel requested.");
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
