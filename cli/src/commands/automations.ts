import { usage } from "../errors.js";
import { p } from "../http.js";
import { print, table } from "../output.js";
import { type Args } from "../args.js";
import { type Command, command, MODEL, remove, type Row, WORKSPACE, YES } from "./command.js";
import { resolveWorkspace } from "./workspaces.js";

const FIELDS = {
  name: { type: "string", value: "N" },
  prompt: { type: "string", value: "P", help: "what the agent is asked each run" },
  cron: { type: "string", value: "EXPR", help: "run on this cron schedule" },
  at: { type: "string", value: "ISO", help: "run once, at this time" },
  timezone: { type: "string", value: "TZ", help: "the schedule's timezone (default: this machine's)" },
  workspace: WORKSPACE,
  model: MODEL,
} as const;

function schedule(args: Args): Row {
  const cron = args.str("cron");
  const at = args.str("at");
  if (cron && at) throw usage("Pass --cron or --at, not both");
  if (cron) return { trigger_type: "cron", cron_expression: cron };
  if (at) return { trigger_type: "once", next_run_at: at };
  return {};
}

const action = (verb: "trigger" | "pause" | "resume", summary: string, done: string) =>
  command({
    words: ["automations", verb],
    summary,
    operands: ["id"],
    async run({ api, args, json }) {
      const result = await api.json<Row>("POST", p`/api/v1/automations/${args.op("id")}/${verb}`);
      print(json, result, () => done);
    },
  });

export const automations: Command[] = [
  command({
    words: ["automations", "list"],
    summary: "your automations",
    flags: {
      status: { type: "string", value: "S", help: "only those in this status (active, paused, ...)" },
      limit: { type: "int", value: "N", help: "at most this many (default 50)" },
    },
    async run({ api, args, json }) {
      const page = await api.json<{ automations: Row[] }>("GET", "/api/v1/automations", {
        query: { status: args.str("status"), limit: args.int("limit") ?? 50 },
      });
      print(json, page.automations, () =>
        table(page.automations, ["automation_id", "name", "status", "trigger_type", "cron_expression", "next_run_at"]),
      );
    },
  }),
  command({
    words: ["automations", "create"],
    summary: "schedule the agent to run a prompt, on a cron schedule or once",
    // Required, Home being ``-w home``: a create naming none lands in Home,
    // which a computer's token is refused, so the place is always said.
    flags: {
      ...FIELDS,
      name: { ...FIELDS.name, required: true },
      prompt: { ...FIELDS.prompt, required: true },
      workspace: { ...FIELDS.workspace, required: true },
    },
    async run({ api, args, json }) {
      const when = schedule(args);
      if (!when.trigger_type) throw usage("An automation needs --cron or --at");
      const body = {
        ...when,
        name: args.str("name"),
        instruction: args.str("prompt"),
        timezone: args.str("timezone") ?? Intl.DateTimeFormat().resolvedOptions().timeZone,
        llm_model: args.str("model"),
        workspace_id: await resolveWorkspace(api, args.str("workspace")!),
      };
      const automation = await api.json<Row>("POST", "/api/v1/automations", { body });
      print(json, automation, () => `Created automation ${automation.name} (${automation.automation_id}).`);
    },
  }),
  command({
    words: ["automations", "edit"],
    summary: "change an automation",
    operands: ["id"],
    flags: FIELDS,
    async run({ api, args, json }) {
      const ref = args.str("workspace");
      const fields = {
        ...schedule(args),
        name: args.str("name"),
        instruction: args.str("prompt"),
        timezone: args.str("timezone"),
        llm_model: args.str("model"),
      };
      if (!ref && Object.values(fields).every((v) => v === undefined)) throw usage("Nothing to change");
      const body = { ...fields, workspace_id: ref ? await resolveWorkspace(api, ref) : undefined };
      const automation = await api.json<Row>("PATCH", p`/api/v1/automations/${args.op("id")}`, { body });
      print(json, automation, () => "Automation updated.");
    },
  }),
  action("trigger", "run an automation now", "Automation triggered."),
  action("pause", "pause an automation", "Automation paused."),
  action("resume", "resume a paused automation", "Automation resumed."),
  command({
    words: ["automations", "delete"],
    summary: "delete an automation and its run history",
    operands: ["id"],
    flags: { yes: YES },
    async run(ctx) {
      const id = ctx.args.op("id");
      await remove(ctx, {
        path: p`/api/v1/automations/${id}`,
        key: { automation_id: id },
        warning: "Deleting an automation deletes its run history",
        done: "Automation deleted.",
      });
    },
  }),
];
