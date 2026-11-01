import { CliError, Exit, usage } from "../errors.js";
import { type Api, p } from "../http.js";
import { print, table } from "../output.js";
import { type Command, command, remove, type Row, YES } from "./command.js";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
// The most the server lists at once.
const PAGE = 100;

interface WorkspaceRow {
  workspace_id: string;
  name: string;
}

/** ``home``, a workspace id, or a workspace's exact name. The server filters
 *  by no name, so a name is looked for across every page. */
export async function resolveWorkspace(api: Api, ref: string): Promise<string> {
  if (ref.toLowerCase() === "home") {
    const home = await api.json<WorkspaceRow>("POST", "/api/v1/workspaces/home");
    return home.workspace_id;
  }
  if (UUID.test(ref)) return ref;
  const matches = new Set<string>();
  for (let offset = 0; ; offset += PAGE) {
    const page = await api.json<{ workspaces: WorkspaceRow[] }>("GET", "/api/v1/workspaces", {
      query: { limit: PAGE, offset, include_home: true, sort_by: "name" },
    });
    for (const w of page.workspaces) if (w.name === ref) matches.add(w.workspace_id);
    if (page.workspaces.length < PAGE) break;
  }
  const [only, ...others] = matches;
  if (only === undefined) throw new CliError(`No workspace named "${ref}"`, Exit.notFound);
  if (others.length) throw usage(`More than one workspace is named "${ref}"; pass its id`);
  return only;
}

export const workspaces: Command[] = [
  command({
    words: ["workspaces", "list"],
    summary: "your workspaces",
    flags: {
      "include-home": { type: "bool", help: "list Home as well" },
      limit: { type: "int", value: "N", help: "at most this many (default 100)" },
    },
    async run({ api, args, json }) {
      const page = await api.json<{ workspaces: Row[] }>("GET", "/api/v1/workspaces", {
        query: { limit: args.int("limit") ?? PAGE, include_home: args.has("include-home") || undefined },
      });
      print(json, page.workspaces, () => table(page.workspaces, ["workspace_id", "name", "status", "computer_id"]));
    },
  }),
  command({
    words: ["workspaces", "create"],
    summary: "create a workspace",
    operands: ["name"],
    flags: {
      description: { type: "string", value: "D" },
      computer: { type: "string", value: "C", help: "a computer id (default: your primary computer)" },
    },
    async run({ api, args, json }) {
      const body = { name: args.op("name"), description: args.str("description"), computer_id: args.str("computer") };
      const ws = await api.json<Row>("POST", "/api/v1/workspaces", { body });
      print(json, ws, () => `Created workspace ${ws.name} (${ws.workspace_id}).`);
    },
  }),
  command({
    words: ["workspaces", "archive"],
    summary: "archive a stopped workspace's computer and every workspace on it",
    operands: ["ws"],
    async run({ api, args, json }) {
      const ws = await resolveWorkspace(api, args.op("ws"));
      const result = await api.json<Row>("POST", p`/api/v1/workspaces/${ws}/archive`);
      print(json, result, () => "Archived its computer and every workspace on it.");
    },
  }),
  command({
    words: ["workspaces", "delete"],
    summary: "delete a workspace, its threads and its files (on your own machine, its folder only with --delete-files)",
    operands: ["ws"],
    flags: {
      yes: YES,
      "delete-files": { type: "bool", help: "on a machine you added over SSH, delete its folder there too, as the app asks" },
    },
    async run(ctx) {
      const ws = await resolveWorkspace(ctx.api, ctx.args.op("ws"));
      const deleteFiles = ctx.args.has("delete-files");
      await remove(ctx, {
        path: p`/api/v1/workspaces/${ws}`,
        key: { workspace_id: ws },
        warning: deleteFiles
          ? "Deleting a workspace deletes its threads and files, its folder on your own machine included"
          : "Deleting a workspace deletes its threads and files; on a machine you added over SSH its folder stays",
        done: "Workspace deleted.",
        query: deleteFiles ? { delete_files: true } : undefined,
      });
    },
  }),
];
