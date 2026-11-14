import { CliError, Exit, usage } from "../errors.js";
import { workspaceRef } from "../grammar.js";
import { type Api, p, type Query } from "../http.js";
import { print, table } from "../output.js";
import { type Command, command, remove, type Row, YES } from "./command.js";

// The most the server lists at once.
const PAGE = 100;

interface WorkspaceRow {
  workspace_id: string;
  name: string;
}

/** Every workspace the query lists, a page at a time. */
async function* everyWorkspace<T>(api: Api, query: Query): AsyncGenerator<T> {
  for (let offset = 0; ; offset += PAGE) {
    const page = await api.json<{ workspaces: T[] }>("GET", "/api/v1/workspaces", { query: { ...query, limit: PAGE, offset } });
    yield* page.workspaces;
    if (page.workspaces.length < PAGE) break;
  }
}

/** ``home``, a workspace id, or a workspace's exact name. The server filters
 *  by no name, so a name is looked for across every page. */
export async function resolveWorkspace(api: Api, ref: string): Promise<string> {
  const named = workspaceRef(ref);
  if ("home" in named) {
    const home = await api.json<WorkspaceRow>("POST", "/api/v1/workspaces/home");
    return home.workspace_id;
  }
  if ("id" in named) return named.id;
  const matches = new Set<string>();
  for await (const w of everyWorkspace<WorkspaceRow>(api, { include_home: true, sort_by: "name" })) {
    if (w.name === ref) matches.add(w.workspace_id);
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
      limit: { type: "int", value: "N", help: "at most this many, up to 100 (default: all)" },
    },
    async run({ api, args, json }) {
      const query = { include_home: args.has("include-home") || undefined };
      const limit = args.int("limit");
      // All of them by default: an agent finding a workspace takes a missing
      // one for one that does not exist.
      let listed: Row[] = [];
      if (limit !== undefined) {
        listed = (await api.json<{ workspaces: Row[] }>("GET", "/api/v1/workspaces", { query: { ...query, limit } })).workspaces;
      } else {
        for await (const w of everyWorkspace<Row>(api, query)) listed.push(w);
      }
      // A workspace's folder sits at its computer's root; the computer goes by
      // its name, which is what a person, or the agent's tools, call it.
      const rows = listed.map((w) => ({ ...w, folder: w.dir_name, computer: w.computer_name }));
      print(json, listed, () => table(rows, ["workspace_id", "name", "folder", "computer", "status"]));
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
