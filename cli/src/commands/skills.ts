import { CliError, Exit } from "../errors.js";
import { p } from "../http.js";
import { print } from "../output.js";
import { type Command, command, WORKSPACE } from "./command.js";
import { resolveWorkspace } from "./workspaces.js";

interface Saved {
  name: string;
  saved: boolean;
  changed: boolean;
  reason: string | null;
}

export const skills: Command[] = [
  command({
    words: ["skills", "save"],
    summary: "save a skill folder you wrote in a workspace's .agents/skills, and hear at once if it is refused",
    operands: ["name"],
    flags: {
      workspace: { ...WORKSPACE, help: "the workspace whose folder it is (default: the one this command runs in)" },
    },
    async run({ api, args, json }) {
      const name = args.op("name");
      const ref = args.str("workspace");
      const query = { workspace_id: ref ? await resolveWorkspace(api, ref) : undefined };
      const result = await api.json<Saved>("POST", p`/api/v1/skills/${name}/save`, { query });
      if (!result.saved) {
        // The refusal is the answer an agent acts on, so it fails the command
        // rather than printing beside an exit of 0.
        throw new CliError(`Not saved: ${result.reason ?? "the server gave no reason"}`, Exit.failed, {
          detail: result,
          reason: "skill_refused",
        });
      }
      print(json, result, () => (result.changed ? `Saved ${name}` : `${name} is already saved as it stands`));
    },
  }),
];
