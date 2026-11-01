import { bind, type Command, flagList, GLOBAL, parseFor, select, synopsis } from "./commands/command.js";
import { COMMANDS } from "./commands/index.js";
import { resolveCredential } from "./credentials.js";
import { CliError, Exit } from "./errors.js";
import { Api, VERSION } from "./http.js";
import { printable } from "./output.js";
import { CLI_NAME } from "./name.js";

// A synopsis this long or shorter keeps its summary on the same line.
const COLUMN = 44;

function commandLines(commands: readonly Command[]): string {
  return commands
    .map((c) => {
      const s = synopsis(c);
      return s.length <= COLUMN ? `  ${s.padEnd(COLUMN)}  ${c.summary}` : `  ${s}\n  ${" ".repeat(COLUMN)}  ${c.summary}`;
    })
    .join("\n");
}

function help(): string {
  // One-word commands (login, logout, whoami) stand together.
  const group = (c: Command) => (c.words.length > 1 ? c.words[0] : "");
  const groups = [...new Set(COMMANDS.map(group))].map((g) => commandLines(COMMANDS.filter((c) => group(c) === g)));
  return `langalpha ${VERSION}: LangAlpha from the command line

Usage: ${CLI_NAME} <command> [options]          ${CLI_NAME} <command> --help shows its options
       la is the short name, where your shell does not alias it to ls

${groups.join("\n\n")}

  WS is a workspace id, its exact name, or "home".

Options:
${flagList(GLOBAL)}

Exit codes: 0 ok, 1 error, 2 usage, 3 not signed in or not allowed, 4 not found, 5 conflict,
            6 the turn is waiting for approval, 7 the turn failed or was cancelled.
`;
}

function commandHelp(c: Command): string {
  const options = c.flags && Object.keys(c.flags).length ? `\nOptions:\n${flagList(c.flags)}\n` : "";
  return `Usage: ${CLI_NAME} ${synopsis(c)}\n\n${c.summary}\n${options}\nGlobal options:\n${flagList(GLOBAL)}\n`;
}

/** No command matched: help when asked or when a group's commands are wanted. */
function unmatched(words: readonly string[], asked: boolean): number {
  if (words.length === 0 || words[0] === "help") {
    process.stdout.write(help());
    return words.length || asked ? Exit.ok : Exit.usage;
  }
  const group = COMMANDS.filter((c) => c.words[0] === words[0]);
  asked ||= words.includes("--help") || words.includes("-h");
  if (group.length && (words.length === 1 || words[1]!.startsWith("-"))) {
    (asked ? process.stdout : process.stderr).write(`${commandLines(group)}\n`);
    return asked ? Exit.ok : Exit.usage;
  }
  const named = words.slice(0, group.length ? 2 : 1).join(" ");
  process.stderr.write(`Unknown command: ${printable(named)}\n\n${group.length ? `${commandLines(group)}\n` : help()}`);
  return Exit.usage;
}

export async function main(argv: string[]): Promise<number> {
  let json = argv.includes("--json");
  let command: Command | undefined;
  try {
    const selection = select(COMMANDS, argv);
    command = selection.command;
    const parsed = parseFor(command, selection.rest);
    const { values } = parsed;
    json = values.has("json");
    if (values.has("version")) {
      process.stdout.write(`${VERSION}\n`);
      return Exit.ok;
    }
    if (!command) return unmatched(selection.words, values.has("help"));
    if (values.has("help")) {
      process.stdout.write(commandHelp(command));
      return Exit.ok;
    }
    const args = bind(command, parsed);
    const api = new Api(resolveCredential({ server: args.str("server"), token: args.str("token") }));
    return (await command.run({ api, args, json })) ?? Exit.ok;
  } catch (e) {
    const err = e instanceof CliError ? e : new CliError(e instanceof Error ? e.message : String(e));
    if (json) {
      process.stderr.write(`${JSON.stringify({ error: err.message, status: err.status, detail: err.detail })}\n`);
    } else {
      // A usage error of our own, not a server's 400 or 422, shows how the command goes.
      const how = command && err.code === Exit.usage && err.status === undefined ? `\nUsage: ${CLI_NAME} ${synopsis(command)}` : "";
      process.stderr.write(`${CLI_NAME}: ${printable(err.message)}${how}\n`);
    }
    return err.code;
  }
}

main(process.argv.slice(2)).then(
  (code) => {
    process.exitCode = code;
  },
  (e) => {
    process.stderr.write(`${CLI_NAME}: ${printable(e instanceof Error ? e.message : String(e))}\n`);
    process.exitCode = Exit.error;
  },
);
