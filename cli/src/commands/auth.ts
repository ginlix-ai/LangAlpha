import { CliError, Exit, usage } from "../errors.js";
import { Api } from "../http.js";
import { browserLogin, discover, readSecret } from "../login.js";
import { print } from "../output.js";
import { forgetLogin, saveLogin, selectServer } from "../store.js";
import { type Command, command, type Row } from "./command.js";

export const auth: Command[] = [
  command({
    words: ["login"],
    summary: "sign in: in the browser, or with a personal access token on stdin",
    flags: {
      "with-token": { type: "bool", help: "read a personal access token (la_pat_...) from stdin, as CI does" },
      "no-browser": { type: "bool", help: "print the sign-in page without opening it" },
    },
    async run({ api, args, json }) {
      const baseUrl = api.baseUrl;
      const cli = await discover(api);
      if (!cli.auth_required) {
        await selectServer(baseUrl);
        print(json, { server: baseUrl, signed_in: true }, () => `${baseUrl} needs no sign-in.`);
        return;
      }
      if (args.has("with-token") || !cli.issuer || !cli.client_id) {
        if (!cli.personal_access_tokens) throw new CliError(`${baseUrl} takes no CLI sign-in`, Exit.auth);
        const token = await readSecret("Personal access token: ");
        if (!token.startsWith("la_pat_")) throw usage("That is not a personal access token (la_pat_...)");
        // Saved only once the server takes it, so a mistyped or revoked token
        // never becomes the default sign-in.
        const asked = new Api({ baseUrl, token: async () => token, renew: async () => false });
        await asked.call("GET", "/api/v1/auth/principal").catch((e: unknown) => {
          if (e instanceof CliError && e.status === 401) {
            throw new CliError(`${baseUrl} does not accept that token. Check it, or make a new one.`, Exit.auth, { status: 401 });
          }
          throw e;
        });
        await saveLogin(baseUrl, { kind: "pat", token }, { select: true });
      } else {
        await browserLogin(baseUrl, { issuer: cli.issuer, clientId: cli.client_id, scopes: cli.scopes }, !args.has("no-browser"));
      }
      print(json, { server: baseUrl, signed_in: true }, () => `Signed in to ${baseUrl}.`);
    },
  }),
  command({
    words: ["logout"],
    summary: "forget your sign-in to the server",
    async run({ api, json }) {
      const had = await forgetLogin(api.baseUrl);
      print(json, { server: api.baseUrl, signed_out: had }, () =>
        had ? `Signed out of ${api.baseUrl}.` : `Not signed in to ${api.baseUrl}.`,
      );
    },
  }),
  command({
    words: ["whoami"],
    summary: "who the server takes you for, and what you may do",
    async run({ api, json }) {
      const me = await api.json<Row>("GET", "/api/v1/auth/principal");
      print(json, { server: api.baseUrl, ...me }, () => {
        // A session or the server's own call holds no scopes and needs none;
        // a token holding none may do nothing a scope guards.
        const unscoped = me.auth === "session" || me.auth === "service";
        const scopes = Array.isArray(me.scopes) && me.scopes.length ? (me.scopes as string[]).join(" ") : unscoped ? "all" : "none";
        const lines = [`server   ${api.baseUrl}`, `user     ${me.user_id}`, `via      ${me.auth}`, `scopes   ${scopes}`];
        if (me.computer_id) lines.push(`computer ${me.computer_id}`);
        return lines.join("\n");
      });
    },
  }),
];
