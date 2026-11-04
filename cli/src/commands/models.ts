import { print, table } from "../output.js";
import { type Command, command } from "./command.js";

interface ModelRow {
  name: string;
  provider: string;
  custom: boolean;
  payer: "credits" | "server_key" | "own_key" | "subscription" | "local" | null;
  plan?: string;
  signed_in?: boolean;
  needs?: "plan" | "key";
}

/** Who a turn on the model is billed to, as the server worked it out. */
export function billsTo(m: ModelRow): string {
  switch (m.payer) {
    case "credits":
      return "LangAlpha credits";
    case "server_key":
      return "the server's key";
    case "own_key":
      return "your API key";
    case "local":
      return "nobody (local server)";
    case "subscription": {
      const plan = m.plan ?? "your";
      const name = /\bplan$/i.test(plan) ? `your ${plan}` : `your ${plan} subscription`;
      return m.signed_in === false ? `${name} (not signed in)` : name;
    }
    default:
      return m.needs === "plan" ? "not on your plan" : "needs your API key";
  }
}

export const models: Command[] = [
  command({
    words: ["models", "list"],
    summary: "the models you can pick, and who each one bills to",
    async run({ api, json }) {
      const res = await api.json<{ models: ModelRow[] }>("GET", "/api/v1/users/me/models");
      const rows = res.models.map((m) => ({
        ...m,
        model: m.custom ? `${m.name} (yours)` : m.name,
        bills_to: billsTo(m),
      }));
      print(json, res.models, () => table(rows, ["model", "provider", "bills_to"]));
    },
  }),
];
