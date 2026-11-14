// Writes the web app's copy of the command table: `pnpm gen:web-grammar`.
import { writeFileSync } from "node:fs";
import { WEB_GRAMMAR, webGrammar } from "./web-grammar.js";

writeFileSync(WEB_GRAMMAR, webGrammar());
