import { basename } from "node:path";

/** The name to put in usage lines and hints: `la` only when that is what was
 *  typed, since many shells already alias `la` to `ls`, so `langalpha` is the
 *  one sure to work. A computer's shim passes the name it was run by; an npm
 *  bin's own name arrives as argv[1]. */
export const CLI_NAME =
  (process.env.LANGALPHA_CLI_NAME || basename(process.argv[1] ?? "")) === "la" ? "la" : "langalpha";
