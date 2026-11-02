// One file, no runtime dependencies: the sandbox installs it by copying.
import { build } from "esbuild";
import { readFileSync } from "node:fs";

const { version } = JSON.parse(readFileSync(new URL("./package.json", import.meta.url)));

await build({
  entryPoints: ["src/main.ts"],
  outfile: "dist/la.mjs",
  bundle: true,
  platform: "node",
  format: "esm",
  target: "node20",
  banner: { js: "#!/usr/bin/env node" },
  define: {
    __LA_VERSION__: JSON.stringify(version),
    // The server a bundle talks to when nothing names one. Left unset, a build
    // from source defaults to a local server; the npm release sets it.
    __LA_DEFAULT_SERVER__: JSON.stringify(process.env.LANGALPHA_CLI_DEFAULT_SERVER ?? ""),
  },
  legalComments: "none",
});
