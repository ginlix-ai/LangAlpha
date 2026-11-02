// What npm would ship, checked before it ships: only the bundle and the
// package's own docs, and a bundle that runs and reports this version.
// `npm pack` decides the list, so a new file in `files` or a missing build
// fails here rather than in someone's install.
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const SHIPS = ["LICENSE", "README.md", "dist/la.mjs", "package.json"];

const here = new URL("..", import.meta.url);
const { version } = JSON.parse(readFileSync(new URL("package.json", here)));
const [packed] = JSON.parse(execFileSync("npm", ["pack", "--dry-run", "--json"], { cwd: here, encoding: "utf8" }));
const files = packed.files.map((f) => f.path).sort();

const problems = [];
if (JSON.stringify(files) !== JSON.stringify(SHIPS)) {
  problems.push(`ships ${files.join(", ")}; expected ${SHIPS.join(", ")}`);
}
const reported = execFileSync(process.execPath, [fileURLToPath(new URL("dist/la.mjs", here)), "--version"], {
  encoding: "utf8",
}).trim();
if (reported !== version) problems.push(`the bundle reports "${reported}", not ${version}`);

if (problems.length) {
  console.error(`langalpha package check failed:\n  ${problems.join("\n  ")}`);
  process.exit(1);
}
console.log(`langalpha ${version}: ${files.join(", ")}`);
