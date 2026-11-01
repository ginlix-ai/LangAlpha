// Copy rules every page follows (docs/README.md "Writing rules"): no em or en
// dashes, and no storage terms a reader should never have to learn. Runs
// before every build so a page that breaks one never ships.
import { readFileSync, readdirSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../src/content/docs/', import.meta.url));
const rules = [
  [/[\u2013\u2014]/, 'em or en dash'],
  [/\b(checkpoint|checkpoints|checkpointed|fork|forks|forked|branch|branches)\b/i, 'storage term'],
];

function* files(dir) {
  for (const name of readdirSync(dir)) {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) yield* files(path);
    else if (/\.mdx?$/.test(name)) yield path;
  }
}

const problems = [];
for (const file of files(root)) {
  let inCode = false;
  readFileSync(file, 'utf8').split('\n').forEach((line, i) => {
    if (line.trimStart().startsWith('```')) inCode = !inCode;
    if (inCode) return;
    for (const [pattern, label] of rules) {
      const match = line.match(pattern);
      if (match) problems.push(`${relative(root, file)}:${i + 1}: ${label} "${match[0]}"`);
    }
  });
}

if (problems.length) {
  console.error(`Copy check failed:\n${problems.join('\n')}`);
  process.exit(1);
}
console.log('Copy check passed.');
