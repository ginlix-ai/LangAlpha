import fs from 'node:fs';
import path from 'node:path';

/**
 * The installed monaco-editor release as a `define`. The package only supplies
 * types; the editor runs Monaco's CDN build, which CodeEditor pins to this
 * release so the code that runs is the code the types describe. Read off disk
 * because the package's `exports` map does not expose its package.json.
 */
export function monacoVersionDefine(root: string): Record<string, string> {
  const file = path.join(root, 'node_modules/monaco-editor/package.json');
  const { version } = JSON.parse(fs.readFileSync(file, 'utf8')) as { version: string };
  return { __MONACO_VERSION__: JSON.stringify(version) };
}
