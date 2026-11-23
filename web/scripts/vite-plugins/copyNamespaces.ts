import fs from 'node:fs';
import path from 'node:path';
import { parseAst, type Plugin, type Rollup } from 'vite';
import { entryChunks } from './entryChunks.ts';

// Holds the bundle to the rule that makes a deferred i18n namespace safe:
// nothing renders a namespace's keys before the namespace has loaded.
//
// A deferred namespace (`locales/<ns>/<locale>.json`) arrives only when
// something asks for it, and a key read before then renders as itself. Two
// things keep that from happening, and both are checked here against the
// chunks the build actually wrote, after tree-shaking, rather than against the
// source's import graph, which reaches nearly everything through the API
// barrels:
//
//  - The entry and every chunk it imports read no deferred namespace, since
//    nothing loads one before first paint.
//  - Every dynamic import loads, beside its chunk, the namespaces that chunk
//    and its static imports read beyond what the importing chunk already
//    needed: `withCopy(() => import('./X'), ['setup'])` (src/i18n.ts). Those
//    the importer needed were loaded by whatever loaded the importer, so by
//    induction from the entry every chunk runs with its copy in. A listed
//    namespace nothing needs is only warned about (see where it is found).
//
// A chunk reads a namespace when its code holds a string that starts like one
// of its keys, `"ns:<word>` or a template head `` `ns:${ ``, whatever follows:
// a plural or a suffix completes a key at runtime, so matching the catalog's
// own key names would miss those. A look-alike that is not a key goes in
// NOT_KEYS, since a false match costs a listed namespace and a missed one costs
// raw keys on screen.

/** Strings in app code that start like a namespace's key and are not one:
 *  storage and layout-cache keys, matched in the emitted code. */
const NOT_KEYS = [
  /(["'`])setup:return\1/g, // lib/setupReturn
  /(["'`])mcp:connect-started\1/g, // Plugins/hooks/useConnectLanding
  /(["'`])settings:modelMode\1/g, // Settings/panels/ModelsPanel
  /`dashboard:brief\.height\.\$\{/g, // Dashboard/components/AIDailyBriefCard
  /`dashboard:\$\{[\w$]+\}\.rows`/g, // Dashboard/widgets/definitions/_holdingsPrimitives
];

type AstNode = { type: string; [key: string]: unknown };
const isNode = (v: unknown): v is AstNode => typeof v === 'object' && v !== null && typeof (v as AstNode).type === 'string';
const stringValue = (n: unknown) => (isNode(n) && n.type === 'Literal' && typeof n.value === 'string' ? n.value : null);

/** Each `import('...')` in a module and the namespaces it loads with: the
 *  list of the `withCopy(load, [...])` whose loader holds it, else none. */
function importsWithCopy(code: string): { spec: string; copy: string[] }[] {
  const found: { spec: string; copy: string[] }[] = [];
  const walk = (node: AstNode, copy: string[]) => {
    const [load, list] = node.type === 'CallExpression' && isNode(node.callee) && node.callee.name === 'withCopy'
      ? node.arguments as unknown[] : [];
    if (isNode(load) && isNode(list) && list.type === 'ArrayExpression') {
      walk(load, (list.elements as unknown[]).map(stringValue).filter((ns) => ns !== null));
      return;
    }
    const spec = node.type === 'ImportExpression' ? stringValue(node.source) : null;
    if (spec !== null) found.push({ spec, copy });
    for (const value of Object.values(node)) {
      for (const child of Array.isArray(value) ? value : [value]) if (isNode(child)) walk(child, copy);
    }
  };
  walk(parseAst(code) as unknown as AstNode, []);
  return found;
}

export function copyNamespaces(localesDir: string): Plugin {
  const dir = path.resolve(localesDir);
  /** Each deferred namespace and a pattern for its keys in emitted code. */
  let readers = new Map<string, RegExp>();
  /** Per importing module: namespaces each specifier is loaded with. */
  const listed = new Map<string, Map<string, string[]>>();

  return {
    name: 'la-copy-namespaces',
    apply: 'build',
    buildStart() {
      readers = new Map();
      for (const ns of fs.readdirSync(dir)) {
        if (!fs.existsSync(path.join(dir, ns, 'en-US.json'))) continue;
        readers.set(ns, new RegExp(`["'\`]${ns}:(?:[\\w$]|\\$\\{)`));
      }
    },
    async transform(code, id) {
      // Kept across rebuilds in watch mode, which transforms only what changed.
      listed.delete(id);
      if (id.includes('/node_modules/') || !/\bwithCopy\s*\(/.test(code)) return null;
      // What every import of a target loads with it: the namespaces all of
      // them agree on, so a bare import beside a wrapped one counts as none.
      const bySpec = new Map<string, string[]>();
      for (const { spec, copy } of importsWithCopy(code)) {
        const resolved = await this.resolve(spec, id);
        if (!resolved) continue;
        const prior = bySpec.get(resolved.id);
        bySpec.set(resolved.id, prior ? prior.filter((ns) => copy.includes(ns)) : copy);
      }
      listed.set(id, bySpec);
      return null;
    },
    generateBundle: {
      order: 'post',
      handler(_options, bundle) {
        const chunks = Object.values(bundle).filter((c): c is Rollup.OutputChunk => c.type === 'chunk');
        const byFile = new Map(chunks.map((c) => [c.fileName, c]));
        const chunkOf = new Map<string, Rollup.OutputChunk>();
        for (const c of chunks) for (const id of c.moduleIds) chunkOf.set(id, c);

        const keyText = (code: string) => NOT_KEYS.reduce((text, re) => text.replace(re, ''), code);
        const reads = (c: Rollup.OutputChunk) => {
          const text = keyText(c.code);
          return [...readers].filter(([, re]) => re.test(text)).map(([ns]) => ns);
        };
        const closure = new Map<string, { files: Set<string>; ns: Set<string> }>();
        const closureOf = (c: Rollup.OutputChunk) => {
          let hit = closure.get(c.fileName);
          if (hit) return hit;
          hit = { files: new Set(), ns: new Set() };
          const queue = [c];
          while (queue.length) {
            const next = queue.pop()!;
            if (hit.files.has(next.fileName)) continue;
            hit.files.add(next.fileName);
            for (const ns of reads(next)) hit.ns.add(ns);
            for (const f of next.imports) {
              const dep = byFile.get(f);
              if (dep) queue.push(dep);
            }
          }
          closure.set(c.fileName, hit);
          return hit;
        };
        /** The modules in a chunk's closure that read `ns`, for the report. */
        const readersOf = (c: Rollup.OutputChunk, ns: string) => {
          const re = readers.get(ns)!;
          return [...closureOf(c).files]
            .flatMap((f) => Object.entries(byFile.get(f)!.modules))
            .filter(([, m]) => m.code && re.test(keyText(m.code)))
            .map(([id]) => path.relative(process.cwd(), id))
            .slice(0, 3);
        };

        const problems: string[] = [];
        const spare = new Set<string>();
        for (const entry of entryChunks(bundle)) {
          for (const ns of closureOf(entry).ns) {
            problems.push(`the entry reads deferred namespace "${ns}" (${readersOf(entry, ns).join(', ')}); nothing loads it before first paint`);
          }
        }

        for (const c of chunks) {
          const importer = closureOf(c);
          for (const id of c.moduleIds) {
            const info = this.getModuleInfo(id);
            if (!info) continue;
            for (const target of info.dynamicallyImportedIds) {
              if (path.dirname(target).startsWith(dir)) continue;
              const into = chunkOf.get(target);
              if (!into) continue;
              const copy = listed.get(id)?.get(target) ?? [];
              const needs = importer.files.has(into.fileName) ? [] : [...closureOf(into).ns].filter((ns) => !importer.ns.has(ns));
              const site = `${path.relative(process.cwd(), id)}: import('${path.relative(path.dirname(id), target)}')`;
              for (const ns of copy.filter((n) => !readers.has(n))) {
                problems.push(`${site} lists "${ns}", which is no namespace folder, so its load fails`);
              }
              for (const ns of needs.filter((n) => !copy.includes(n))) {
                problems.push(
                  `${site} reaches "${ns}" (${readersOf(into, ns).join(', ')}) without loading it; `
                  + `list it: withCopy(() => import(...), [..., '${ns}'])`,
                );
              }
              // Only a warning: a namespace read by code one host mode keeps
              // and the other drops is needed in one build and spare in the
              // other, and the list has to serve both.
              for (const ns of copy.filter((n) => readers.has(n) && !needs.includes(n))) {
                spare.add(`${site} lists "${ns}", which nothing it reaches reads beyond what its importer loaded`);
              }
            }
          }
        }

        if (spare.size) this.warn(`namespaces loaded for nothing (${spare.size}):\n  - ${[...spare].join('\n  - ')}`);
        if (problems.length) {
          this.error(`deferred i18n namespaces read before they load (${problems.length}):\n  - ${[...new Set(problems)].join('\n  - ')}`);
        }
      },
    },
  };
}
