// Asserts what the browser fetches before first paint: which chunks, and how many bytes.
//
// Vite's build summary lists every chunk in one flat table, which reads as though
// they are all lazy. They are not: whatever the entry statically imports gets a
// <link rel="modulepreload"> in index.html and is on the critical path of every
// page load. A chunking change can silently move a 500 kB vendor bundle onto that
// list, and no typecheck or unit test will notice. This did happen — a lazy-vendor
// manualChunks entry pinned the chart bundle to the entry for five months.
//
// Two assertions, because names alone leak: a vendor that is eagerly imported gets
// folded into `index` itself rather than gaining a chunk name, so the name set can
// stay identical while the payload grows. The byte ceiling is the invariant that
// matters; the name set localizes the blame when it trips.
//
// Update either constant deliberately, never to make a red build go green.

import { readFileSync } from 'node:fs'
import { gzipSync } from 'node:zlib'
import { join } from 'node:path'

const EXPECTED = ['index', 'vendor-dnd', 'vendor-motion', 'vendor-react']

// Measured against platform mode, which is what ships (oss builds land ~60 kB
// lower). Headroom is deliberately thin — routine growth should be visible here,
// not quietly absorbed.
//
// Raised 450 -> 460 for per-model tuning: the composer resolves the selected
// model's profile to label its trigger before first paint, which puts
// modelPreferences.ts, useUpdatePreferences.ts and the dropdown primitive on the
// critical path for +1.9 kB gz. Nothing moved chunks; the eager set is unchanged.
const MAX_EAGER_KB = 460

const outDir = process.argv[2] || 'dist'
const indexPath = join(outDir, 'index.html')

let html
try {
  html = readFileSync(indexPath, 'utf8')
} catch {
  console.error(`\n✗ ${indexPath} not found — run \`vite build\` first.\n`)
  process.exit(1)
}

// Both the entry <script> and each <link rel="modulepreload"> are fetched up
// front; a preload the entry did not import would not be emitted here. CSS counts
// too — a stylesheet link is render-blocking.
const assets = [...new Set(
  [...html.matchAll(/(?:src|href)="[^"]*\/assets\/([^"]+?\.(?:js|css))"/g)].map((m) => m[1]),
)]

// Zero matches means the scrape went stale (assetsDir renamed, bundle inlined),
// not that the critical path emptied. Without this it reports as "no longer
// eager", which is the most misleading way to say "I broke".
if (!assets.length) {
  console.error(`\n✗ no /assets/* references found in ${indexPath}`)
  console.error('  The scrape pattern is stale — check build.assetsDir / output options.\n')
  process.exit(1)
}

// --- stale-build recovery contract -------------------------------------------
//
// One build identity is derived twice and the two derivations never meet at
// runtime: the vite plugin picks the entry by `chunk.isEntry` and writes it to
// version.json, while the browser re-reads it off the DOM (staleBuild.tsx
// `currentBuild()`). Every way they can disagree is silent by construction —
// checkForNewBuild returns quietly on a non-OK status, a non-JSON content-type
// and a parse error, because "unknown" must never become "you are behind". So a
// dropped plugin, a renamed assetsDir or a shell served at /version.json costs
// the whole version layer with no console line and no red test. This is the one
// place both halves exist at once, so it is where they get compared.

// Attribute order carries no meaning in HTML, and requiring type before src is
// not merely brittle here — it fails silently in the direction that matters. A
// second entry written `<script src="…" type="module">` would go uncounted, so
// this gate would still see exactly one and pass, while currentBuild()'s
// `script[type="module"][src]` selector takes it as the first match and
// compares the wrong filename for every visitor.
const moduleScripts = [...html.matchAll(/<script\b([^>]*)>/g)]
  .map(([, attrs]) => attrs)
  .filter((attrs) => /\btype\s*=\s*"module"/.test(attrs))
  .map((attrs) => /\bsrc\s*=\s*"([^"]+)"/.exec(attrs))
  .filter(Boolean)

// currentBuild() takes querySelector's first match. More than one module script
// and it may read something that is not the entry, and then `build !== mine` is
// true for every visitor — a permanent, undismissable "new version" toast.
if (moduleScripts.length !== 1) {
  console.error(`\n✗ expected exactly 1 module <script> in ${indexPath}, found ${moduleScripts.length}`)
  console.error('  staleBuild.tsx currentBuild() reads the first one and assumes it is')
  console.error('  the entry. Another module script in <head> makes every user see a')
  console.error('  permanent "new version" prompt.\n')
  process.exit(1)
}

const entryFile = moduleScripts[0][1].split('/').pop()

let version
try {
  version = JSON.parse(readFileSync(join(outDir, 'version.json'), 'utf8'))
} catch {
  console.error(`\n✗ ${outDir}/version.json missing or unparseable`)
  console.error('  emitVersionManifest (vite.config.js) did not run. The version poll')
  console.error('  fails closed, so stale-build detection is dead with no signal.\n')
  process.exit(1)
}

if (version.build !== entryFile) {
  console.error(`\n✗ version.json disagrees with ${indexPath}`)
  console.error(`  version.json build: ${version.build}`)
  console.error(`  index.html entry:   ${entryFile}`)
  console.error('  The client compares these two. A mismatch prompts every user to')
  console.error('  reload, forever, and the reload does not clear it.\n')
  process.exit(1)
}

const stripHash = (f) => f.replace(/\.(js|css)$/, '').replace(/-[A-Za-z0-9_-]{8}$/, '')

const eager = [...new Set(assets.filter((f) => f.endsWith('.js')).map(stripHash))].sort()
const expected = [...EXPECTED].sort()

// index.html counts too, and it is the one file here with no cache lifetime at
// all: stale-build recovery depends on the document being refetched every load,
// so every byte in it is paid on every visit, forever. Leaving it outside the
// ceiling is how an inline script grows without anything noticing.
const bytes = assets.reduce(
  (n, f) => n + gzipSync(readFileSync(join(outDir, 'assets', f))).length,
  gzipSync(html).length,
)
const kb = bytes / 1024

const added = eager.filter((c) => !expected.includes(c))
const removed = expected.filter((c) => !eager.includes(c))
const overBudget = kb > MAX_EAGER_KB

if (added.length || removed.length || overBudget) {
  console.error('\n✗ critical path changed\n')
  console.error(`  chunks:   ${eager.join(', ')}`)
  console.error(`  expected: ${expected.join(', ')}`)
  console.error(`  payload:  ${kb.toFixed(1)} kB gz (ceiling ${MAX_EAGER_KB} kB)`)
  if (added.length) {
    console.error(`\n  NEW on the critical path: ${added.join(', ')}`)
    console.error('  Every visitor now downloads these before first paint.')
    console.error('  Usually an eager import reaching a lazy module, or a manualChunks')
    console.error('  entry for a vendor that is not actually eager. Trace it with:')
    console.error(`    grep -o 'from"\\./vendor-[^"]*"' ${outDir}/assets/index-*.js`)
  }
  if (removed.length) {
    console.error(`\n  no longer eager: ${removed.join(', ')}`)
    console.error('  Check the payload above before calling this a win — deleting a')
    console.error('  manualChunks entry inlines that vendor into index instead, losing')
    console.error('  the chunk name and its cross-deploy cache key at no byte saving.')
  }
  if (overBudget) {
    console.error(`\n  OVER BUDGET by ${(kb - MAX_EAGER_KB).toFixed(1)} kB.`)
    console.error('  Something eagerly reachable from the entry grew. Trace it with:')
    console.error(`    pnpm exec vite build --sourcemap  # then inspect ${outDir}/assets/index-*.js.map`)
  }
  console.error('')
  process.exit(1)
}

console.log(`✓ critical path: ${eager.join(', ')} — ${kb.toFixed(1)} kB gz (ceiling ${MAX_EAGER_KB} kB)`)
