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

import { readFileSync, readdirSync } from 'node:fs'
import { gzipSync } from 'node:zlib'
import { join } from 'node:path'
import { CATALOG_URL } from './locale-preload.ts'

// `rolldown-runtime` is the bundler's shared module runtime (under 1 kB gz).
// Rolldown emits it whenever the build has more than one chunk, and no config
// removes it, so it is eager by construction rather than by an import.
//
// vendor-motion and vendor-dnd load after first paint: the sidebar tree swaps
// them in at idle (navTreeKit), a dialog sheet loads its swipe on first open,
// and each lazy root that animates brings framer with it (lib/lazyWithMotion).
// An eager framer-motion or dnd-kit import puts them back here.
const EXPECTED = ['index', 'rolldown-runtime', 'vendor-react']

// The gzipped first-load payload: the entry and its preloads, index.html, the
// entry stylesheet and the largest locale catalog. Measured against platform
// mode, which is what ships (oss builds land ~60 kB lower), and read against CI's
// gzip, which runs about 1.8 kB above a local build of the same tree. Headroom is
// deliberately thin, so routine growth shows up here instead of being quietly
// absorbed. Raise it only for a measured cause, and put that cause in the commit
// message that raises it; the line's history is the record of why it moved.
const MAX_EAGER_KB = 500

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
  .filter((m) => m !== null)

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
  console.error('  versionManifest (scripts/vite-plugins) did not run. The version poll')
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

// No lazy import may list the entry among the chunks to preload: it is already
// running, Firefox 146 fails a modulepreload for it, and index.html reads that
// failure as a dead build, so every route chunk that imports shared code raised
// the "new version" toast. skipEntryPreload (scripts/vite-plugins) filters it through
// `resolveDependencies`, which Vite marks experimental, so an upgrade could
// quietly turn the filter into a no-op; this is where that shows.
const entryAsDep = ['"', "'", '`'].map((q) => `${q}assets/${entryFile}${q}`)
const preloadingEntry = readdirSync(join(outDir, 'assets')).filter((f) => {
  if (!f.endsWith('.js')) return false
  const code = readFileSync(join(outDir, 'assets', f), 'utf8')
  return entryAsDep.some((dep) => code.includes(dep))
})
if (preloadingEntry.length) {
  console.error(`\n✗ ${preloadingEntry.length} chunk(s) list the entry ${entryFile} as a preload dep`)
  console.error(`  e.g. ${preloadingEntry.slice(0, 3).join(', ')}`)
  console.error('  Firefox 146 fails that modulepreload and the stale-build listener in')
  console.error('  index.html turns it into a false "new version" toast. Check that')
  console.error('  skipEntryPreload (scripts/vite-plugins) still runs.\n')
  process.exit(1)
}

const stripHash = (/** @type {string} */ f) => f.replace(/\.(js|css)$/, '').replace(/-[A-Za-z0-9_-]{8}$/, '')

const eager = [...new Set(assets.filter((f) => f.endsWith('.js')).map(stripHash))].sort()
const expected = [...EXPECTED].sort()

// index.html counts too, and it is the one file here with no cache lifetime at
// all: stale-build recovery depends on the document being refetched every load,
// so every byte in it is paid on every visit, forever. Leaving it outside the
// ceiling is how an inline script grows without anything noticing.
const gz = (/** @type {string} */ f) => gzipSync(readFileSync(join(outDir, 'assets', f))).length

// The byte count depends on the zlib this Node links, not only on the files.
// Official Node builds, which CI runs, bundle Chromium's zlib and agree across
// CPUs; a Node linked against the system zlib (Homebrew's on macOS) reads about
// 1.2 kB lower on the same build. Node's default level, memLevel and strategy
// are fixed constants, so pinning them would not close that gap; naming the
// zlib next to the number is what makes two readings comparable.
const zlib = `zlib ${process.versions.zlib}`

// --- locale catalog ----------------------------------------------------------
//
// Each locale's catalog is a chunk of its own, so none of them is a src/href
// above, yet every first visit fetches one: nothing renders until the active
// catalog is in, and the inline preload (scripts/locale-preload.ts) starts it
// alongside the entry. The largest counts, since that is what some visitor
// pays. A build without the preload fails here rather than reading light while
// every visitor waits on a request that only starts once the entry has run.
const catalogs = [...html.matchAll(CATALOG_URL)]
  .map(([, locale, file]) => ({ locale, bytes: gz(file) }))

if (!catalogs.length) {
  console.error(`\n✗ no locale catalog preload in ${indexPath}`)
  console.error('  scripts/locale-preload.ts did not run, so the catalog request waits')
  console.error('  for the entry to run, and this gate cannot count it.\n')
  process.exit(1)
}

const catalog = catalogs.reduce((a, b) => (b.bytes > a.bytes ? b : a))

const bytes = assets.reduce((n, f) => n + gz(f), gzipSync(html).length + catalog.bytes)
const kb = bytes / 1024

const added = eager.filter((c) => !expected.includes(c))
const removed = expected.filter((c) => !eager.includes(c))
const overBudget = kb > MAX_EAGER_KB

if (added.length || removed.length || overBudget) {
  console.error('\n✗ critical path changed\n')
  console.error(`  chunks:   ${eager.join(', ')} + ${catalog.locale} catalog`)
  console.error(`  expected: ${expected.join(', ')}`)
  console.error(`  payload:  ${kb.toFixed(1)} kB gz, ${zlib} (ceiling ${MAX_EAGER_KB} kB)`)
  if (added.length) {
    console.error(`\n  NEW on the critical path: ${added.join(', ')}`)
    console.error('  Every visitor now downloads these before first paint.')
    console.error('  Usually an eager import reaching a lazy module, or a chunk group')
    console.error('  claiming a vendor that is not actually eager. Trace it with:')
    console.error(`    grep -o 'from"\\./vendor-[^"]*"' ${outDir}/assets/index-*.js`)
  }
  if (removed.length) {
    console.error(`\n  no longer eager: ${removed.join(', ')}`)
    console.error('  Check the payload above before calling this a win — deleting a')
    console.error('  chunk group inlines that vendor into index instead, losing')
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

console.log(`✓ critical path: ${eager.join(', ')} + ${catalog.locale} catalog — ${kb.toFixed(1)} kB gz, ${zlib} (ceiling ${MAX_EAGER_KB} kB)`)
