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
const MAX_EAGER_KB = 450

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

const stripHash = (f) => f.replace(/\.(js|css)$/, '').replace(/-[A-Za-z0-9_-]{8}$/, '')

const eager = [...new Set(assets.filter((f) => f.endsWith('.js')).map(stripHash))].sort()
const expected = [...EXPECTED].sort()

const bytes = assets.reduce(
  (n, f) => n + gzipSync(readFileSync(join(outDir, 'assets', f))).length,
  0,
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
