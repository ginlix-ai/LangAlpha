#!/usr/bin/env node
/**
 * Writes the release index, in two copies:
 *
 *   latest.json              what the download page and /latest/<platform> read
 *   versions/<version>.json  the same thing, frozen, for /v/<version>/<platform>
 *
 *   node scripts/make-release-index.mjs <dir> <version>
 *
 * Two files rather than one because they answer different questions and expire
 * differently: the first is a moving target and can never be cached, the second
 * describes a build that has shipped and so can be cached forever. Writing both
 * from one scan is what keeps them from disagreeing.
 *
 * Composed from what is actually in the directory rather than from a hardcoded
 * list, because the alternative is an index that promises a build the release
 * did not produce. A platform whose job failed is then simply absent, and the
 * download page shows the ones that exist instead of a button that 404s.
 *
 * `signed` is read from the environment for the same reason the release notes
 * are: it drives the "how to open this on macOS" copy, and a page that keeps
 * telling people to visit Privacy and Security after the build is signed is
 * worse than one that never said it.
 */
import { mkdirSync, readdirSync, writeFileSync } from 'node:fs'
import path from 'node:path'

const [dir, version] = process.argv.slice(2)
if (!dir || !version) {
  console.error('usage: make-release-index.mjs <dir> <version>')
  process.exit(1)
}

// The version is joined into a path, so it has to be a filename and not a
// route to one. It arrives from the workflow input, which is a free-text field.
if (!/^[A-Za-z0-9][A-Za-z0-9._+-]*$/.test(version)) {
  console.error(`[index] '${version}' is not a usable version; expected something like 0.1.2`)
  process.exit(1)
}

// Ordered: the first pattern that matches a filename wins, so the mac zip (which
// the updater reads) never takes the slot the dmg (which people download) wants.
const PLATFORMS = [
  ['mac-arm64', /-arm64\.dmg$/],
  ['mac-x64', /-x64\.dmg$/],
  ['win-x64', /-x64\.exe$/],
  ['win-arm64', /-arm64\.exe$/],
  ['linux-x64', /\.AppImage$/],
  ['linux-deb', /\.deb$/],
]

const files = readdirSync(dir, { recursive: true })
  .map((f) => path.basename(String(f)))
  .filter((f, i, all) => all.indexOf(f) === i)

// One match per platform, or none. Two is not a tie to break: the scan is
// recursive and flattened to basenames, so a directory holding more than one
// candidate is either a previous version's artifacts left behind or a parent of
// the per-edition output directories electron-builder now writes
// (`dist/<edition>`, see electron-builder.yml). Picking the first in read order
// publishes an index that names a build this release did not produce, which is
// the one failure this file exists to prevent, and it does it silently.
const artifacts = {}
for (const [key, pattern] of PLATFORMS) {
  const matches = files.filter((f) => pattern.test(f))
  if (matches.length > 1) {
    console.error(`[index] ${matches.length} candidates for ${key} in ${dir}: ${matches.join(', ')}`)
    console.error('[index] point this at one build\'s output directory, not a parent of several')
    process.exit(1)
  }
  if (matches.length === 1) artifacts[key] = matches[0]
}

if (Object.keys(artifacts).length === 0) {
  console.error(`[index] no recognisable artifacts in ${dir}; refusing to publish an empty index`)
  process.exit(1)
}

const index = {
  version,
  // Which source produced these binaries. The build no longer necessarily runs
  // in the repository the code came from, so "the commit this was built from" is
  // not something a reader can recover from context any more. Recording it is
  // what keeps a published artifact traceable to an exact tree.
  commit: process.env.BUILD_COMMIT || null,
  published: new Date().toISOString(),
  // Absent secrets mean an unsigned build, which is the current state and the
  // thing the download page needs to warn about.
  signed: {
    mac: process.env.SIGNED_MAC === 'true',
    win: process.env.SIGNED_WIN === 'true',
  },
  artifacts,
}

const body = `${JSON.stringify(index, null, 2)}\n`
mkdirSync(path.join(dir, 'versions'), { recursive: true })
for (const out of [path.join(dir, 'latest.json'), path.join(dir, 'versions', `${version}.json`)]) {
  writeFileSync(out, body)
  console.log(`[index] ${out}`)
}
console.log(body)
