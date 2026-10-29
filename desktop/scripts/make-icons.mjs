#!/usr/bin/env node
/**
 * Generates the packaging icons from the app logo.
 *
 * electron-builder derives .icns and .ico from one square PNG, so the only work
 * here is scaling it.
 *
 * The editions take different icons deliberately: oss the near-black squircle,
 * saas the white one. Both sources are finished icons rather than bare logos,
 * and having them differ means which build is running is legible from the Dock
 * without opening it.
 *
 * Their corners are transparent and must stay that way. macOS does not mask app
 * icons, so flattening them onto an opaque ground turns the squircle into a
 * square that sits wrong beside every other icon in the Dock.
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const img = path.join(here, '..', '..', 'web', 'src', 'assets', 'img')
// Matches write-build-config.mjs and build.mjs, which treat an unset edition as oss.
const edition = (process.env.DESKTOP_EDITION || 'oss').trim()
const source = path.join(img, edition === 'saas' ? 'logo-favicon.png' : 'logo-favicon-dark.png')
const outDir = path.join(here, '..', 'resources')
const out = path.join(outDir, 'icon.png')

if (!fs.existsSync(source)) {
  console.error(`[icons] source not found: ${source}`)
  process.exit(1)
}

let sharp
try {
  ({ default: sharp } = await import('sharp'))
} catch {
  // resources/icon.png is gitignored, so there is no checked-in fallback: an
  // earlier build's output is the only thing that can stand in. Reusing it is
  // fine for a dev run, but it belongs to whichever edition ran last, so say so
  // rather than let a saas package quietly ship the oss icon.
  if (fs.existsSync(out)) {
    console.warn(`[icons] sharp is not installed; keeping the existing ${path.basename(out)}, which may be another edition's`)
    process.exit(0)
  }
  console.error('[icons] sharp is not installed and there is no resources/icon.png to fall back on')
  process.exit(1)
}

fs.mkdirSync(outDir, { recursive: true })

await sharp(source)
  .resize(1024, 1024, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
  .png()
  .toFile(out)

console.log(`[icons] wrote ${path.relative(process.cwd(), out)}`)
