#!/usr/bin/env node
/**
 * Generates the menu bar / notification-area icons from the app logo.
 *
 * Unlike the app icon these are committed: they are runtime resources the shell
 * loads, not packaging inputs, so a dev run and every edition need them without
 * a build step. Rerun this when the logo changes.
 *
 * - `trayTemplate.png` / `@2x` (macOS): the bare glyph in black on transparent.
 *   A template image is read for its alpha alone, and macOS tints it for a light
 *   or dark menu bar and for the highlighted state. Thicker strokes than the
 *   logo's, because 18 points is a long way down from 60.
 * - `tray.png` / `@2x` (Windows, Linux): the dark squircle with the white glyph,
 *   which reads on a light taskbar and a dark one; neither tints anything.
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import sharp from 'sharp'

const here = path.dirname(fileURLToPath(import.meta.url))
const img = path.join(here, '..', '..', 'web', 'src', 'assets', 'img')
const outDir = path.join(here, '..', 'resources')

const glyph = fs.readFileSync(path.join(img, 'logo.svg'), 'utf8')
  .replace(/stroke="white"/g, 'stroke="black"')
  .replace(/stroke-width="3"/g, 'stroke-width="4.6"')
  // The glyph's own extent, so it fills the menu bar's square.
  .replace('viewBox="0 0 60 60"', 'viewBox="6.5 6.5 47 47"')

const squircle = fs.readFileSync(path.join(img, 'logo-favicon-dark.svg'), 'utf8')
  .replace(/stroke-width="3"/g, 'stroke-width="4"')

async function render(svg, size, file) {
  await sharp(Buffer.from(svg), { density: 72 * Math.ceil(size / 18) * 4 })
    .resize(size, size, { fit: 'contain', background: { r: 0, g: 0, b: 0, alpha: 0 } })
    .png()
    .toFile(path.join(outDir, file))
  console.log(`[tray-icons] wrote resources/${file}`)
}

fs.mkdirSync(outDir, { recursive: true })
await render(glyph, 18, 'trayTemplate.png')
await render(glyph, 36, 'trayTemplate@2x.png')
await render(squircle, 16, 'tray.png')
await render(squircle, 32, 'tray@2x.png')
